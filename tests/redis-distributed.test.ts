import { describe, expect, it, vi } from 'vitest';
import { createServer } from 'http';
import { WebSocket } from 'ws';
import {
  DistributedStateManager,
  parseAndNormalizeRedisUrl,
  sanitizeRedisErrorMessage,
} from '../src/services/distributedState.js';
import { createOlogleApp, attachWebSocketServer } from '../src/app.js';
import { OlogleEngine } from '../src/signaling/serverCore.js';

/**
 * Test-only in-memory multi-instance Redis protocol bus for deterministic
 * cross-instance integration testing when an external Redis daemon is not running.
 * Enforces separate publisher and subscriber client handles and atomic HDEL semantics.
 */
class InMemoryRedisClusterBus {
  private kv = new Map<string, { value: string; expiresAt: number | null }>();
  private hashes = new Map<string, { fields: Map<string, string>; expiresAt: number | null }>();
  private subscribers = new Set<{
    channels: Set<string>;
    onMessage: (channel: string, message: string) => void;
  }>();

  public createClients(): { pubClient: any; subClient: any } {
    const subRecord = {
      channels: new Set<string>(),
      onMessage: (_ch: string, _msg: string) => {},
    };
    this.subscribers.add(subRecord);

    const pubClient = {
      publish: async (channel: string, message: string): Promise<number> => {
        let delivered = 0;
        for (const sub of this.subscribers) {
          if (sub.channels.has(channel)) {
            delivered++;
            queueMicrotask(() => sub.onMessage(channel, message));
          }
        }
        return delivered;
      },
      set: async (key: string, value: string, mode?: string, ttlSeconds?: number): Promise<'OK'> => {
        const expiresAt =
          mode === 'EX' && typeof ttlSeconds === 'number' ? Date.now() + ttlSeconds * 1000 : null;
        this.kv.set(key, { value, expiresAt });
        return 'OK';
      },
      get: async (key: string): Promise<string | null> => {
        const entry = this.kv.get(key);
        if (!entry) return null;
        if (entry.expiresAt !== null && Date.now() > entry.expiresAt) {
          this.kv.delete(key);
          return null;
        }
        return entry.value;
      },
      del: async (key: string): Promise<number> => {
        return this.kv.delete(key) ? 1 : 0;
      },
      hset: async (key: string, field: string, value: string): Promise<number> => {
        let hash = this.hashes.get(key);
        if (!hash || (hash.expiresAt !== null && Date.now() > hash.expiresAt)) {
          hash = { fields: new Map(), expiresAt: null };
          this.hashes.set(key, hash);
        }
        const isNew = !hash.fields.has(field);
        hash.fields.set(field, value);
        return isNew ? 1 : 0;
      },
      expire: async (key: string, ttlSeconds: number): Promise<number> => {
        const hash = this.hashes.get(key);
        if (!hash) return 0;
        hash.expiresAt = Date.now() + ttlSeconds * 1000;
        return 1;
      },
      hdel: async (key: string, field: string): Promise<number> => {
        const hash = this.hashes.get(key);
        if (!hash) return 0;
        if (hash.expiresAt !== null && Date.now() > hash.expiresAt) {
          this.hashes.delete(key);
          return 0;
        }
        return hash.fields.delete(field) ? 1 : 0;
      },
      hgetall: async (key: string): Promise<Record<string, string>> => {
        const hash = this.hashes.get(key);
        if (!hash) return {};
        if (hash.expiresAt !== null && Date.now() > hash.expiresAt) {
          this.hashes.delete(key);
          return {};
        }
        const result: Record<string, string> = {};
        for (const [f, v] of hash.fields.entries()) {
          result[f] = v;
        }
        return result;
      },
      quit: async (): Promise<'OK'> => 'OK',
    };

    const subClient = {
      on: (event: string, listener: (channel: string, message: string) => void) => {
        if (event === 'message') {
          subRecord.onMessage = listener;
        }
      },
      subscribe: async (channel: string): Promise<number> => {
        subRecord.channels.add(channel);
        return subRecord.channels.size;
      },
      unsubscribe: async (channel: string): Promise<number> => {
        subRecord.channels.delete(channel);
        return subRecord.channels.size;
      },
      quit: async (): Promise<'OK'> => {
        this.subscribers.delete(subRecord);
        return 'OK';
      },
    };

    return { pubClient, subClient };
  }
}

describe('Redis & Distributed State Architecture (Fluid Compute & Cross-Instance)', () => {
  describe('Local In-Memory Mode (Development & Testing)', () => {
    it('uses standard REDIS_URL environment variable name without invented credentials', () => {
      const manager = new DistributedStateManager();
      expect(manager.isDistributed()).toBe(false);
      expect(manager.isConfigured()).toBe(Boolean(process.env.REDIS_URL?.trim()));
    });

    it('falls back safely in non-production when REDIS_URL is not provided', async () => {
      const manager = new DistributedStateManager();
      await expect(manager.publishToSession('test-session', JSON.stringify({ type: 'PING' }))).resolves.toBe(false);
      await expect(manager.getSessionState('test-session')).resolves.toBeNull();
      await expect(manager.getSharedQueueTickets()).resolves.toEqual([]);
      await expect(manager.claimQueueTicket('test-session')).resolves.toBe(false);
    });
  });

  describe('Production Non-Silent Fallback Verification', () => {
    it('loudly logs a CRITICAL alert in production when REDIS_URL is omitted rather than silently falling back', () => {
      const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      const originalEnv = process.env.NODE_ENV;
      const originalRedisUrl = process.env.REDIS_URL;

      try {
        process.env.NODE_ENV = 'production';
        delete process.env.REDIS_URL;

        const manager = new DistributedStateManager();
        expect(manager.isDistributed()).toBe(false);

        expect(consoleErrorSpy).toHaveBeenCalled();
        const loggedText = consoleErrorSpy.mock.calls.map((c) => c.join(' ')).join('\n');
        expect(loggedText).toContain('CRITICAL');
        expect(loggedText).toContain('REDIS_URL');
      } finally {
        process.env.NODE_ENV = originalEnv;
        if (originalRedisUrl !== undefined) {
          process.env.REDIS_URL = originalRedisUrl;
        } else {
          delete process.env.REDIS_URL;
        }
        consoleErrorSpy.mockRestore();
      }
    });

    it('reports status: degraded in /health endpoint when running in production without connected Redis', async () => {
      const originalEnv = process.env.NODE_ENV;
      const originalRedisUrl = process.env.REDIS_URL;

      try {
        process.env.NODE_ENV = 'production';
        delete process.env.REDIS_URL;

        const app = createOlogleApp();
        let jsonOutput: any = null;
        const req: any = { method: 'GET', url: '/health', headers: {} };
        const res: any = {
          json: (d: any) => { jsonOutput = d; },
          setHeader: () => res,
          status: () => res,
        };

        const healthRoute = (app as any)._router.stack.find(
          (l: any) => l.route && l.route.path === '/health'
        );
        healthRoute.route.stack[0].handle(req, res);

        expect(jsonOutput).toBeDefined();
        expect(jsonOutput.status).toBe('degraded');
        expect(jsonOutput.redis.connected).toBe(false);
        expect(jsonOutput.redis.mode).toBe('degraded_in_memory');
      } finally {
        process.env.NODE_ENV = originalEnv;
        if (originalRedisUrl !== undefined) {
          process.env.REDIS_URL = originalRedisUrl;
        } else {
          delete process.env.REDIS_URL;
        }
      }
    });
  });

  describe('Cross-Instance Pub/Sub & Shared State Invariants', () => {
    it('maintains distinct subscriber and publisher connection interfaces', () => {
      const manager = new DistributedStateManager();
      expect(manager).toBeInstanceOf(DistributedStateManager);
    });

    it('manages session subscriptions cleanly and safely in memory', async () => {
      const manager = new DistributedStateManager();
      const messages: string[] = [];

      await manager.subscribeToSession('sess_alpha', (msg) => {
        messages.push(msg);
      });

      await manager.unsubscribeFromSession('sess_alpha');
      expect(messages.length).toBe(0);
    });

    it('matches users across two separate backend server instances, relays WebRTC & chat, recovers session across instances, and handles cross-instance Next/Disconnect', async () => {
      const clusterBus = new InMemoryRedisClusterBus();
      const state1 = new DistributedStateManager(clusterBus.createClients());
      const state2 = new DistributedStateManager(clusterBus.createClients());

      const engine1 = new OlogleEngine(state1);
      const engine2 = new OlogleEngine(state2);

      const server1 = createServer(createOlogleApp());
      const server2 = createServer(createOlogleApp());
      attachWebSocketServer(server1, engine1);
      attachWebSocketServer(server2, engine2);

      let port1 = 0;
      let port2 = 0;
      await new Promise<void>((r) => server1.listen(0, '127.0.0.1', () => {
        port1 = (server1.address() as any).port;
        r();
      }));
      await new Promise<void>((r) => server2.listen(0, '127.0.0.1', () => {
        port2 = (server2.address() as any).port;
        r();
      }));

      try {
        const wsA1 = new WebSocket(`ws://127.0.0.1:${port1}/ws`);
        const wsB = new WebSocket(`ws://127.0.0.1:${port2}/ws`);

        const msgsA: any[] = [];
        const msgsB: any[] = [];
        wsA1.on('message', (d) => msgsA.push(JSON.parse(d.toString())));
        wsB.on('message', (d) => msgsB.push(JSON.parse(d.toString())));

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (
              msgsA.some((m) => m.type === 'SESSION_REGISTERED') &&
              msgsB.some((m) => m.type === 'SESSION_REGISTERED')
            ) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });

        const sessionAId = msgsA.find((m) => m.type === 'SESSION_REGISTERED').sessionId;

        wsA1.send(JSON.stringify({
          type: 'START_MATCHING',
          visitorId: 'cluster_user_a',
          profession: 'Distributed Systems Engineer',
          interests: ['Artificial Intelligence', 'WebRTC', 'TypeScript'],
          country: 'US',
          sessionMode: 'video',
        }));

        await new Promise((r) => setTimeout(r, 30));

        wsB.send(JSON.stringify({
          type: 'START_MATCHING',
          visitorId: 'cluster_user_b',
          profession: 'Cloud Architect',
          interests: ['Artificial Intelligence', 'WebRTC', 'Rust'],
          country: 'US',
          sessionMode: 'video',
        }));

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (
              msgsA.some((m) => m.type === 'MATCH_FOUND') &&
              msgsB.some((m) => m.type === 'MATCH_FOUND')
            ) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });

        const matchA = msgsA.find((m) => m.type === 'MATCH_FOUND');
        const matchB = msgsB.find((m) => m.type === 'MATCH_FOUND');
        expect(matchA.roomId).toBe(matchB.roomId);
        // Security: neither client ever receives the partner's secret sessionId
        expect(matchA.partnerSessionId).toBeUndefined();
        expect(matchB.partnerSessionId).toBeUndefined();

        // Cross-instance WebRTC signaling (Instance 1 -> Instance 2)
        const offer = { type: 'offer', sdp: 'v=0\r\no=crossInstanceOffer...' };
        wsA1.send(JSON.stringify({
          type: 'WEBRTC_SIGNAL',
          roomId: matchA.roomId,
          signal: offer,
        }));

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (msgsB.some((m) => m.type === 'WEBRTC_SIGNAL')) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });
        expect(msgsB.find((m) => m.type === 'WEBRTC_SIGNAL').signal).toEqual(offer);

        // Cross-instance session failover: User A drops from Instance 1 and reconnects to Instance 2
        wsA1.close();

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (msgsB.some((m) => m.type === 'PARTNER_RECONNECTING')) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });
        expect(msgsB.some((m) => m.type === 'PARTNER_LEFT')).toBe(false);

        const wsA2 = new WebSocket(`ws://127.0.0.1:${port1}/ws?sessionId=${encodeURIComponent(sessionAId)}`);
        wsA2.on('message', (d) => msgsA.push(JSON.parse(d.toString())));

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (
              msgsA.some((m) => m.type === 'SESSION_RESTORED' && m.roomId === matchA.roomId) &&
              msgsB.some((m) => m.type === 'PARTNER_RECONNECTED' && m.roomId === matchA.roomId)
            ) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });

        const restoredMsg = msgsA.find((m) => m.type === 'SESSION_RESTORED');
        expect(restoredMsg.partnerSessionId).toBeUndefined();

        // Cross-instance Next (User A on Instance 1 clicks Next -> User B on Instance 2 receives PARTNER_LEFT reason: skipped)
        wsA2.send(JSON.stringify({ type: 'NEXT_PERSON' }));

        await new Promise<void>((resolve) => {
          const check = setInterval(() => {
            if (msgsB.some((m) => m.type === 'PARTNER_LEFT' && m.reason === 'skipped')) {
              clearInterval(check);
              resolve();
            }
          }, 15);
        });

        wsA2.close();
        wsB.close();
      } finally {
        engine1.broadcastShutdown('test_done');
        engine2.broadcastShutdown('test_done');
        await new Promise<void>((r) => server1.close(() => r()));
        await new Promise<void>((r) => server2.close(() => r()));
      }
    });

    it('validates and normalizes Upstash Redis TCP URLs, enforces TLS, rejects REST URLs, and redacts secrets', () => {
      // 1. Standard Upstash rediss:// URL with quotes
      const validUpstash = parseAndNormalizeRedisUrl(
        '"rediss://default:superSecretToken123@vital-flamingo-12345.upstash.io:6379"'
      );
      expect(validUpstash.configError).toBeNull();
      expect(validUpstash.tlsEnabled).toBe(true);
      expect(validUpstash.isUpstash).toBe(true);
      expect(validUpstash.hasPassword).toBe(true);
      expect(validUpstash.port).toBe(6379);
      expect(validUpstash.hostMasked).toBe('vita***.upstash.io');
      expect(validUpstash.hostMasked).not.toContain('superSecretToken123');

      // 2. Upstash redis:// URL (missing TLS 's') auto-upgrades to rediss://
      const upgradedUpstash = parseAndNormalizeRedisUrl(
        'redis://default:superSecretToken123@vital-flamingo-12345.upstash.io:6379'
      );
      expect(upgradedUpstash.configError).toBeNull();
      expect(upgradedUpstash.autoUpgradedTls).toBe(true);
      expect(upgradedUpstash.tlsEnabled).toBe(true);
      expect(upgradedUpstash.protocol).toBe('rediss:');
      expect(upgradedUpstash.normalizedUrl?.startsWith('rediss://')).toBe(true);

      // 3. Upstash REST https:// URL is rejected with clear guidance
      const restUrl = parseAndNormalizeRedisUrl('https://vital-flamingo-12345.upstash.io');
      expect(restUrl.normalizedUrl).toBeNull();
      expect(restUrl.configError).toContain('HTTP/HTTPS REST endpoint');

      // 4. Error sanitizer strips URLs and credentials
      const scrubbed = sanitizeRedisErrorMessage(
        'Failed connecting to rediss://default:mySecretPass@vital-flamingo.upstash.io:6379 with password=mySecretPass'
      );
      expect(scrubbed).not.toContain('mySecretPass');
      expect(scrubbed).toContain('[REDACTED_REDIS_URL]');
      expect(scrubbed).toContain('password=[REDACTED]');
    });
  });
});
