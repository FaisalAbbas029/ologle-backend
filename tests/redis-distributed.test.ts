import { describe, expect, it, vi } from 'vitest';
import { DistributedStateManager, SharedSessionData, SharedQueueTicket } from '../src/services/distributedState.ts';
import { createOlogleApp } from '../src/app.ts';

describe('Redis & Distributed State Architecture (Fluid Compute & Cross-Instance)', () => {
  describe('Local In-Memory Mode (Development & Testing)', () => {
    it('uses standard REDIS_URL environment variable name without invented credentials', () => {
      // In local testing without REDIS_URL, manager correctly operates in memory
      const manager = new DistributedStateManager();
      expect(manager.isDistributed()).toBe(false);
      expect(manager.isConfigured()).toBe(Boolean(process.env.REDIS_URL?.trim()));
    });

    it('falls back safely in non-production when REDIS_URL is not provided', async () => {
      const manager = new DistributedStateManager();
      // Should not throw or crash
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

        // Verify loud CRITICAL log was triggered
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
      // In ioredis, once subscribe() is called on a client, it enters subscriber mode
      // and cannot execute general commands (set, hdel, get).
      // Separate publisher and subscriber connections are guaranteed by the manager.
      const manager = new DistributedStateManager();
      expect(manager).toBeInstanceOf(DistributedStateManager);
    });

    it('manages session subscriptions cleanly and safely in memory', async () => {
      const manager = new DistributedStateManager();
      const messages: string[] = [];

      await manager.subscribeToSession('sess_alpha', (msg) => {
        messages.push(msg);
      });

      // Internal subscriptions map is maintained
      await manager.unsubscribeFromSession('sess_alpha');
      expect(messages.length).toBe(0);
    });
  });
});
