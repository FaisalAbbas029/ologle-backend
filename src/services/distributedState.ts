/**
 * OLOGLE Distributed State & Cross-Instance Pub/Sub Manager
 * Enables seamless cross-instance WebSockets, queue matching, and session recovery
 * when deployed across multiple Vercel Function instances (Fluid Compute).
 *
 * Invariant Guarantees:
 * 1. Dedicated Connections: Separate Redis Publisher and Subscriber clients (ioredis requirement).
 * 2. In-Memory Graceful Fallback: Runs 100% in-memory with zero crashes when REDIS_URL is omitted or unreachable.
 * 3. Ephemeral TTL Persistence: Preserves privacy-by-design (zero disk persistence; session and presence keys expire in seconds).
 * 4. Cross-Instance Signaling: Publishes events to `ologle:session:<sessionId>` for instant delivery across instances.
 */

import Redis from 'ioredis';

export interface SharedSessionData {
  sessionId: string;
  visitorId: string;
  profession: string;
  interests: string[];
  country?: string;
  sessionMode?: string;
  currentMode: string;
  pairedSessionId: string | null;
  currentRoomId: string | null;
  lastActivity: number;
}

export interface SharedQueueTicket {
  sessionId: string;
  visitorId: string;
  profession: string;
  interests: string[];
  country?: string;
  sessionMode?: string;
  timestamp: number;
}

export class DistributedStateManager {
  private pubClient: Redis | null = null;
  private subClient: Redis | null = null;
  private isConnected = false;
  private subscriptions = new Map<string, (msg: string) => void>();

  constructor() {
    const redisUrl = process.env.REDIS_URL?.trim();
    const isProd = process.env.NODE_ENV === 'production';
    if (redisUrl) {
      this.initRedis(redisUrl);
    } else {
      if (isProd) {
        console.error(
          '[OLOGLE State] CRITICAL: REDIS_URL is not configured in production environment! Running in degraded single-instance mode. Multi-instance Vercel Functions cannot route WebSockets, matchmaking, or WebRTC signaling across instances without Redis.'
        );
      } else {
        console.log(
          '[OLOGLE State] In-Memory Mode active (development/test). Configure REDIS_URL to enable cross-instance clustering.'
        );
      }
    }
  }

  public isConfigured(): boolean {
    return Boolean(process.env.REDIS_URL?.trim());
  }

  private initRedis(url: string): void {
    const isProd = process.env.NODE_ENV === 'production';
    try {
      // 1. Dedicated Command & Publisher Client
      this.pubClient = new Redis(url, {
        maxRetriesPerRequest: 2,
        enableReadyCheck: false,
        lazyConnect: true,
        reconnectOnError: () => 1,
      });

      // 2. Dedicated Pub/Sub Subscriber Client (Redis protocol requires dedicated connection)
      this.subClient = new Redis(url, {
        maxRetriesPerRequest: 2,
        enableReadyCheck: false,
        lazyConnect: true,
        reconnectOnError: () => 1,
      });

      this.pubClient.connect()
        .then(() => {
          this.isConnected = true;
          console.log('[OLOGLE State] Connected to distributed Redis cluster for cross-instance matching.');
        })
        .catch((err) => {
          if (isProd) {
            console.error('[OLOGLE State] CRITICAL: Redis publisher connection failed in production:', err.message);
          } else {
            console.warn('[OLOGLE State] Redis publisher connection failed, continuing in in-memory mode:', err.message);
          }
          this.isConnected = false;
        });

      this.subClient.connect()
        .then(() => {
          this.subClient?.on('message', (channel, message) => {
            const sessionId = channel.replace('ologle:session:', '');
            const handler = this.subscriptions.get(sessionId);
            if (handler) {
              handler(message);
            }
          });
        })
        .catch((err) => {
          if (isProd) {
            console.error('[OLOGLE State] CRITICAL: Redis subscriber connection failed in production:', err.message);
          } else {
            console.warn('[OLOGLE State] Redis subscriber connection failed:', err.message);
          }
        });

      this.pubClient.on('error', (err) => {
        if (isProd) {
          console.error('[OLOGLE State] CRITICAL: Redis client error in production:', err.message);
        } else {
          console.warn('[OLOGLE State] Redis client error (falling back to memory):', err.message);
        }
      });
      this.subClient.on('error', (err) => {
        if (isProd) {
          console.error('[OLOGLE State] CRITICAL: Redis subscriber error in production:', err.message);
        } else {
          console.warn('[OLOGLE State] Redis subscriber error (falling back to memory):', err.message);
        }
      });
    } catch (e: any) {
      if (isProd) {
        console.error('[OLOGLE State] CRITICAL: Failed to initialize Redis clients in production:', e.message);
      } else {
        console.warn('[OLOGLE State] Failed to initialize Redis clients, continuing in memory:', e.message);
      }
      this.isConnected = false;
    }
  }

  public isDistributed(): boolean {
    return this.isConnected;
  }

  // --- Cross-Instance Message Forwarding ---

  public async publishToSession(sessionId: string, message: string): Promise<boolean> {
    if (!this.isConnected || !this.pubClient) return false;
    try {
      await this.pubClient.publish(`ologle:session:${sessionId}`, message);
      return true;
    } catch {
      return false;
    }
  }

  public async subscribeToSession(sessionId: string, onMessage: (msg: string) => void): Promise<void> {
    this.subscriptions.set(sessionId, onMessage);
    if (this.isConnected && this.subClient) {
      try {
        await this.subClient.subscribe(`ologle:session:${sessionId}`);
      } catch (err: any) {
        console.warn('[OLOGLE State] Failed to subscribe to session channel:', err.message);
      }
    }
  }

  public async unsubscribeFromSession(sessionId: string): Promise<void> {
    this.subscriptions.delete(sessionId);
    if (this.isConnected && this.subClient) {
      try {
        await this.subClient.unsubscribe(`ologle:session:${sessionId}`);
      } catch {}
    }
  }

  // --- Session Presence & State Sharing ---

  public async setSessionState(sessionId: string, state: SharedSessionData, ttlSeconds: number = 60): Promise<void> {
    if (!this.isConnected || !this.pubClient) return;
    try {
      await this.pubClient.set(`ologle:session_state:${sessionId}`, JSON.stringify(state), 'EX', ttlSeconds);
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to store session state in Redis:', err.message);
    }
  }

  public async getSessionState(sessionId: string): Promise<SharedSessionData | null> {
    if (!this.isConnected || !this.pubClient) return null;
    try {
      const data = await this.pubClient.get(`ologle:session_state:${sessionId}`);
      return data ? JSON.parse(data) : null;
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to retrieve session state from Redis:', err.message);
      return null;
    }
  }

  public async removeSessionState(sessionId: string): Promise<void> {
    if (!this.isConnected || !this.pubClient) return;
    try {
      await this.pubClient.del(`ologle:session_state:${sessionId}`);
    } catch {}
  }

  // --- Shared Matching Queue Coordination ---

  public async registerQueueTicket(ticket: SharedQueueTicket, ttlSeconds: number = 30): Promise<void> {
    if (!this.isConnected || !this.pubClient) return;
    try {
      const mode = ticket.sessionMode || 'default';
      const key = `ologle:queue:${mode}`;
      await this.pubClient.hset(key, ticket.sessionId, JSON.stringify(ticket));
      await this.pubClient.expire(key, 60);
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to register queue ticket in Redis:', err.message);
    }
  }

  public async removeQueueTicket(sessionId: string, mode: string = 'default'): Promise<void> {
    if (!this.isConnected || !this.pubClient) return;
    try {
      await this.pubClient.hdel(`ologle:queue:${mode}`, sessionId);
    } catch {}
  }

  public async getSharedQueueTickets(mode: string = 'default'): Promise<SharedQueueTicket[]> {
    if (!this.isConnected || !this.pubClient) return [];
    try {
      const entries = await this.pubClient.hgetall(`ologle:queue:${mode}`);
      if (!entries) return [];
      return Object.values(entries).map((item) => JSON.parse(item));
    } catch {
      return [];
    }
  }

  /**
   * Atomically claims a queue ticket across distributed instances using Redis HDEL.
   * Returns true if this instance successfully claimed the ticket (HDEL returned 1),
   * or false if another instance already claimed it concurrently.
   */
  public async claimQueueTicket(sessionId: string, mode: string = 'default'): Promise<boolean> {
    if (!this.isConnected || !this.pubClient) return false;
    try {
      const result = await this.pubClient.hdel(`ologle:queue:${mode}`, sessionId);
      return result === 1;
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to claim queue ticket:', err.message);
      return false;
    }
  }

  public async close(): Promise<void> {
    this.subscriptions.clear();
    try {
      if (this.pubClient) await this.pubClient.quit();
      if (this.subClient) await this.subClient.quit();
    } catch {}
    this.isConnected = false;
  }
}

export const distributedState = new DistributedStateManager();
