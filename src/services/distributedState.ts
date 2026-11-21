/**
 * OLOGLE Distributed State & Cross-Instance Pub/Sub Manager
 * Enables seamless cross-instance WebSockets, queue matching, and session recovery
 * when deployed across multiple backend instances.
 *
 * Invariant Guarantees:
 * 1. Dedicated Connections: Separate Redis Publisher and Subscriber clients (ioredis requirement).
 * 2. Non-Silent Production Alerting: Logs CRITICAL errors and flags degraded status in production if Redis is omitted or unreachable.
 * 3. Ephemeral TTL Persistence: Preserves privacy-by-design (zero disk persistence; session and presence keys expire in seconds).
 * 4. Cross-Instance Signaling: Publishes events to `ologle:session:<sessionId>` for instant delivery across instances.
 */

import Redis, { type RedisOptions } from 'ioredis';

export interface SharedSessionData {
  sessionId: string;
  visitorId: string;
  ipAddress?: string;
  profession: string;
  interests: string[];
  country?: string;
  sessionMode?: string;
  currentMode: string;
  pairedSessionId: string | null;
  currentRoomId: string | null;
  lastActivity: number;
  isRecovering?: boolean;
  partnerProfession?: string;
  partnerCountry?: string;
  partnerVisitorId?: string;
  sharedInterests?: string[];
  sharedTopics?: string[];
  matchScore?: number;
  scoreBreakdown?: any;
  isInitiator?: boolean;
  endedWhileOfflineReason?: 'skipped' | 'ended' | 'disconnected' | 'reported' | 'blocked' | null;
  blockedSessionIds?: string[];
  blockedVisitorIds?: string[];
}

export interface SharedQueueTicket {
  sessionId: string;
  visitorId: string;
  ipAddress?: string;
  profession: string;
  strictProfessionFilter?: boolean;
  interests: string[];
  country?: string;
  sessionMode?: string;
  timestamp: number;
  blockedSessionIds?: string[];
  blockedVisitorIds?: string[];
}

export interface RedisConnectionDiagnostics {
  configured: boolean;
  connected: boolean;
  publisherStatus: string;
  subscriberStatus: string;
  protocol: string | null;
  tlsEnabled: boolean;
  hostMasked: string | null;
  port: number | null;
  isUpstash: boolean;
  hasPassword: boolean;
  configError: string | null;
  lastError: string | null;
}

export interface NormalizedRedisConfig {
  normalizedUrl: string | null;
  protocol: string | null;
  tlsEnabled: boolean;
  hostname: string | null;
  hostMasked: string | null;
  port: number | null;
  isUpstash: boolean;
  hasPassword: boolean;
  autoUpgradedTls: boolean;
  configError: string | null;
}

/**
 * Redacts any credentials, passwords, or URLs from error strings so logs and
 * health diagnostics never expose secrets.
 */
export function sanitizeRedisErrorMessage(message: string | undefined | null): string {
  if (!message) return 'Unknown Redis error';
  return message
    .replace(/rediss?:\/\/[^\s'"]+/gi, '[REDACTED_REDIS_URL]')
    .replace(/https?:\/\/[^\s'"]+/gi, '[REDACTED_HTTP_URL]')
    .replace(/(password|token|secret|auth)[=:\s]+[^\s,;]+/gi, '$1=[REDACTED]');
}

function maskHostname(hostname: string): string {
  if (!hostname) return 'unknown';
  const parts = hostname.split('.');
  if (parts.length >= 3) {
    const first = parts[0];
    const maskedFirst = first.length > 4 ? `${first.slice(0, 4)}***` : '***';
    return [maskedFirst, ...parts.slice(1)].join('.');
  }
  return hostname.length > 4 ? `${hostname.slice(0, 4)}***` : '***';
}

/**
 * Validates and normalizes REDIS_URL for ioredis + Upstash Redis TCP connections.
 * Never returns or logs raw passwords or full connection strings in diagnostics.
 */
export function parseAndNormalizeRedisUrl(rawInput: string | undefined): NormalizedRedisConfig {
  if (!rawInput || !rawInput.trim()) {
    return {
      normalizedUrl: null,
      protocol: null,
      tlsEnabled: false,
      hostname: null,
      hostMasked: null,
      port: null,
      isUpstash: false,
      hasPassword: false,
      autoUpgradedTls: false,
      configError: null,
    };
  }

  let cleaned = rawInput.trim();

  // Strip accidental surrounding quotes from copy-pasted Vercel env vars
  if (
    (cleaned.startsWith('"') && cleaned.endsWith('"')) ||
    (cleaned.startsWith("'") && cleaned.endsWith("'"))
  ) {
    cleaned = cleaned.slice(1, -1).trim();
  }

  // Strip accidental `redis-cli` command prefix if user pasted full CLI command from Upstash console
  if (cleaned.startsWith('redis-cli ')) {
    const urlMatch = cleaned.match(/(rediss?:\/\/[^\s'"]+)/i);
    if (urlMatch) {
      cleaned = urlMatch[1];
    }
  }

  let parsed: URL;
  try {
    parsed = new URL(cleaned);
  } catch {
    return {
      normalizedUrl: null,
      protocol: null,
      tlsEnabled: false,
      hostname: null,
      hostMasked: null,
      port: null,
      isUpstash: false,
      hasPassword: false,
      autoUpgradedTls: false,
      configError:
        'Invalid REDIS_URL format. Expected Upstash TCP URL: rediss://default:<password>@<endpoint>.upstash.io:6379',
    };
  }

  const hostname = parsed.hostname.toLowerCase();
  const isUpstash = hostname.endsWith('.upstash.io');
  const hostMasked = maskHostname(hostname);
  const hasPassword = Boolean(parsed.password);
  const port = parsed.port ? Number(parsed.port) : 6379;

  if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
    return {
      normalizedUrl: null,
      protocol: parsed.protocol,
      tlsEnabled: parsed.protocol === 'https:',
      hostname,
      hostMasked,
      port: parsed.port ? Number(parsed.port) : 443,
      isUpstash,
      hasPassword,
      autoUpgradedTls: false,
      configError:
        'REDIS_URL is set to an HTTP/HTTPS REST endpoint. OLOGLE requires real-time Redis Pub/Sub over TCP; use the Upstash TCP URL starting with rediss://default:<password>@<endpoint>.upstash.io:6379',
    };
  }

  if (parsed.protocol !== 'redis:' && parsed.protocol !== 'rediss:') {
    return {
      normalizedUrl: null,
      protocol: parsed.protocol,
      tlsEnabled: false,
      hostname,
      hostMasked,
      port,
      isUpstash,
      hasPassword,
      autoUpgradedTls: false,
      configError: `Unsupported REDIS_URL protocol "${parsed.protocol}". Expected rediss:// (or redis:// for local development).`,
    };
  }

  // Upstash Redis endpoints strictly require TLS (rediss://) on port 6379
  let autoUpgradedTls = false;
  if (isUpstash && parsed.protocol === 'redis:') {
    parsed.protocol = 'rediss:';
    autoUpgradedTls = true;
  }

  if (!parsed.port) {
    parsed.port = '6379';
  }

  const tlsEnabled = parsed.protocol === 'rediss:';

  return {
    normalizedUrl: parsed.toString(),
    protocol: parsed.protocol,
    tlsEnabled,
    hostname,
    hostMasked,
    port,
    isUpstash,
    hasPassword,
    autoUpgradedTls,
    configError: isUpstash && !hasPassword
      ? 'Upstash REDIS_URL is missing a password in rediss://default:<password>@<host>:6379'
      : null,
  };
}

export class DistributedStateManager {
  private pubClient: Redis | null = null;
  private subClient: Redis | null = null;
  private isConnected = false;
  private usingCustomClients = false;
  private normalizedConfig: NormalizedRedisConfig;
  private lastError: string | null = null;
  private connectPromise: Promise<boolean> | null = null;
  private subscriptions = new Map<string, (msg: string) => void>();

  constructor(customClients?: { pubClient: Redis; subClient: Redis }) {
    if (customClients) {
      this.pubClient = customClients.pubClient;
      this.subClient = customClients.subClient;
      this.isConnected = true;
      this.usingCustomClients = true;
      this.normalizedConfig = {
        normalizedUrl: null,
        protocol: 'custom:',
        tlsEnabled: false,
        hostname: 'in-memory-test-bus',
        hostMasked: 'in-memory-test-bus',
        port: 6379,
        isUpstash: false,
        hasPassword: false,
        autoUpgradedTls: false,
        configError: null,
      };
      this.bindSubscriberEvents();
      return;
    }

    const rawRedisUrl = process.env.REDIS_URL;
    const isProd = process.env.NODE_ENV === 'production';
    this.normalizedConfig = parseAndNormalizeRedisUrl(rawRedisUrl);

    if (this.normalizedConfig.configError) {
      this.lastError = this.normalizedConfig.configError;
      console.error(
        `[OLOGLE State] CRITICAL: ${this.normalizedConfig.configError} (host=${this.normalizedConfig.hostMasked || 'none'}, protocol=${this.normalizedConfig.protocol || 'none'})`
      );
      if (!this.normalizedConfig.normalizedUrl) {
        return;
      }
    }

    if (this.normalizedConfig.autoUpgradedTls) {
      console.warn(
        `[OLOGLE State] Auto-upgraded Upstash REDIS_URL from redis:// to rediss:// (TLS is required for ${this.normalizedConfig.hostMasked}:${this.normalizedConfig.port}).`
      );
    }

    if (this.normalizedConfig.normalizedUrl) {
      this.initRedis(this.normalizedConfig.normalizedUrl);
    } else {
      if (isProd) {
        console.error(
          '[OLOGLE State] CRITICAL: REDIS_URL is not configured in production environment! Running in degraded single-instance mode. Multi-instance deployments cannot route WebSockets, matchmaking, or WebRTC signaling across instances without Redis.'
        );
      } else {
        console.log(
          '[OLOGLE State] In-Memory Mode active (development/test). Configure REDIS_URL to enable cross-instance clustering.'
        );
      }
    }
  }

  public isConfigured(): boolean {
    return Boolean(process.env.REDIS_URL?.trim()) || this.isConnected;
  }

  public getDiagnostics(): RedisConnectionDiagnostics {
    return {
      configured: this.isConfigured(),
      connected: this.isConnected,
      publisherStatus: this.usingCustomClients ? 'ready' : (this.pubClient?.status || 'uninitialized'),
      subscriberStatus: this.usingCustomClients ? 'ready' : (this.subClient?.status || 'uninitialized'),
      protocol: this.normalizedConfig.protocol,
      tlsEnabled: this.normalizedConfig.tlsEnabled,
      hostMasked: this.normalizedConfig.hostMasked,
      port: this.normalizedConfig.port,
      isUpstash: this.normalizedConfig.isUpstash,
      hasPassword: this.normalizedConfig.hasPassword,
      configError: this.normalizedConfig.configError,
      lastError: this.lastError,
    };
  }

  private bindSubscriberEvents(): void {
    if (!this.subClient) return;
    this.subClient.on('message', (channel: string, message: string) => {
      const sessionId = channel.replace('ologle:session:', '');
      const handler = this.subscriptions.get(sessionId);
      if (handler) {
        handler(message);
      }
    });
    for (const sessionId of this.subscriptions.keys()) {
      this.subClient.subscribe(`ologle:session:${sessionId}`).catch(() => {});
    }
  }

  private createClientOptions(): RedisOptions {
    const { tlsEnabled, hostname } = this.normalizedConfig;
    const options: RedisOptions = {
      connectTimeout: 10000,
      commandTimeout: 5000,
      keepAlive: 10000,
      maxRetriesPerRequest: 2,
      enableReadyCheck: false,
      lazyConnect: true,
      family: 4,
      retryStrategy: (times: number) => {
        if (times > 5) {
          return null;
        }
        return Math.min(times * 250, 1500);
      },
      reconnectOnError: (err: Error) => {
        const msg = err?.message || '';
        if (
          msg.includes('READONLY') ||
          msg.includes('ETIMEDOUT') ||
          msg.includes('ECONNRESET') ||
          msg.includes('EPIPE')
        ) {
          return 1;
        }
        return false;
      },
    };

    if (tlsEnabled) {
      options.tls = hostname ? { servername: hostname } : {};
    }

    return options;
  }

  private updateConnectionFlag(): void {
    if (this.usingCustomClients) {
      this.isConnected = true;
      return;
    }
    const pubOk = this.pubClient?.status === 'ready' || this.pubClient?.status === 'connect';
    const subOk = this.subClient?.status === 'ready' || this.subClient?.status === 'connect';
    this.isConnected = Boolean(pubOk && subOk);
  }

  private attachClientLifecycleListeners(): void {
    const isProd = process.env.NODE_ENV === 'production';
    const diagSummary = `host=${this.normalizedConfig.hostMasked}:${this.normalizedConfig.port}, tls=${this.normalizedConfig.tlsEnabled}, upstash=${this.normalizedConfig.isUpstash}`;

    if (this.pubClient) {
      this.pubClient.on('ready', () => {
        this.lastError = null;
        this.updateConnectionFlag();
      });
      this.pubClient.on('connect', () => {
        this.updateConnectionFlag();
      });
      this.pubClient.on('close', () => {
        this.updateConnectionFlag();
      });
      this.pubClient.on('end', () => {
        this.updateConnectionFlag();
      });
      this.pubClient.on('error', (err: any) => {
        const safeMsg = sanitizeRedisErrorMessage(err?.message);
        const code = err?.code ? ` [code=${err.code}]` : '';
        this.lastError = `publisher: ${safeMsg}${code}`;
        if (isProd) {
          console.error(`[OLOGLE State] CRITICAL: Redis publisher error in production (${diagSummary}): ${safeMsg}${code}`);
        } else {
          console.warn(`[OLOGLE State] Redis publisher error (${diagSummary}): ${safeMsg}${code}`);
        }
      });
    }

    if (this.subClient) {
      this.subClient.on('ready', () => {
        this.lastError = null;
        this.updateConnectionFlag();
        // Re-subscribe all active local sessions whenever the subscriber connects or reconnects
        for (const sessionId of this.subscriptions.keys()) {
          this.subClient?.subscribe(`ologle:session:${sessionId}`).catch(() => {});
        }
      });
      this.subClient.on('connect', () => {
        this.updateConnectionFlag();
      });
      this.subClient.on('close', () => {
        this.updateConnectionFlag();
      });
      this.subClient.on('end', () => {
        this.updateConnectionFlag();
      });
      this.subClient.on('error', (err: any) => {
        const safeMsg = sanitizeRedisErrorMessage(err?.message);
        const code = err?.code ? ` [code=${err.code}]` : '';
        this.lastError = `subscriber: ${safeMsg}${code}`;
        if (isProd) {
          console.error(`[OLOGLE State] CRITICAL: Redis subscriber error in production (${diagSummary}): ${safeMsg}${code}`);
        } else {
          console.warn(`[OLOGLE State] Redis subscriber error (${diagSummary}): ${safeMsg}${code}`);
        }
      });
    }
  }

  private initRedis(url: string): void {
    const isProd = process.env.NODE_ENV === 'production';
    const diagSummary = `host=${this.normalizedConfig.hostMasked}:${this.normalizedConfig.port}, tls=${this.normalizedConfig.tlsEnabled}, upstash=${this.normalizedConfig.isUpstash}`;
    try {
      const clientOptions = this.createClientOptions();

      // 1. Dedicated Command & Publisher Client
      this.pubClient = new Redis(url, clientOptions);

      // 2. Dedicated Pub/Sub Subscriber Client (Redis protocol requires dedicated connection)
      this.subClient = new Redis(url, clientOptions);

      this.bindSubscriberEvents();
      this.attachClientLifecycleListeners();

      void this.ensureConnected(5000);
    } catch (e: any) {
      const safeMsg = sanitizeRedisErrorMessage(e?.message);
      this.lastError = safeMsg;
      if (isProd) {
        console.error(`[OLOGLE State] CRITICAL: Failed to initialize Redis clients in production (${diagSummary}): ${safeMsg}`);
      } else {
        console.warn(`[OLOGLE State] Failed to initialize Redis clients (${diagSummary}): ${safeMsg}`);
      }
      this.isConnected = false;
    }
  }

  private async connectSingleClient(client: Redis, role: 'publisher' | 'subscriber'): Promise<void> {
    const status = client.status;
    if (status === 'ready' || status === 'connect') {
      return;
    }
    if (status === 'connecting' || status === 'reconnecting') {
      await new Promise<void>((resolve, reject) => {
        const onReady = () => {
          cleanup();
          resolve();
        };
        const onError = (err: Error) => {
          cleanup();
          reject(err);
        };
        const onClose = () => {
          cleanup();
          reject(new Error(`Redis ${role} connection closed during handshake`));
        };
        const cleanup = () => {
          client.off('ready', onReady);
          client.off('error', onError);
          client.off('close', onClose);
          client.off('end', onClose);
        };
        client.once('ready', onReady);
        client.once('error', onError);
        client.once('close', onClose);
        client.once('end', onClose);
      });
      return;
    }
    // Status is 'wait', 'close', or 'end' -> initiate fresh connection
    await client.connect();
  }

  /**
   * Ensures both publisher and subscriber Redis clients are connected.
   * Crucial for Vercel Serverless Functions so cold-start health/API calls do not
   * freeze the container mid-handshake and thawed instances recover closed sockets.
   */
  public async ensureConnected(timeoutMs: number = 3500): Promise<boolean> {
    if (this.usingCustomClients) {
      return true;
    }
    if (!this.pubClient || !this.subClient || !this.normalizedConfig.normalizedUrl) {
      return false;
    }
    this.updateConnectionFlag();
    if (this.isConnected) {
      return true;
    }

    if (!this.connectPromise) {
      const isProd = process.env.NODE_ENV === 'production';
      const diagSummary = `host=${this.normalizedConfig.hostMasked}:${this.normalizedConfig.port}, tls=${this.normalizedConfig.tlsEnabled}`;

      this.connectPromise = (async () => {
        try {
          // If a client reached permanent 'end' state after retry exhaustion, recreate both clients cleanly
          if (this.pubClient?.status === 'end' || this.subClient?.status === 'end') {
            try {
              this.pubClient?.disconnect();
              this.subClient?.disconnect();
            } catch {}
            const clientOptions = this.createClientOptions();
            this.pubClient = new Redis(this.normalizedConfig.normalizedUrl!, clientOptions);
            this.subClient = new Redis(this.normalizedConfig.normalizedUrl!, clientOptions);
            this.bindSubscriberEvents();
            this.attachClientLifecycleListeners();
          }

          await Promise.all([
            this.connectSingleClient(this.pubClient!, 'publisher'),
            this.connectSingleClient(this.subClient!, 'subscriber'),
          ]);

          this.updateConnectionFlag();
          if (this.isConnected) {
            this.lastError = null;
            console.log(`[OLOGLE State] Connected publisher & subscriber to distributed Redis (${diagSummary}).`);
          }
          return this.isConnected;
        } catch (err: any) {
          const safeMsg = sanitizeRedisErrorMessage(err?.message);
          this.lastError = safeMsg;
          this.updateConnectionFlag();
          if (isProd) {
            console.error(`[OLOGLE State] CRITICAL: Redis connection failed in production (${diagSummary}): ${safeMsg}`);
          } else {
            console.warn(`[OLOGLE State] Redis connection failed (${diagSummary}), using in-memory fallback: ${safeMsg}`);
          }
          return false;
        } finally {
          this.connectPromise = null;
        }
      })();
    }

    let timeoutHandle: NodeJS.Timeout | null = null;
    try {
      const timeoutPromise = new Promise<boolean>((resolve) => {
        timeoutHandle = setTimeout(() => resolve(this.isConnected), timeoutMs);
      });
      return await Promise.race([this.connectPromise, timeoutPromise]);
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
  }

  public isDistributed(): boolean {
    this.updateConnectionFlag();
    return this.isConnected;
  }

  private async isReadyOrReconnect(): Promise<boolean> {
    if (this.isConnected) return true;
    if (!this.normalizedConfig.normalizedUrl) return false;
    return this.ensureConnected(1500);
  }

  // --- Cross-Instance Message Forwarding ---

  public async publishToSession(sessionId: string, message: string): Promise<boolean> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return false;
    if (!this.pubClient) return false;
    try {
      const receivers = await this.pubClient.publish(`ologle:session:${sessionId}`, message);
      return Number(receivers) > 0;
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to publish to session channel:', sanitizeRedisErrorMessage(err?.message));
      return false;
    }
  }

  public async ensureSessionSubscribed(sessionId: string): Promise<void> {
    if (!this.subscriptions.has(sessionId)) return;
    if (!this.isConnected && this.normalizedConfig.normalizedUrl) {
      await this.isReadyOrReconnect();
    }
    if (this.isConnected && this.subClient) {
      try {
        await this.subClient.subscribe(`ologle:session:${sessionId}`);
      } catch (err: any) {
        console.warn('[OLOGLE State] Failed to ensure session subscription:', sanitizeRedisErrorMessage(err?.message));
      }
    }
  }

  public async subscribeToSession(sessionId: string, onMessage: (msg: string) => void): Promise<void> {
    this.subscriptions.set(sessionId, onMessage);
    if (!this.isConnected && this.normalizedConfig.normalizedUrl) {
      await this.isReadyOrReconnect();
    }
    if (this.isConnected && this.subClient) {
      try {
        await this.subClient.subscribe(`ologle:session:${sessionId}`);
      } catch (err: any) {
        console.warn('[OLOGLE State] Failed to subscribe to session channel:', sanitizeRedisErrorMessage(err?.message));
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
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return;
    if (!this.pubClient) return;
    try {
      await this.pubClient.set(`ologle:session_state:${sessionId}`, JSON.stringify(state), 'EX', ttlSeconds);
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to store session state in Redis:', sanitizeRedisErrorMessage(err?.message));
    }
  }

  public async getSessionState(sessionId: string): Promise<SharedSessionData | null> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return null;
    if (!this.pubClient) return null;
    try {
      const data = await this.pubClient.get(`ologle:session_state:${sessionId}`);
      return data ? JSON.parse(data) : null;
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to retrieve session state from Redis:', sanitizeRedisErrorMessage(err?.message));
      return null;
    }
  }

  public async removeSessionState(sessionId: string): Promise<void> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return;
    if (!this.pubClient) return;
    try {
      await this.pubClient.del(`ologle:session_state:${sessionId}`);
    } catch {}
  }

  // --- Shared Matching Queue Coordination ---

  public async registerQueueTicket(ticket: SharedQueueTicket, ttlSeconds: number = 60): Promise<void> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return;
    if (!this.pubClient) return;
    try {
      const mode = ticket.sessionMode || 'default';
      const key = `ologle:queue:${mode}`;
      await this.pubClient.hset(key, ticket.sessionId, JSON.stringify(ticket));
      await this.pubClient.expire(key, ttlSeconds);
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to register queue ticket in Redis:', sanitizeRedisErrorMessage(err?.message));
    }
  }

  public async removeQueueTicket(sessionId: string, mode: string = 'default'): Promise<void> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return;
    if (!this.pubClient) return;
    try {
      await this.pubClient.hdel(`ologle:queue:${mode}`, sessionId);
    } catch {}
  }

  public async getSharedQueueTickets(mode: string = 'default'): Promise<SharedQueueTicket[]> {
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return [];
    if (!this.pubClient) return [];
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
    if (!this.isConnected && !(await this.isReadyOrReconnect())) return false;
    if (!this.pubClient) return false;
    try {
      const result = await this.pubClient.hdel(`ologle:queue:${mode}`, sessionId);
      return result === 1;
    } catch (err: any) {
      console.warn('[OLOGLE State] Failed to claim queue ticket:', sanitizeRedisErrorMessage(err?.message));
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
