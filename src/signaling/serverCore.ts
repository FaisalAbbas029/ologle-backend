/**
 * OLOGLE Server Core & WebSocket Protocol Handler
 * Multi-Modal Real-Time Architecture: Video Chat, Audio Chat, and Text Messaging.
 *
 * Privacy-by-Design Invariants:
 * 1. Zero Disk Persistence: No messages, media, or logs are ever written to disk or databases.
 * 2. Multi-Modal Isolation: WebRTC Audio and Video streams flow strictly Peer-to-Peer (P2P);
 *    the backend processes zero audio/video bytes. Text messages are relayed in volatile RAM and discarded.
 * 3. Volatile RAM Scrubbing: Active sessions exist only in volatile V8 RAM and are scrubbed upon disconnect.
 * 4. Ephemeral IP Anonymization: IP addresses are one-way hashed with a cryptographic salt rotated every 24h.
 */

import crypto from 'crypto';
import { WebSocket } from 'ws';
import { defaultSafetyAgent, type SafetyClassificationResult } from '../services/safetyAgent.js';
import { MatchmakerQueue, calculateCompatibility } from '../matchmaking/matchmaker.js';
import type { QueuedSession, MatchScoreResult } from '../matchmaking/matchmaker.js';
import { validateProfession, validateInterests } from '../validation/index.js';
import type { ScoreBreakdown, EphemeralLiveTicket } from '../types/index.js';
import { distributedState, DistributedStateManager } from '../services/distributedState.js';

/**
 * Transient volatile in-memory cache with automated TTL purge.
 * Strictly volatile RAM—zero disk or database writes.
 */
export class TransientVolatileCache<T> {
  private map: Map<string, { value: T; expiresAt: number }> = new Map();

  public set(key: string, value: T, ttlMs: number): void {
    this.map.set(key, { value, expiresAt: Date.now() + ttlMs });
  }

  public get(key: string): T | null {
    const item = this.map.get(key);
    if (!item) return null;
    if (Date.now() > item.expiresAt) {
      this.map.delete(key);
      return null;
    }
    return item.value;
  }

  public delete(key: string): void {
    this.map.delete(key);
  }

  public purgeExpired(): number {
    const now = Date.now();
    let purged = 0;
    for (const [k, v] of this.map.entries()) {
      if (now > v.expiresAt) {
        this.map.delete(k);
        purged++;
      }
    }
    return purged;
  }
}

export interface EphemeralSession {
  sessionId: string;
  visitorId: string;
  ipAddress: string;
  isLoopback: boolean;
  ws: WebSocket | null;
  profession: string;
  strictProfessionFilter?: boolean;
  interests: string[];
  country?: string; // 'GLOBAL' or country code/name
  sessionMode?: string; // 'video' | 'audio' | 'text'
  currentMode: 'none' | 'matching' | '1on1' | 'community' | 'class';
  currentRoomId: string | null;
  pairedSessionId: string | null;
  blockedSessionIds: Set<string>;
  blockedVisitorIds: Set<string>;
  createdAt: number;
  lastActivity: number;
  violationCount: number;
  isMutedUntil: number; // Unix timestamp
  isRestrictedUntil: number; // Unix timestamp
  skipTimestamps: number[];
  messageTimestamps: number[];
  incomingFrameTimestamps?: number[];
  floodWarningCount?: number;
  isRecovering?: boolean;
  partnerProfession?: string;
  partnerCountry?: string;
  partnerVisitorId?: string;
  sharedInterests?: string[];
  sharedTopics?: string[];
  matchScore?: number;
  scoreBreakdown?: ScoreBreakdown;
  isInitiator?: boolean;
  endedWhileOfflineReason?: 'skipped' | 'ended' | 'disconnected' | 'reported' | 'blocked' | null;
  restorationPromise?: Promise<void> | null;
}

export interface CommunityRoom {
  id: string;
  name: string;
  topic: string;
  description: string;
  category: string;
  maxParticipants: number;
  memberSessionIds: Set<string>;
  isPreset: boolean;
  createdAt: number;
}

export interface ClassQuestion {
  id: string;
  askerSessionId: string;
  question: string;
  timestamp: number;
  answer?: string;
  answeredAt?: number;
}

export interface ClassRoom {
  id: string;
  name: string;
  topic: string;
  hostSessionId: string;
  participantSessionIds: Set<string>;
  questions: ClassQuestion[];
  createdAt: number;
}

// Client -> Server Action Messages
export type ClientMessage =
  | { type: 'REGISTER_SESSION'; visitorId?: string; blockedStrangers?: string[]; profession?: string; interests?: string[] }
  | { type: 'START_MATCHING'; visitorId?: string; blockedStrangers?: string[]; profession: string; strictProfessionFilter?: boolean; interests: string[]; country?: string; mode?: string; sessionMode?: string }
  | { type: 'CANCEL_MATCHING' }
  | { type: 'NEXT_PERSON' }
  | { type: 'END_CONVERSATION' }
  | { type: 'SEND_1ON1_MESSAGE'; text: string }
  | { type: 'JOIN_COMMUNITY'; communityId: string }
  | { type: 'CREATE_COMMUNITY'; name: string; topic: string; category: string }
  | { type: 'LEAVE_COMMUNITY' }
  | { type: 'SEND_COMMUNITY_MESSAGE'; text: string }
  | { type: 'JOIN_CLASS'; classId: string }
  | { type: 'CREATE_CLASS'; name: string; topic: string }
  | { type: 'LEAVE_CLASS' }
  | { type: 'SEND_CLASS_MESSAGE'; text: string }
  | { type: 'SUBMIT_CLASS_QUESTION'; question: string }
  | { type: 'ANSWER_CLASS_QUESTION'; questionId: string; answer: string }
  | { type: 'REPORT_USER'; category: string; details?: string }
  | { type: 'BLOCK_USER' }
  | { type: 'block_partner'; blockerId: string; blockedId: string; reason: string }
  | { type: 'WEBRTC_SIGNAL'; roomId?: string; signal: any }
  | { type: 'VOICE_TRANSCRIPT'; text: string; language: string; isFinal: boolean; id?: string; timestamp?: number }
  | { type: 'PAD_UPDATE'; content: string; language?: string }
  | { type: 'TYPING_INDICATOR'; isTyping: boolean }
  | { type: 'MESSAGE_READ'; messageId: string }
  | { type: 'GET_EXPLORE_DATA' }
  | { type: 'MOD_SUBSCRIBE' }
  | { type: 'MOD_ACTION'; ticketId: string; action: 'KICK' | 'MUTE' | 'DISMISS' }
  | { type: 'FLAG_LIVE_STREAM'; reason: string; confidence: number }
  | { type: 'PING'; timestamp?: number };

// Pre-serialized immutable static payloads for zero-allocation JSON broadcasts
export const PRE_SERIALIZED_MESSAGES = {
  MATCHING_STARTED: JSON.stringify({ type: 'MATCHING_STARTED' }),
  MATCHING_CANCELED: JSON.stringify({ type: 'MATCHING_CANCELED' }),
  ERROR_INVALID_PAYLOAD: JSON.stringify({ type: 'ERROR', message: 'Invalid message payload.' }),
  ERROR_MIN_INTERESTS: JSON.stringify({ type: 'ERROR', message: 'Please select at least 3 interests to match.' }),
  SERVER_SHUTDOWN: JSON.stringify({ type: 'SERVER_SHUTDOWN', reason: 'maintenance' }),
} as const;

export const PRE_SERIALIZED_PARTNER_LEFT: Record<string, string> = {
  skipped: JSON.stringify({ type: 'PARTNER_LEFT', reason: 'skipped' }),
  ended: JSON.stringify({ type: 'PARTNER_LEFT', reason: 'ended' }),
  disconnected: JSON.stringify({ type: 'PARTNER_LEFT', reason: 'disconnected' }),
  reported: JSON.stringify({ type: 'PARTNER_LEFT', reason: 'reported' }),
  blocked: JSON.stringify({ type: 'PARTNER_LEFT', reason: 'blocked' }),
};

// Server -> Client Event Messages
export type ServerMessage =
  | { type: 'SESSION_REGISTERED'; sessionId: string }
  | { type: 'MATCHING_STARTED' }
  | { type: 'MATCHING_CANCELED' }
  | {
      type: 'MATCH_FOUND';
      roomId: string;
      sharedInterests: string[];
      sharedTopics: string[];
      score: number;
      partnerProfession?: string;
      partnerCountry?: string;
      partnerVisitorId?: string;
      partnerSessionId?: string;
      scoreBreakdown?: ScoreBreakdown;
      isInitiator?: boolean;
    }
  | {
      type: 'SESSION_RESTORED';
      roomId: string;
      sharedInterests: string[];
      sharedTopics: string[];
      score: number;
      partnerProfession?: string;
      partnerCountry?: string;
      partnerVisitorId?: string;
      partnerSessionId?: string;
      scoreBreakdown?: ScoreBreakdown;
      isInitiator?: boolean;
    }
  | { type: 'PARTNER_RECONNECTING'; graceSeconds: number }
  | { type: 'PARTNER_RECONNECTED'; roomId: string }
  | { type: 'PARTNER_LEFT'; reason: 'skipped' | 'ended' | 'disconnected' | 'reported' | 'blocked' }
  | { type: 'PONG'; timestamp: number }
  | { type: 'WEBRTC_SIGNAL'; signal: any }
  | { type: 'VOICE_TRANSCRIPT'; id: string; text: string; language: string; isFinal: boolean; senderSessionId: string; timestamp: number }
  | { type: 'RECEIVE_1ON1_MESSAGE'; id: string; text: string; sender: 'self' | 'peer'; timestamp: number }
  | { type: 'PAD_UPDATE'; content: string; language: string; senderSessionId: string }
  | { type: 'TYPING_INDICATOR'; isTyping: boolean; senderSessionId: string }
  | { type: 'MESSAGE_READ'; messageId: string; readAt: number }
  | { type: 'MESSAGE_BLOCKED'; reason: string }
  | { type: 'SAFETY_WARNING'; message: string }
  | { type: 'RATE_LIMIT_COOLDOWN'; cooldownSeconds: number; reason: string }
  | { type: 'TEMPORARY_RESTRICTION'; restrictedUntil: number; reason: string }
  | { type: 'COMMUNITY_JOINED'; room: { id: string; name: string; topic: string; category: string; participantCount: number; maxParticipants: number } }
  | { type: 'COMMUNITY_UPDATE'; participantCount: number }
  | { type: 'RECEIVE_COMMUNITY_MESSAGE'; id: string; text: string; senderSessionId: string; isSelf: boolean; timestamp: number }
  | { type: 'CLASS_JOINED'; room: { id: string; name: string; topic: string; isHost: boolean; participantCount: number; questions: ClassQuestion[] } }
  | { type: 'CLASS_UPDATE'; participantCount: number; questions: ClassQuestion[] }
  | { type: 'RECEIVE_CLASS_MESSAGE'; id: string; text: string; senderSessionId: string; isHost: boolean; isSelf: boolean; timestamp: number }
  | { type: 'EXPLORE_DATA'; communities: Array<{ id: string; name: string; topic: string; category: string; count: number; max: number }>; classes: Array<{ id: string; name: string; topic: string; count: number }> }
  | { type: 'MOD_QUEUE_UPDATE'; tickets: EphemeralLiveTicket[] }
  | { type: 'STREAM_MUTED_BY_MODERATOR'; reason: string }
  | { type: 'ERROR'; message: string };

export class OlogleEngine {
  private sessions: Map<string, EphemeralSession> = new Map();
  // In-memory persistent stores for anonymous blocking & safety
  private blockedPairs: Set<string> = new Set(); // "blockerVisitorId:blockedVisitorId"
  private blockedIpPairs: Set<string> = new Set(); // "ipA:ipB"
  private matchQueue = new MatchmakerQueue(this.blockedPairs, this.blockedIpPairs);
  private communityRooms: Map<string, CommunityRoom> = new Map();
  private classRooms: Map<string, ClassRoom> = new Map();
  private cleanupTimer: NodeJS.Timeout | null = null;

  // Auto-moderation sliding window tracking (10 min rolling window, 3 reports threshold, 30 min temporary ban)
  private readonly REPORT_ROLLING_WINDOW_MS = 10 * 60 * 1000; // 10 minutes
  private readonly BAN_THRESHOLD = 3; // 3 or more reports
  private readonly BAN_DURATION_MS = 30 * 60 * 1000; // 30 minutes

  private reportsByVisitorId: Map<string, number[]> = new Map();
  private reportsByIp: Map<string, number[]> = new Map();
  private bannedVisitorIds: Map<string, number> = new Map();
  private bannedIps: Map<string, number> = new Map();

  // Transient in-memory volatile cache (TTL auto-purged from volatile RAM)
  private transientVolatileCache = new TransientVolatileCache<any>();
  // Rotating in-memory salt for session IP hashing (Raw IPs never retained; rotated every 24h)
  private rotatingIpSalt: string = crypto.randomBytes(16).toString('hex');
  private lastSaltRotation: number = Date.now();
  private readonly SALT_ROTATION_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24-hour cryptographic salt rotation
  // Live Ephemeral Moderation Queue (Volatile RAM only, 0 database storage)
  private ephemeralTickets: Map<string, EphemeralLiveTicket> = new Map();
  private moderatorSessionIds: Set<string> = new Set();

  // Active ping-pong watchdog timer & reconnection idempotency timers
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private distributedQueueSyncTimer: NodeJS.Timeout | null = null;
  private distributedMatchInFlight = new Set<string>();
  private disconnectTimers: Map<string, NodeJS.Timeout> = new Map();
  private reconnectGraceWindowMs: number = Number(process.env.SESSION_RECOVERY_WINDOW_MS) || 20000; // 20-second recovery window ONLY for unexpected connection loss
  private heartbeatIntervalMs: number = Number(process.env.HEARTBEAT_INTERVAL_MS) || 20000; // 20-second keep-alive ping
  private stateManager: DistributedStateManager;

  // Rate limit constants
  private readonly MAX_SKIPS_IN_WINDOW = 4;
  private readonly SKIP_WINDOW_MS = 25000;
  private readonly SKIP_COOLDOWN_SECONDS = 15;

  private readonly MAX_MESSAGES_IN_WINDOW = 8;
  private readonly MESSAGE_WINDOW_MS = 5000;

  private readonly SESSION_INACTIVITY_TTL_MS = 5 * 60 * 1000; // 5 minutes TTL (applies ONLY to orphaned sessions with no active socket)

  constructor(customState?: DistributedStateManager) {
    this.stateManager = customState || distributedState;
    this.initializePresetRooms();
    this.startInactivityGarbageCollector();
    this.startHeartbeatWatchdog();
  }

  public setRecoveryWindowMs(ms: number): void {
    this.reconnectGraceWindowMs = Math.max(50, ms);
  }

  public getRecoveryWindowMs(): number {
    return this.reconnectGraceWindowMs;
  }

  private initializePresetRooms(): void {
    const presets: Array<Omit<CommunityRoom, 'memberSessionIds' | 'createdAt'>> = [
      {
        id: 'comm-ai-ml',
        name: 'AI & Machine Learning Builders',
        topic: 'LLMs, Neural Networks, Agents & Ethics',
        description: 'Discuss latest breakthroughs, open models, and application architecture.',
        category: 'Technology',
        maxParticipants: 30,
        isPreset: true,
      },
      {
        id: 'comm-web-dev',
        name: 'Web Engineering & Architecture',
        topic: 'React, TypeScript, Cloud, Next-gen Web',
        description: 'Frontend performance, backend scalability, and developer tooling.',
        category: 'Technology',
        maxParticipants: 30,
        isPreset: true,
      },
      {
        id: 'comm-startups',
        name: 'Early-Stage Founders',
        topic: 'Bootstrapping, Ideation, Product Market Fit',
        description: 'No hype, no spam. Honest founder discussions and user feedback tactics.',
        category: 'Business',
        maxParticipants: 30,
        isPreset: true,
      },
      {
        id: 'comm-design',
        name: 'Product & Visual Design',
        topic: 'Design Systems, Typography, Interaction',
        description: 'Critiques, aesthetic debates, and human-computer interface design.',
        category: 'Creative',
        maxParticipants: 30,
        isPreset: true,
      },
      {
        id: 'comm-science-math',
        name: 'Mathematics & Science Exchange',
        topic: 'Pure Math, Quantum Physics, Deep Research',
        description: 'Deep discussions on proofs, cosmology, biology, and scientific inquiry.',
        category: 'STEM',
        maxParticipants: 30,
        isPreset: true,
      },
      {
        id: 'comm-books-writing',
        name: 'Writers & Avid Readers',
        topic: 'Philosophy, Fiction, Craft, Non-Fiction',
        description: 'Share book recommendations, writing habits, and analytical thoughts.',
        category: 'Media & Arts',
        maxParticipants: 30,
        isPreset: true,
      },
    ];

    for (const preset of presets) {
      this.communityRooms.set(preset.id, {
        ...preset,
        memberSessionIds: new Set(),
        createdAt: Date.now(),
      });
    }

    // Initialize preset classes
    const defaultClasses = [
      {
        id: 'class-system-design',
        name: 'System Design: Scaling to 10M Users',
        topic: 'Distributed caching, sharding, and fault tolerance patterns',
      },
      {
        id: 'class-math-proofs',
        name: 'Introduction to Discrete Mathematics',
        topic: 'Combinatorics, Graph Theory, and Modular Arithmetic',
      },
    ];

    for (const c of defaultClasses) {
      this.classRooms.set(c.id, {
        id: c.id,
        name: c.name,
        topic: c.topic,
        hostSessionId: 'system-mentor',
        participantSessionIds: new Set(),
        questions: [
          {
            id: 'q-intro',
            askerSessionId: 'anonymous',
            question: 'What is the most common pitfall when designing distributed systems?',
            timestamp: Date.now() - 360000,
            answer: 'Premature optimization and assuming the network is reliable.',
            answeredAt: Date.now() - 300000,
          },
        ],
        createdAt: Date.now(),
      });
    }
  }

  private startInactivityGarbageCollector(): void {
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    this.cleanupTimer = setInterval(() => {
      const now = Date.now();
      // Purge ONLY disconnected orphaned sessions (never touches active sockets or sessions inside recovery window)
      for (const [sessionId, session] of this.sessions.entries()) {
        if (
          now - session.lastActivity > this.SESSION_INACTIVITY_TTL_MS &&
          !session.ws &&
          !this.disconnectTimers.has(sessionId)
        ) {
          this.destroySession(sessionId);
        }
      }
      // Purge expired live tickets from volatile RAM
      for (const [ticketId, ticket] of this.ephemeralTickets.entries()) {
        if (now > ticket.expiresAt) {
          this.ephemeralTickets.delete(ticketId);
        }
      }
      // Purge transient memory cache
      this.transientVolatileCache.purgeExpired();

      // Cryptographic IP salt rotation (24-hour cycle to prevent long-term correlation)
      if (now - this.lastSaltRotation > this.SALT_ROTATION_INTERVAL_MS) {
        this.rotatingIpSalt = crypto.randomBytes(16).toString('hex');
        this.lastSaltRotation = now;
      }
    }, 30000);
    if (this.cleanupTimer && typeof this.cleanupTimer === 'object' && 'unref' in this.cleanupTimer) {
      this.cleanupTimer.unref();
    }
  }

  private startHeartbeatWatchdog(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const [sessionId, session] of this.sessions.entries()) {
        if (!session.ws) continue;
        const sock = session.ws as any;
        if (sock.isAlive === false) {
          // Dead socket detected (failed to respond to ping): terminate socket and enter recovery window
          try {
            sock.terminate();
          } catch {}
          this.handleDisconnect(sessionId);
          continue;
        }
        sock.isAlive = false;
        session.lastActivity = now;
        // Keep active session state fresh in Redis so long-lived chats (hours+) never expire
        if (this.stateManager.isDistributed() && session.currentMode === '1on1') {
          this.syncSessionToRedis(session, 120);
        }
        try {
          sock.ping();
        } catch {
          this.handleDisconnect(sessionId);
        }
      }
    }, this.heartbeatIntervalMs);
    if (this.heartbeatTimer && typeof this.heartbeatTimer === 'object' && 'unref' in this.heartbeatTimer) {
      this.heartbeatTimer.unref();
    }
    this.startDistributedQueueSync();
  }

  private getQueueMode(mode?: string): string {
    return (mode || 'video').trim().toLowerCase();
  }

  private startDistributedQueueSync(): void {
    if (this.distributedQueueSyncTimer) clearInterval(this.distributedQueueSyncTimer);
    this.distributedQueueSyncTimer = setInterval(() => {
      if (this.matchQueue.size() === 0) return;

      // 1. First, evaluate local queue pairing for waiting sessions (supports progressive relaxation)
      for (const session of this.sessions.values()) {
        if (session.currentMode !== 'matching' || !session.ws || session.ws.readyState !== WebSocket.OPEN) {
          continue;
        }
        const queueMode = this.getQueueMode(session.sessionMode);
        const queuedSession: QueuedSession = {
          sessionId: session.sessionId,
          visitorId: session.visitorId,
          ipAddress: session.ipAddress,
          isLoopback: session.isLoopback,
          profession: session.profession,
          strictProfessionFilter: session.strictProfessionFilter,
          interests: session.interests,
          country: session.country || 'GLOBAL',
          mode: queueMode,
          queuedAt: session.lastActivity || Date.now(),
          blockedSessionIds: session.blockedSessionIds,
          blockedVisitorIds: session.blockedVisitorIds,
          matchedSessionId: null,
        };

        const localMatch = this.matchQueue.findBestMatch(queuedSession);
        if (localMatch) {
          this.pairSessions(session, localMatch.matchedSession, localMatch.scoreResult);
          continue;
        }

        // 2. Refresh registration in shared Redis queue and attempt cross-instance match
        if (this.stateManager.isConfigured()) {
          void this.stateManager.registerQueueTicket(
            {
              sessionId: session.sessionId,
              visitorId: session.visitorId,
              ipAddress: session.ipAddress,
              profession: session.profession,
              strictProfessionFilter: session.strictProfessionFilter,
              interests: session.interests,
              country: session.country || 'GLOBAL',
              sessionMode: queueMode,
              timestamp: Date.now(),
              blockedSessionIds: Array.from(session.blockedSessionIds),
              blockedVisitorIds: Array.from(session.blockedVisitorIds),
            },
            60
          );
          void this.tryMatchFromSharedQueue(session, queuedSession);
        }
      }
    }, 2000);
    if (
      this.distributedQueueSyncTimer &&
      typeof this.distributedQueueSyncTimer === 'object' &&
      'unref' in this.distributedQueueSyncTimer
    ) {
      this.distributedQueueSyncTimer.unref();
    }
  }

  public hashSessionIp(ip: string): string {
    return crypto
      .createHash('sha256')
      .update((ip || '127.0.0.1') + this.rotatingIpSalt)
      .digest('hex')
      .substring(0, 16);
  }

  private isLoopbackAddress(ip: string): boolean {
    const address = ip.trim().toLowerCase().replace(/^::ffff:/, '');
    return address === '::1' || address === 'localhost' || address.startsWith('127.');
  }

  public createEphemeralTicket(
    flaggedSession: EphemeralSession,
    category: string,
    reason: string,
    confidence = 0.9
  ): EphemeralLiveTicket {
    const ticketId = `mod_live_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`;
    const ticket: EphemeralLiveTicket = {
      ticketId,
      roomId: flaggedSession.currentRoomId || `room_${flaggedSession.sessionId.substring(0, 6)}`,
      sessionHash: flaggedSession.ipAddress || this.hashSessionIp(flaggedSession.sessionId),
      reason,
      category,
      confidence,
      flaggedAt: Date.now(),
      expiresAt: Date.now() + 5 * 60 * 1000, // 5 min TTL in volatile RAM
      status: 'PENDING_REVIEW',
    };
    this.ephemeralTickets.set(ticketId, ticket);
    this.broadcastModerationQueue();
    return ticket;
  }

  public getActiveSessionsCount(): number {
    return this.sessions.size;
  }

  public getQueuedUsersCount(): number {
    return this.matchQueue.size();
  }

  public getLiveTickets(): EphemeralLiveTicket[] {
    const now = Date.now();
    return Array.from(this.ephemeralTickets.values()).filter((t) => t.expiresAt > now);
  }

  public broadcastModerationQueue(): void {
    const tickets = this.getLiveTickets();
    const payload: ServerMessage = {
      type: 'MOD_QUEUE_UPDATE',
      tickets,
    };
    for (const modSessionId of this.moderatorSessionIds) {
      const modSession = this.sessions.get(modSessionId);
      if (modSession && modSession.ws) {
        this.send(modSession.ws, payload);
      } else {
        this.moderatorSessionIds.delete(modSessionId);
      }
    }
  }

  private syncSessionToRedis(session: EphemeralSession, ttlSeconds = 120): void {
    if (!this.stateManager.isDistributed()) return;
    this.stateManager.setSessionState(
      session.sessionId,
      {
        sessionId: session.sessionId,
        visitorId: session.visitorId,
        ipAddress: session.ipAddress,
        profession: session.profession,
        interests: session.interests,
        country: session.country,
        sessionMode: session.sessionMode,
        currentMode: session.currentMode,
        pairedSessionId: session.pairedSessionId,
        currentRoomId: session.currentRoomId,
        lastActivity: session.lastActivity,
        isRecovering: Boolean(session.isRecovering),
        partnerProfession: session.partnerProfession,
        partnerCountry: session.partnerCountry,
        partnerVisitorId: session.partnerVisitorId,
        sharedInterests: session.sharedInterests,
        sharedTopics: session.sharedTopics,
        matchScore: session.matchScore,
        scoreBreakdown: session.scoreBreakdown,
        isInitiator: session.isInitiator,
        endedWhileOfflineReason: session.endedWhileOfflineReason || null,
        blockedSessionIds: Array.from(session.blockedSessionIds),
        blockedVisitorIds: Array.from(session.blockedVisitorIds),
      },
      ttlSeconds
    );
  }

  private generateSecureSessionId(): string {
    return `hl_${crypto.randomBytes(16).toString('hex')}`;
  }

  public registerSession(ws: WebSocket, existingId?: string, ipAddress = '127.0.0.1', visitorId?: string): EphemeralSession {
    const now = Date.now();
    const hashedIp = this.hashSessionIp(ipAddress);

    // Validate existingId format and guard against unauthorized session takeover or arbitrary ID injection
    let candidateId =
      existingId && /^hl_[a-zA-Z0-9_-]{8,64}$/.test(existingId) ? existingId : undefined;

    if (candidateId) {
      const existingLocal = this.sessions.get(candidateId);
      if (existingLocal) {
        const isLiveActiveSocket =
          existingLocal.ws &&
          existingLocal.ws.readyState === WebSocket.OPEN &&
          !existingLocal.isRecovering;
        const isIpMismatchOnActive = isLiveActiveSocket && existingLocal.ipAddress !== hashedIp;
        const isVisitorMismatch =
          Boolean(visitorId && existingLocal.visitorId && visitorId !== existingLocal.visitorId);
        if (isIpMismatchOnActive || isVisitorMismatch) {
          // Block unauthorized session takeover attempt: allocate a fresh sessionId
          candidateId = undefined;
        }
      } else if (!this.stateManager.isDistributed()) {
        // In single-instance mode, any unknown existingId is expired or forged: allocate a fresh sessionId
        candidateId = undefined;
      }
    }

    const sessionId = candidateId || this.generateSecureSessionId();

    // Cancel pending disconnect disposal timer if reconnecting within the recovery grace window
    const pendingTimer = this.disconnectTimers.get(sessionId);
    if (pendingTimer) {
      clearTimeout(pendingTimer);
      this.disconnectTimers.delete(sessionId);
    }

    let session = this.sessions.get(sessionId);
    let wasRecoveringLocal1on1 = false;

    if (!session) {
      session = {
        sessionId,
        visitorId: visitorId || `vst_${crypto.randomBytes(6).toString('hex')}`,
        ipAddress: hashedIp,
        isLoopback: this.isLoopbackAddress(ipAddress),
        ws,
        profession: '',
        interests: [],
        currentMode: 'none',
        currentRoomId: null,
        pairedSessionId: null,
        blockedSessionIds: new Set(),
        blockedVisitorIds: new Set(),
        createdAt: now,
        lastActivity: now,
        violationCount: 0,
        isMutedUntil: 0,
        isRestrictedUntil: 0,
        skipTimestamps: [],
        messageTimestamps: [],
        isRecovering: false,
      };
      this.sessions.set(sessionId, session);

      // Recovery from distributed Redis state if reconnecting to a different backend instance
      if (candidateId && this.stateManager.isDistributed()) {
        const targetSession = session;
        targetSession.restorationPromise = this.stateManager
          .getSessionState(sessionId)
          .then((cached) => {
            if (cached && this.sessions.get(sessionId) === targetSession) {
              // Guard against cross-instance takeover of an active non-recovering session from a different IP/visitor
              const isIpMismatchOnActive =
                cached.isRecovering === false &&
                Boolean(cached.ipAddress && cached.ipAddress !== hashedIp);
              const isVisitorMismatch =
                Boolean(visitorId && cached.visitorId && visitorId !== cached.visitorId);
              if (isIpMismatchOnActive || isVisitorMismatch) {
                return;
              }

              targetSession.visitorId = cached.visitorId || targetSession.visitorId;
              targetSession.profession = cached.profession || targetSession.profession;
              targetSession.interests = cached.interests || targetSession.interests;
              targetSession.country = cached.country || targetSession.country;
              targetSession.sessionMode = cached.sessionMode || targetSession.sessionMode;
              targetSession.currentMode = (cached.currentMode as any) || targetSession.currentMode;
              targetSession.pairedSessionId = cached.pairedSessionId;
              targetSession.currentRoomId = cached.currentRoomId;
              targetSession.partnerProfession = cached.partnerProfession;
              targetSession.partnerCountry = cached.partnerCountry;
              targetSession.partnerVisitorId = cached.partnerVisitorId;
              targetSession.sharedInterests = cached.sharedInterests;
              targetSession.sharedTopics = cached.sharedTopics;
              targetSession.matchScore = cached.matchScore;
              targetSession.scoreBreakdown = cached.scoreBreakdown;
              targetSession.isInitiator = cached.isInitiator;
              targetSession.endedWhileOfflineReason = cached.endedWhileOfflineReason || null;
              if (Array.isArray(cached.blockedSessionIds)) {
                for (const bid of cached.blockedSessionIds) targetSession.blockedSessionIds.add(bid);
              }
              if (Array.isArray(cached.blockedVisitorIds)) {
                for (const bvid of cached.blockedVisitorIds) targetSession.blockedVisitorIds.add(bvid);
              }
              targetSession.isRecovering = false;

              // Notify any previous instance holding a disconnect timer that this session resumed here
              this.stateManager.publishToSession(
                sessionId,
                JSON.stringify({ type: '__INTERNAL_SESSION_RESUMED__', sessionId })
              );
              this.syncSessionToRedis(targetSession, 120);

              if (
                targetSession.currentMode === '1on1' &&
                targetSession.pairedSessionId &&
                targetSession.currentRoomId &&
                targetSession.ws
              ) {
                this.send(targetSession.ws, {
                  type: 'SESSION_RESTORED',
                  roomId: targetSession.currentRoomId,
                  sharedInterests: targetSession.sharedInterests || [],
                  sharedTopics: targetSession.sharedTopics || [],
                  score: targetSession.matchScore ?? 100,
                  partnerProfession: targetSession.partnerProfession,
                  partnerCountry: targetSession.partnerCountry || 'GLOBAL',
                  partnerVisitorId: targetSession.partnerVisitorId,
                  scoreBreakdown: targetSession.scoreBreakdown,
                  isInitiator: targetSession.isInitiator,
                });
                this.sendToPartner(targetSession.pairedSessionId, {
                  type: 'PARTNER_RECONNECTED',
                  roomId: targetSession.currentRoomId,
                });
              } else if (targetSession.endedWhileOfflineReason && targetSession.ws) {
                this.send(targetSession.ws, {
                  type: 'PARTNER_LEFT',
                  reason: targetSession.endedWhileOfflineReason,
                });
                targetSession.endedWhileOfflineReason = null;
                this.syncSessionToRedis(targetSession, 120);
              }
            }
          })
          .catch(() => {})
          .finally(() => {
            targetSession.restorationPromise = null;
          });
      }
    } else {
      // Quietly replace any stale socket handle without triggering disconnect teardown
      if (session.ws && session.ws !== ws) {
        try {
          (session.ws as any).removeAllListeners?.();
          session.ws.close();
        } catch {}
      }
      wasRecoveringLocal1on1 = Boolean(
        session.isRecovering &&
          session.currentMode === '1on1' &&
          session.pairedSessionId &&
          session.currentRoomId
      );
      session.ws = ws;
      session.lastActivity = now;
      session.ipAddress = hashedIp;
      session.isLoopback = this.isLoopbackAddress(ipAddress);
      session.isRecovering = false;
      if (visitorId) session.visitorId = visitorId;
      this.syncSessionToRedis(session, 120);
    }

    (ws as any).isAlive = true;
    this.send(ws, { type: 'SESSION_REGISTERED', sessionId });

    if (wasRecoveringLocal1on1 && session.currentRoomId && session.pairedSessionId) {
      this.send(ws, {
        type: 'SESSION_RESTORED',
        roomId: session.currentRoomId,
        sharedInterests: session.sharedInterests || [],
        sharedTopics: session.sharedTopics || [],
        score: session.matchScore ?? 100,
        partnerProfession: session.partnerProfession,
        partnerCountry: session.partnerCountry || 'GLOBAL',
        partnerVisitorId: session.partnerVisitorId,
        scoreBreakdown: session.scoreBreakdown,
        isInitiator: session.isInitiator,
      });
      this.sendToPartner(session.pairedSessionId, {
        type: 'PARTNER_RECONNECTED',
        roomId: session.currentRoomId,
      });
    } else if (session.endedWhileOfflineReason) {
      this.send(ws, {
        type: 'PARTNER_LEFT',
        reason: session.endedWhileOfflineReason,
      });
      session.endedWhileOfflineReason = null;
      this.syncSessionToRedis(session, 120);
    }

    const activeSession = session;
    this.stateManager.subscribeToSession(sessionId, (rawMsg) => {
      let outboundMsg = rawMsg;
      try {
        const parsed = JSON.parse(rawMsg);
        if (parsed.type === '__INTERNAL_SESSION_RESUMED__') {
          // If this instance only holds a disconnected recovering copy, cancel timer and drop stale local copy
          if (!activeSession.ws) {
            const timer = this.disconnectTimers.get(sessionId);
            if (timer) {
              clearTimeout(timer);
              this.disconnectTimers.delete(sessionId);
            }
            this.stateManager.unsubscribeFromSession(sessionId);
            this.sessions.delete(sessionId);
          }
          return;
        }
        if (parsed.type === 'MATCH_FOUND') {
          activeSession.currentMode = '1on1';
          activeSession.pairedSessionId = parsed.partnerSessionId || null;
          activeSession.currentRoomId = parsed.roomId || null;
          activeSession.partnerProfession = parsed.partnerProfession;
          activeSession.partnerCountry = parsed.partnerCountry;
          activeSession.partnerVisitorId = parsed.partnerVisitorId;
          activeSession.sharedInterests = parsed.sharedInterests;
          activeSession.sharedTopics = parsed.sharedTopics;
          activeSession.matchScore = parsed.score;
          activeSession.scoreBreakdown = parsed.scoreBreakdown;
          activeSession.isInitiator = parsed.isInitiator;
          activeSession.isRecovering = false;
          this.matchQueue.remove(activeSession.sessionId);
          this.syncSessionToRedis(activeSession, 120);
          // Strip internal partnerSessionId before forwarding to client WebSocket
          delete parsed.partnerSessionId;
          outboundMsg = JSON.stringify(parsed);
        } else if (parsed.type === 'PARTNER_LEFT') {
          if (activeSession.currentMode === '1on1') {
            if (
              activeSession.pairedSessionId &&
              (parsed.reason === 'skipped' || parsed.reason === 'reported' || parsed.reason === 'blocked')
            ) {
              activeSession.blockedSessionIds.add(activeSession.pairedSessionId);
            }
            activeSession.currentMode = 'none';
            activeSession.pairedSessionId = null;
            activeSession.currentRoomId = null;
            if (!activeSession.ws) {
              activeSession.endedWhileOfflineReason = parsed.reason || 'ended';
            }
            this.syncSessionToRedis(activeSession, 60);
          }
        }
      } catch {}

      if (activeSession.ws && activeSession.ws.readyState === WebSocket.OPEN) {
        activeSession.ws.send(outboundMsg);
      }
    });
    return session;
  }

  public handleDisconnect(sessionId: string): void {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    session.ws = null;
    session.lastActivity = Date.now();
    const graceSeconds = Math.max(1, Math.ceil(this.reconnectGraceWindowMs / 1000));

    // Handle unexpected socket drop based on active mode
    if (session.currentMode === 'matching') {
      this.matchQueue.remove(sessionId);
      void this.stateManager.removeQueueTicket(sessionId, this.getQueueMode(session.sessionMode));
      session.currentMode = 'none';
      console.log(`[OLOGLE WS] Disconnected while matching; removed queue ticket session=${sessionId.slice(0, 10)}`);
    } else if (session.currentMode === '1on1' && session.pairedSessionId) {
      // Preserve active 1-on-1 room during the recovery grace window!
      // Do NOT send PARTNER_LEFT immediately on transient network loss.
      session.isRecovering = true;
      this.sendToPartner(session.pairedSessionId, {
        type: 'PARTNER_RECONNECTING',
        graceSeconds,
      });
    } else if (session.currentMode === 'community' && session.currentRoomId) {
      this.leaveCommunity(session);
      session.currentMode = 'none';
      session.currentRoomId = null;
    } else if (session.currentMode === 'class' && session.currentRoomId) {
      this.leaveClass(session);
      session.currentMode = 'none';
      session.currentRoomId = null;
    }

    // Persist session state (including active 1on1 room pointers) to Redis for cross-instance recovery
    this.syncSessionToRedis(session, graceSeconds + 30);

    // Recovery grace window timer: only terminates chat if user fails to reconnect before expiry
    const pendingTimer = this.disconnectTimers.get(sessionId);
    if (pendingTimer) clearTimeout(pendingTimer);

    const timer = setTimeout(async () => {
      this.disconnectTimers.delete(sessionId);
      const current = this.sessions.get(sessionId);
      if (!current) return;
      // If user already reconnected on this instance, abort expiry
      if (current.ws !== null) return;

      // Check if user reconnected on a different instance in the cluster
      if (this.stateManager.isDistributed()) {
        try {
          const remoteState = await this.stateManager.getSessionState(sessionId);
          if (remoteState && remoteState.isRecovering === false) {
            this.stateManager.unsubscribeFromSession(sessionId);
            this.sessions.delete(sessionId);
            return;
          }
        } catch {}
      }

      // User failed to reconnect within the recovery window: now notify partner and clean up
      if (current.currentMode === '1on1' && current.pairedSessionId) {
        const partnerId = current.pairedSessionId;
        this.notifyPartnerLeft(partnerId, 'disconnected');
        const partner = this.sessions.get(partnerId);
        if (partner && partner.pairedSessionId === sessionId) {
          partner.currentMode = 'none';
          partner.pairedSessionId = null;
          partner.currentRoomId = null;
          if (!partner.ws) {
            partner.endedWhileOfflineReason = 'disconnected';
          }
          this.syncSessionToRedis(partner, 60);
        }
      }

      this.stateManager.unsubscribeFromSession(sessionId);
      this.stateManager.removeSessionState(sessionId);
      this.destroySession(sessionId);
    }, this.reconnectGraceWindowMs);

    if (typeof timer === 'object' && 'unref' in timer) {
      timer.unref();
    }
    this.disconnectTimers.set(sessionId, timer);
  }

  public destroySession(sessionId: string): void {
    const pendingTimer = this.disconnectTimers.get(sessionId);
    if (pendingTimer) {
      clearTimeout(pendingTimer);
      this.disconnectTimers.delete(sessionId);
    }

    const session = this.sessions.get(sessionId);
    if (!session) return;
    this.stateManager.unsubscribeFromSession(sessionId);
    this.matchQueue.remove(sessionId);
    // Volatile RAM scrubbing: Clear all references and sets for immediate V8 reclamation
    session.blockedSessionIds?.clear();
    session.blockedVisitorIds?.clear();
    session.ws = null;
    session.pairedSessionId = null;
    session.currentRoomId = null;
    this.sessions.delete(sessionId);
  }

  public processMessage(sessionId: string, rawData: string): void {
    const session = this.sessions.get(sessionId);
    if (!session || !session.ws) return;

    // If cross-instance Redis restoration is in flight, defer frame execution until restoration completes
    if (session.restorationPromise) {
      session.restorationPromise.then(() => {
        this.processMessage(sessionId, rawData);
      });
      return;
    }

    const now = Date.now();
    session.lastActivity = now;
    (session.ws as any).isAlive = true;

    // Fast-fail payload length guard to prevent event loop blocking on huge malicious frames
    if (!rawData || typeof rawData !== 'string' || rawData.length > 65536) {
      this.send(session.ws, PRE_SERIALIZED_MESSAGES.ERROR_INVALID_PAYLOAD);
      return;
    }

    let msg: ClientMessage;
    try {
      msg = JSON.parse(rawData);
    } catch {
      this.send(session.ws, PRE_SERIALIZED_MESSAGES.ERROR_INVALID_PAYLOAD);
      return;
    }

    // Application-level heartbeat keep-alive (exempt from flood rate limiting)
    if (msg.type === 'PING') {
      if (this.stateManager.isDistributed() && session.currentMode === '1on1') {
        this.syncSessionToRedis(session, 120);
      }
      this.send(session.ws, { type: 'PONG', timestamp: now });
      return;
    }

    // Per-socket sliding-window rate limit: max 25 frames per 5 seconds
    const FRAME_RATE_WINDOW_MS = 5000;
    const MAX_FRAMES_IN_WINDOW = 25;
    session.incomingFrameTimestamps = (session.incomingFrameTimestamps || []).filter(
      (t) => now - t < FRAME_RATE_WINDOW_MS
    );
    session.incomingFrameTimestamps.push(now);

    if (session.incomingFrameTimestamps.length > MAX_FRAMES_IN_WINDOW) {
      session.floodWarningCount = (session.floodWarningCount || 0) + 1;
      if (session.floodWarningCount >= 3) {
        // Flood threshold exceeded: close socket immediately with 1008 Policy Violation
        session.ws.close(1008, 'Policy Violation');
        return;
      }
      this.send(session.ws, {
        type: 'WARNING',
        message: 'Slow down. Message rate too high.',
      } as any);
      return;
    }

    // Check active restrictions
    if (session.isRestrictedUntil > now) {
      this.send(session.ws, {
        type: 'TEMPORARY_RESTRICTION',
        restrictedUntil: session.isRestrictedUntil,
        reason: 'Temporary session restriction due to repeated policy violations.',
      });
      return;
    }

    switch (msg.type) {
      case 'REGISTER_SESSION':
        if (msg.visitorId) session.visitorId = msg.visitorId;
        if (msg.blockedStrangers && Array.isArray(msg.blockedStrangers)) {
          for (const bid of msg.blockedStrangers) {
            if (bid && typeof bid === 'string') session.blockedVisitorIds.add(bid);
          }
        }
        if (msg.profession) {
          const val = validateProfession(msg.profession);
          if (val.isValid) {
            session.profession = msg.profession.trim();
          }
        }
        if (msg.interests && Array.isArray(msg.interests)) session.interests = msg.interests;
        this.syncSessionToRedis(session, 120);
        break;

      case 'START_MATCHING':
        if (msg.visitorId) session.visitorId = msg.visitorId;
        if (msg.blockedStrangers && Array.isArray(msg.blockedStrangers)) {
          for (const bid of msg.blockedStrangers) {
            if (bid && typeof bid === 'string') session.blockedVisitorIds.add(bid);
          }
        }
        this.startMatching(
          session,
          msg.profession,
          msg.interests,
          msg.country,
          msg.sessionMode || msg.mode,
          msg.strictProfessionFilter
        );
        break;

      case 'CANCEL_MATCHING':
        this.cancelMatching(session);
        break;

      case 'NEXT_PERSON':
        this.handleNextPerson(session);
        break;

      case 'END_CONVERSATION':
        this.handleEndConversation(session);
        break;

      case 'SEND_1ON1_MESSAGE':
        this.handle1on1Message(session, msg.text);
        break;

      case 'MESSAGE_READ':
        this.handleMessageRead(session, msg.messageId);
        break;

      case 'JOIN_COMMUNITY':
        this.joinCommunity(session, msg.communityId);
        break;

      case 'CREATE_COMMUNITY':
        this.createCommunity(session, msg.name, msg.topic, msg.category);
        break;

      case 'LEAVE_COMMUNITY':
        this.leaveCommunity(session);
        break;

      case 'SEND_COMMUNITY_MESSAGE':
        this.handleCommunityMessage(session, msg.text);
        break;

      case 'JOIN_CLASS':
        this.joinClass(session, msg.classId);
        break;

      case 'CREATE_CLASS':
        this.createClass(session, msg.name, msg.topic);
        break;

      case 'LEAVE_CLASS':
        this.leaveClass(session);
        break;

      case 'SEND_CLASS_MESSAGE':
        this.handleClassMessage(session, msg.text);
        break;

      case 'SUBMIT_CLASS_QUESTION':
        this.handleSubmitClassQuestion(session, msg.question);
        break;

      case 'ANSWER_CLASS_QUESTION':
        this.handleAnswerClassQuestion(session, msg.questionId, msg.answer);
        break;

      case 'REPORT_USER':
        this.handleReportUser(session, msg.category, msg.details);
        break;

      case 'BLOCK_USER':
        this.handleBlockUser(session);
        break;

      case 'block_partner':
        this.handleBlockPartner(session, msg.blockerId, msg.blockedId, msg.reason);
        break;

      case 'WEBRTC_SIGNAL':
        this.handleWebRTCSignal(session, msg.roomId, msg.signal);
        break;

      case 'VOICE_TRANSCRIPT':
        this.handleVoiceTranscript(session, msg.text, msg.language, msg.isFinal, msg.id, msg.timestamp);
        break;

      case 'GET_EXPLORE_DATA':
        this.sendExploreData(session);
        break;

      case 'PAD_UPDATE':
        this.handlePadUpdate(session, msg.content, msg.language);
        break;

      case 'TYPING_INDICATOR':
        this.handleTypingIndicator(session, msg.isTyping);
        break;

      case 'MOD_SUBSCRIBE':
        this.moderatorSessionIds.add(session.sessionId);
        if (session.ws) {
          this.send(session.ws, {
            type: 'MOD_QUEUE_UPDATE',
            tickets: this.getLiveTickets(),
          });
        }
        break;

      case 'MOD_ACTION':
        this.handleModAction(session, msg.ticketId, msg.action);
        break;

      case 'FLAG_LIVE_STREAM':
        this.handleFlagLiveStream(session, msg.reason, msg.confidence);
        break;

      default:
        break;
    }
  }

  private isBanned(visitorId: string, ip: string): { banned: boolean; restrictedUntil: number; reason: string } {
    const now = Date.now();
    const visitorBan = this.bannedVisitorIds.get(visitorId);
    if (visitorBan && visitorBan > now) {
      return {
        banned: true,
        restrictedUntil: visitorBan,
        reason: 'You are temporarily banned for 30 minutes following multiple community reports.',
      };
    }
    const ipBan = this.bannedIps.get(ip);
    if (ipBan && ipBan > now) {
      return {
        banned: true,
        restrictedUntil: ipBan,
        reason: 'This network is temporarily banned for 30 minutes following multiple community reports.',
      };
    }
    return { banned: false, restrictedUntil: 0, reason: '' };
  }

  // --- Matchmaking Implementation ---

  private startMatching(
    session: EphemeralSession,
    profession: string,
    interests: string[],
    country?: string,
    mode?: string,
    strictProfessionFilter?: boolean
  ): void {
    const banCheck = this.isBanned(session.visitorId, session.ipAddress);
    if (banCheck.banned) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'TEMPORARY_RESTRICTION',
          restrictedUntil: banCheck.restrictedUntil,
          reason: banCheck.reason,
        });
      }
      return;
    }

    const cleanProf = (profession || session.profession || '').trim();
    if (country) {
      session.country = country;
    }
    if (mode) {
      session.sessionMode = mode;
    }
    session.strictProfessionFilter = Boolean(strictProfessionFilter);

    const profValidation = validateProfession(cleanProf);
    if (!profValidation.isValid) {
      if (session.ws) {
        this.send(session.ws, { type: 'ERROR', message: profValidation.error || 'Please enter a valid profession.' });
      }
      return;
    }

    const interestValidation = validateInterests(interests && interests.length > 0 ? interests : session.interests);
    if (!interestValidation.isValid) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'ERROR',
          message: interestValidation.error || 'Please select at least 3 valid interests to match.',
        });
      }
      return;
    }

    const activeInterests = interestValidation.sanitized;
    const previousQueueMode = this.getQueueMode(session.sessionMode);
    const queueMode = this.getQueueMode(mode || session.sessionMode);
    session.sessionMode = queueMode;

    // Clear existing paired state and remove any prior local queue entry to prevent duplicate matchmaking
    this.cleanCurrentSessionState(session);
    this.matchQueue.remove(session.sessionId);

    session.profession = cleanProf;
    session.interests = activeInterests;
    session.currentMode = 'matching';
    session.isRecovering = false;
    session.endedWhileOfflineReason = null;

    const queuedSession: QueuedSession = {
      sessionId: session.sessionId,
      visitorId: session.visitorId,
      ipAddress: session.ipAddress,
      isLoopback: session.isLoopback,
      profession: session.profession,
      strictProfessionFilter: session.strictProfessionFilter,
      interests: session.interests,
      country: session.country || 'GLOBAL',
      mode: queueMode,
      queuedAt: Date.now(),
      blockedSessionIds: session.blockedSessionIds,
      blockedVisitorIds: session.blockedVisitorIds,
      matchedSessionId: null,
    };

    console.log(
      `[OLOGLE Matchmaker] START_MATCHING session=${session.sessionId.slice(0, 10)} mode=${queueMode} country=${queuedSession.country} interests=${activeInterests.length}`
    );

    if (session.ws) {
      this.send(session.ws, PRE_SERIALIZED_MESSAGES.MATCHING_STARTED);
    }

    // Try finding an immediate compatible match in local queue
    const match = this.matchQueue.findBestMatch(queuedSession);

    if (match) {
      this.pairSessions(session, match.matchedSession, match.scoreResult);
    } else {
      // Put in local queue
      this.matchQueue.enqueue(queuedSession);

      // Perform ordered distributed Redis queue registration + cross-instance matching
      if (this.stateManager.isConfigured()) {
        void this.enqueueAndMatchDistributed(session, queuedSession, previousQueueMode, queueMode);
      }
    }
  }

  private async enqueueAndMatchDistributed(
    session: EphemeralSession,
    queuedSession: QueuedSession,
    previousQueueMode: string,
    queueMode: string
  ): Promise<void> {
    try {
      const connected = await this.stateManager.ensureConnected(2500);
      if (!connected) {
        console.warn(
          `[OLOGLE Matchmaker] Redis unavailable during START_MATCHING for session=${session.sessionId.slice(0, 10)}; staying in local queue.`
        );
        return;
      }

      if (session.currentMode !== 'matching' || !session.ws || session.ws.readyState !== WebSocket.OPEN) {
        return;
      }

      // Ensure subscriber channel is active BEFORE advertising ticket in Redis
      await this.stateManager.ensureSessionSubscribed(session.sessionId);

      // Clean up any stale ticket under a previous mode before registering the current one
      if (previousQueueMode !== queueMode) {
        await this.stateManager.removeQueueTicket(session.sessionId, previousQueueMode);
      }

      // First try matching against an already-waiting remote user before advertising ourselves
      const matchedImmediately = await this.tryMatchFromSharedQueue(session, queuedSession);
      if (matchedImmediately || session.currentMode !== 'matching') {
        return;
      }

      // Register in shared Redis queue so other instances can find and claim this session
      await this.stateManager.registerQueueTicket(
        {
          sessionId: session.sessionId,
          visitorId: session.visitorId,
          ipAddress: session.ipAddress,
          profession: session.profession,
          strictProfessionFilter: session.strictProfessionFilter,
          interests: session.interests,
          country: session.country || 'GLOBAL',
          sessionMode: queueMode,
          timestamp: Date.now(),
          blockedSessionIds: Array.from(session.blockedSessionIds),
          blockedVisitorIds: Array.from(session.blockedVisitorIds),
        },
        60
      );
      this.syncSessionToRedis(session, 120);
      console.log(
        `[OLOGLE Matchmaker] Queued in Redis session=${session.sessionId.slice(0, 10)} mode=${queueMode}`
      );

      // Re-check shared queue once more to close simultaneous cross-instance join windows
      if (session.currentMode === 'matching') {
        await this.tryMatchFromSharedQueue(session, queuedSession);
      }
    } catch (err: any) {
      console.warn('[OLOGLE Matchmaker] Distributed enqueue error:', err?.message || err);
    }
  }

  private async tryMatchFromSharedQueue(session: EphemeralSession, queuedSession: QueuedSession): Promise<boolean> {
    if (!this.stateManager.isDistributed()) return false;
    if (session.currentMode !== 'matching' || !session.ws || session.ws.readyState !== WebSocket.OPEN) return false;
    if (this.distributedMatchInFlight.has(session.sessionId)) return false;

    this.distributedMatchInFlight.add(session.sessionId);
    try {
      const mode = this.getQueueMode(session.sessionMode);
      const tickets = await this.stateManager.getSharedQueueTickets(mode);
      if (!tickets || tickets.length === 0) return false;

      const now = Date.now();
      const MAX_TICKET_AGE_MS = 90_000;

      const candidates = tickets.filter((t) => {
        if (!t || !t.sessionId) return false;
        if (t.sessionId === session.sessionId) return false;
        if (t.timestamp && now - t.timestamp > MAX_TICKET_AGE_MS) return false;
        if (!session.isLoopback && session.visitorId && t.visitorId && t.visitorId === session.visitorId) {
          return false;
        }
        if (session.blockedSessionIds?.has(t.sessionId)) return false;
        if (t.visitorId && session.blockedVisitorIds?.has(t.visitorId)) return false;
        if (Array.isArray(t.blockedSessionIds) && t.blockedSessionIds.includes(session.sessionId)) return false;
        if (
          session.visitorId &&
          Array.isArray(t.blockedVisitorIds) &&
          t.blockedVisitorIds.includes(session.visitorId)
        ) {
          return false;
        }
        const local = this.sessions.get(t.sessionId);
        if (local && local.currentMode !== 'matching') return false;
        return true;
      });

      if (candidates.length === 0) return false;

      // Score and sort all eligible candidates descending so we can skip stale ghost tickets cleanly
      const scoredCandidates = candidates
        .filter((candidate) => {
          // If strict profession filtering was requested by either user, enforce matching profession
          if (queuedSession.strictProfessionFilter || candidate.strictProfessionFilter) {
            const profA = (queuedSession.profession || '').toLowerCase().trim();
            const profB = (candidate.profession || '').toLowerCase().trim();
            if (profA !== profB) return false;
          }
          return true;
        })
        .map((candidate) => {
          const candidateQueued: QueuedSession = {
            sessionId: candidate.sessionId,
            visitorId: candidate.visitorId,
            profession: candidate.profession || 'Undisclosed',
            strictProfessionFilter: candidate.strictProfessionFilter,
            interests: candidate.interests || [],
            country: candidate.country || 'GLOBAL',
            mode: this.getQueueMode(candidate.sessionMode || mode),
            queuedAt: candidate.timestamp || now,
            blockedSessionIds: new Set(candidate.blockedSessionIds || []),
            blockedVisitorIds: new Set(candidate.blockedVisitorIds || []),
            matchedSessionId: null,
          };
          const scoreResult = calculateCompatibility(queuedSession, candidateQueued);

          // Progressive relaxation for small user pools:
          const waitTimeMs = Math.max(
            queuedSession.queuedAt ? now - queuedSession.queuedAt : 0,
            candidateQueued.queuedAt ? now - candidateQueued.queuedAt : 0
          );
          if (scoreResult.score === 0 && waitTimeMs >= 6000) {
            scoreResult.score = 60; // Open discovery baseline affinity score
          }

          return {
            candidate,
            candidateQueued,
            scoreResult,
          };
        })
        .sort((a, b) => b.scoreResult.score - a.scoreResult.score);

      for (const { candidate, candidateQueued, scoreResult } of scoredCandidates) {
        if (session.currentMode !== 'matching') return false;

        const claimed = await this.stateManager.claimQueueTicket(candidate.sessionId, mode);
        if (!claimed) continue;

        // Also remove our own ticket from Redis so no third instance claims us simultaneously
        await this.stateManager.removeQueueTicket(session.sessionId, mode);

        const paired = await this.pairSessionsAsync(session, candidateQueued, scoreResult);
        if (paired) {
          return true;
        }

        // Candidate was a stale ghost ticket (0 active Redis subscribers): re-register self and keep checking
        console.warn(
          `[OLOGLE Matchmaker] Skipped stale ghost ticket candidate=${candidate.sessionId.slice(0, 10)} (0 active subscribers)`
        );
      }
    } catch (err: any) {
      console.warn('[OLOGLE Matchmaker] Shared queue matching error:', err.message);
    } finally {
      this.distributedMatchInFlight.delete(session.sessionId);
    }
    return false;
  }

  private cancelMatching(session: EphemeralSession): void {
    if (session.currentMode === 'matching') {
      this.matchQueue.remove(session.sessionId);
      void this.stateManager.removeQueueTicket(session.sessionId, this.getQueueMode(session.sessionMode));
      session.currentMode = 'none';
      this.syncSessionToRedis(session, 60);
      console.log(`[OLOGLE Matchmaker] CANCEL_MATCHING session=${session.sessionId.slice(0, 10)}`);
      if (session.ws) {
        this.send(session.ws, PRE_SERIALIZED_MESSAGES.MATCHING_CANCELED);
      }
    }
  }

  private pairSessions(sessionA: EphemeralSession, candidateB: QueuedSession, scoreResult: MatchScoreResult): void {
    void this.pairSessionsAsync(sessionA, candidateB, scoreResult);
  }

  private async pairSessionsAsync(
    sessionA: EphemeralSession,
    candidateB: QueuedSession,
    scoreResult: MatchScoreResult
  ): Promise<boolean> {
    const sessionB = this.sessions.get(candidateB.sessionId);
    const isRemoteB = !sessionB;

    if (isRemoteB) {
      if (!this.stateManager.isDistributed()) {
        this.matchQueue.remove(candidateB.sessionId);
        return false;
      }
    } else {
      // Local atomic verification to avoid race conditions
      if (!sessionB.ws || sessionB.currentMode !== 'matching') {
        this.matchQueue.remove(candidateB.sessionId);
        // Re-enqueue sessionA if candidate is gone or no longer matching
        const queuedA: QueuedSession = {
          sessionId: sessionA.sessionId,
          visitorId: sessionA.visitorId,
          ipAddress: sessionA.ipAddress,
          isLoopback: sessionA.isLoopback,
          profession: sessionA.profession,
          interests: sessionA.interests,
          country: sessionA.country || 'GLOBAL',
          mode: this.getQueueMode(sessionA.sessionMode),
          queuedAt: Date.now(),
          blockedSessionIds: sessionA.blockedSessionIds,
          matchedSessionId: null,
        };
        this.matchQueue.enqueue(queuedA);
        return false;
      }
    }

    const roomId = `room_1on1_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

    const professionBonus = Boolean(scoreResult.isSharedProfession || scoreResult.isComplementaryProfession);
    const partnerCountry = sessionB ? sessionB.country : candidateB.country;
    const countryBonus = Boolean(sessionA.country && sessionA.country !== 'GLOBAL' && sessionA.country === partnerCountry);

    const scoreBreakdown = {
      sharedInterests: scoreResult.sharedInterests,
      sharedTopics: scoreResult.sharedTopics,
      professionBonus,
      countryBonus,
      totalScore: scoreResult.score,
    };

    const payloadA: ServerMessage = {
      type: 'MATCH_FOUND',
      roomId,
      sharedInterests: scoreResult.sharedInterests,
      sharedTopics: scoreResult.sharedTopics,
      score: scoreResult.score,
      partnerProfession: sessionB ? sessionB.profession : candidateB.profession,
      partnerCountry: sessionB ? (sessionB.country || 'GLOBAL') : (candidateB.country || 'GLOBAL'),
      partnerVisitorId: sessionB ? sessionB.visitorId : candidateB.visitorId,
      scoreBreakdown,
      isInitiator: true,
    };

    const payloadB: ServerMessage = {
      type: 'MATCH_FOUND',
      roomId,
      sharedInterests: scoreResult.sharedInterests,
      sharedTopics: scoreResult.sharedTopics,
      score: scoreResult.score,
      partnerProfession: sessionA.profession,
      partnerCountry: sessionA.country || 'GLOBAL',
      partnerVisitorId: sessionA.visitorId,
      scoreBreakdown,
      isInitiator: false,
    };

    // For cross-instance matches, verify remote delivery BEFORE finalizing sessionA's state
    if (isRemoteB) {
      const delivered = await this.stateManager.publishToSession(
        candidateB.sessionId,
        JSON.stringify({
          ...payloadB,
          partnerSessionId: sessionA.sessionId,
        })
      );
      if (!delivered) {
        return false;
      }
    }

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = candidateB.sessionId;
    sessionA.currentRoomId = roomId;
    sessionA.partnerProfession = payloadA.partnerProfession;
    sessionA.partnerCountry = payloadA.partnerCountry;
    sessionA.partnerVisitorId = payloadA.partnerVisitorId;
    sessionA.sharedInterests = scoreResult.sharedInterests;
    sessionA.sharedTopics = scoreResult.sharedTopics;
    sessionA.matchScore = scoreResult.score;
    sessionA.scoreBreakdown = scoreBreakdown;
    sessionA.isInitiator = true;
    sessionA.isRecovering = false;
    sessionA.endedWhileOfflineReason = null;
    this.matchQueue.remove(sessionA.sessionId);
    this.syncSessionToRedis(sessionA, 120);

    if (sessionB) {
      sessionB.currentMode = '1on1';
      sessionB.pairedSessionId = sessionA.sessionId;
      sessionB.currentRoomId = roomId;
      sessionB.partnerProfession = sessionA.profession;
      sessionB.partnerCountry = sessionA.country || 'GLOBAL';
      sessionB.partnerVisitorId = sessionA.visitorId;
      sessionB.sharedInterests = scoreResult.sharedInterests;
      sessionB.sharedTopics = scoreResult.sharedTopics;
      sessionB.matchScore = scoreResult.score;
      sessionB.scoreBreakdown = scoreBreakdown;
      sessionB.isInitiator = false;
      sessionB.isRecovering = false;
      sessionB.endedWhileOfflineReason = null;
      this.matchQueue.remove(sessionB.sessionId);
      this.syncSessionToRedis(sessionB, 120);
    }

    const modeA = this.getQueueMode(sessionA.sessionMode);
    const modeB = this.getQueueMode(candidateB.mode || sessionA.sessionMode);
    void this.stateManager.removeQueueTicket(sessionA.sessionId, modeA);
    void this.stateManager.removeQueueTicket(candidateB.sessionId, modeB);

    if (sessionA.ws) this.send(sessionA.ws, payloadA);
    if (sessionB && sessionB.ws) {
      this.send(sessionB.ws, payloadB);
    }

    console.log(
      `[OLOGLE Matchmaker] MATCH_FOUND room=${roomId} scope=${isRemoteB ? 'cross-instance' : 'local'} mode=${modeA} score=${scoreResult.score}`
    );
    return true;
  }

  private handlePadUpdate(session: EphemeralSession, content: string, language?: string): void {
    if (session.currentMode === '1on1' && session.pairedSessionId) {
      this.sendToPartner(session.pairedSessionId, {
        type: 'PAD_UPDATE',
        content: (content || '').slice(0, 10000),
        language: (language || 'javascript').slice(0, 50),
        senderSessionId: session.sessionId.substring(0, 6),
      });
    }
  }

  private handleTypingIndicator(session: EphemeralSession, isTyping: boolean): void {
    if (session.currentMode === '1on1' && session.pairedSessionId) {
      this.sendToPartner(session.pairedSessionId, {
        type: 'TYPING_INDICATOR',
        isTyping: Boolean(isTyping),
        senderSessionId: session.sessionId.substring(0, 6),
      });
    }
  }

  private updatePartnerEndState(
    partnerId: string,
    originSessionId: string,
    reason: 'skipped' | 'ended' | 'reported' | 'blocked'
  ): void {
    const partner = this.sessions.get(partnerId);
    if (partner) {
      if (reason === 'skipped' || reason === 'reported' || reason === 'blocked') {
        partner.blockedSessionIds.add(originSessionId);
      }
      partner.currentMode = 'none';
      partner.pairedSessionId = null;
      partner.currentRoomId = null;
      if (!partner.ws) {
        partner.endedWhileOfflineReason = reason;
      }
      this.syncSessionToRedis(partner, 60);
    } else if (this.stateManager.isDistributed()) {
      this.stateManager
        .getSessionState(partnerId)
        .then((cached) => {
          if (cached && cached.pairedSessionId === originSessionId) {
            cached.currentMode = 'none';
            cached.pairedSessionId = null;
            cached.currentRoomId = null;
            if (cached.isRecovering) {
              cached.endedWhileOfflineReason = reason;
            }
            this.stateManager.setSessionState(partnerId, cached, 60);
          }
        })
        .catch(() => {});
    }
  }

  private handleNextPerson(session: EphemeralSession): void {
    const now = Date.now();

    // Enforce server-authoritative rate limiting on skips
    session.skipTimestamps = session.skipTimestamps.filter((t) => now - t < this.SKIP_WINDOW_MS);
    if (session.skipTimestamps.length >= this.MAX_SKIPS_IN_WINDOW) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'RATE_LIMIT_COOLDOWN',
          cooldownSeconds: this.SKIP_COOLDOWN_SECONDS,
          reason: 'You are skipping too quickly. Please pause for a few seconds.',
        });
      }
      return;
    }
    session.skipTimestamps.push(now);

    const prevPartnerId = session.pairedSessionId;
    if (prevPartnerId) {
      // Prevent immediate rematch between these two in this queue cycle
      session.blockedSessionIds.add(prevPartnerId);
      this.notifyPartnerLeft(prevPartnerId, 'skipped');
      this.updatePartnerEndState(prevPartnerId, session.sessionId, 'skipped');
    }

    session.pairedSessionId = null;
    session.currentRoomId = null;
    session.isRecovering = false;

    // Put straight back into matching queue
    this.startMatching(session, session.profession, session.interests, session.country);
  }

  private handleEndConversation(session: EphemeralSession): void {
    const partnerId = session.pairedSessionId;
    if (partnerId) {
      this.notifyPartnerLeft(partnerId, 'ended');
      this.updatePartnerEndState(partnerId, session.sessionId, 'ended');
    }

    session.currentMode = 'none';
    session.pairedSessionId = null;
    session.currentRoomId = null;
    session.isRecovering = false;
    this.syncSessionToRedis(session, 60);
  }

  private handleReportUser(session: EphemeralSession, category: string, details?: string): void {
    const partnerId = session.pairedSessionId;
    if (!partnerId) return;

    const partner = this.sessions.get(partnerId);

    // Mutual separation
    session.blockedSessionIds.add(partnerId);
    if (partner) {
      partner.blockedSessionIds.add(session.sessionId);
      partner.violationCount += 1;

      // If user receives multiple reports, apply temporary restriction
      if (partner.violationCount >= 2) {
        partner.isRestrictedUntil = Date.now() + 5 * 60 * 1000;
        if (partner.ws) {
          this.send(partner.ws, {
            type: 'TEMPORARY_RESTRICTION',
            restrictedUntil: partner.isRestrictedUntil,
            reason: `Account received multiple community safety reports (${category}).`,
          });
        }
      }

      // Route immediately to live ephemeral moderation queue for human inspection
      this.createEphemeralTicket(
        partner,
        category,
        details || `User flagged/reported for ${category}`,
        0.95
      );
    }

    this.notifyPartnerLeft(partnerId, 'reported');
    this.updatePartnerEndState(partnerId, session.sessionId, 'reported');

    session.currentMode = 'none';
    session.pairedSessionId = null;
    session.currentRoomId = null;
    session.isRecovering = false;
    this.syncSessionToRedis(session, 60);
  }

  private handleFlagLiveStream(session: EphemeralSession, reason: string, confidence: number): void {
    const partnerId = session.pairedSessionId;
    const flaggedTarget = partnerId ? this.sessions.get(partnerId) || session : session;
    this.createEphemeralTicket(flaggedTarget, 'inappropriate_stream', reason || 'On-device vision model flagged stream', confidence || 0.85);
  }

  private handleModAction(modSession: EphemeralSession, ticketId: string, action: 'KICK' | 'MUTE' | 'DISMISS'): void {
    const ticket = this.ephemeralTickets.get(ticketId);
    if (!ticket) return;

    if (action === 'KICK') {
      for (const s of this.sessions.values()) {
        if (s.currentRoomId === ticket.roomId || s.ipAddress === ticket.sessionHash) {
          if (s.pairedSessionId) {
            this.notifyPartnerLeft(s.pairedSessionId, 'blocked');
            const partner = this.sessions.get(s.pairedSessionId);
            if (partner) {
              partner.currentMode = 'none';
              partner.pairedSessionId = null;
              partner.currentRoomId = null;
            }
          }
          s.currentMode = 'none';
          s.pairedSessionId = null;
          s.currentRoomId = null;
          s.isRestrictedUntil = Date.now() + 30 * 60 * 1000;
          if (s.ws) {
            this.send(s.ws, {
              type: 'TEMPORARY_RESTRICTION',
              restrictedUntil: s.isRestrictedUntil,
              reason: 'Removed from live session by human moderator for community safety inspection.',
            });
          }
        }
      }
      this.ephemeralTickets.delete(ticketId);
      this.broadcastModerationQueue();
    } else if (action === 'MUTE') {
      for (const s of this.sessions.values()) {
        if (s.currentRoomId === ticket.roomId || s.ipAddress === ticket.sessionHash) {
          s.isMutedUntil = Date.now() + 10 * 60 * 1000;
          if (s.ws) {
            this.send(s.ws, {
              type: 'STREAM_MUTED_BY_MODERATOR',
              reason: 'Stream audio and video muted by human moderator for safety verification.',
            });
          }
        }
      }
    } else if (action === 'DISMISS') {
      this.ephemeralTickets.delete(ticketId);
      this.broadcastModerationQueue();
    }
  }

  private handleBlockUser(session: EphemeralSession): void {
    const partnerId = session.pairedSessionId;
    if (!partnerId) return;

    session.blockedSessionIds.add(partnerId);
    const partner = this.sessions.get(partnerId);
    const partnerVisitorId = partner ? partner.visitorId : session.partnerVisitorId;
    if (partnerVisitorId && session.visitorId) {
      this.blockedPairs.add(`${session.visitorId}:${partnerVisitorId}`);
      this.blockedPairs.add(`${partnerVisitorId}:${session.visitorId}`);
      session.blockedVisitorIds.add(partnerVisitorId);
      if (partner) partner.blockedVisitorIds.add(session.visitorId);
    }

    this.notifyPartnerLeft(partnerId, 'blocked');
    this.updatePartnerEndState(partnerId, session.sessionId, 'blocked');

    session.currentMode = 'none';
    session.pairedSessionId = null;
    session.currentRoomId = null;
    session.isRecovering = false;
    this.syncSessionToRedis(session, 60);
  }

  public handleBlockPartner(blockerSession: EphemeralSession, blockerId: string, blockedId: string, reason: string): void {
    const effectiveBlockerId = blockerId || blockerSession.visitorId;
    const partnerId = blockerSession.pairedSessionId;
    const partner = partnerId ? this.sessions.get(partnerId) : null;
    const effectiveBlockedId = blockedId || (partner ? partner.visitorId : '');
    const partnerIp = partner ? partner.ipAddress : '';

    // 1. Maintain in-memory store of blocked pairs (bidirectional lookup)
    if (effectiveBlockerId && effectiveBlockedId) {
      this.blockedPairs.add(`${effectiveBlockerId}:${effectiveBlockedId}`);
      this.blockedPairs.add(`${effectiveBlockedId}:${effectiveBlockerId}`);
      blockerSession.blockedVisitorIds.add(effectiveBlockedId);
      if (partner) partner.blockedVisitorIds.add(effectiveBlockerId);
    }

    if (blockerSession.ipAddress && partnerIp) {
      this.blockedIpPairs.add(`${blockerSession.ipAddress}:${partnerIp}`);
      this.blockedIpPairs.add(`${partnerIp}:${blockerSession.ipAddress}`);
    }

    // 2. Track total block reports per user ID & IP address within rolling 10-minute window
    const now = Date.now();
    let shouldBanPartner = false;
    let banUntil = now + this.BAN_DURATION_MS;

    if (effectiveBlockedId) {
      const timestamps = (this.reportsByVisitorId.get(effectiveBlockedId) || []).filter(
        (t) => now - t < this.REPORT_ROLLING_WINDOW_MS
      );
      timestamps.push(now);
      this.reportsByVisitorId.set(effectiveBlockedId, timestamps);

      if (timestamps.length >= this.BAN_THRESHOLD) {
        shouldBanPartner = true;
        this.bannedVisitorIds.set(effectiveBlockedId, banUntil);
      }
    }

    if (partnerIp) {
      const ipTimestamps = (this.reportsByIp.get(partnerIp) || []).filter(
        (t) => now - t < this.REPORT_ROLLING_WINDOW_MS
      );
      ipTimestamps.push(now);
      this.reportsByIp.set(partnerIp, ipTimestamps);

      if (ipTimestamps.length >= this.BAN_THRESHOLD) {
        shouldBanPartner = true;
        this.bannedIps.set(partnerIp, banUntil);
      }
    }

    // 3. Immediately terminate WebRTC media stream & text socket room
    if (partnerId) {
      blockerSession.blockedSessionIds.add(partnerId);
    }
    if (partner) {
      partner.blockedSessionIds.add(blockerSession.sessionId);

      if (shouldBanPartner) {
        partner.isRestrictedUntil = banUntil;
        if (partner.ws) {
          this.send(partner.ws, {
            type: 'TEMPORARY_RESTRICTION',
            restrictedUntil: banUntil,
            reason: `Account temporarily restricted for 30 minutes following multiple community reports (${reason || 'Safety violation'}).`,
          });
        }
      }
    }

    if (partnerId) {
      this.notifyPartnerLeft(partnerId, 'blocked');
      this.updatePartnerEndState(partnerId, blockerSession.sessionId, 'blocked');
    }

    blockerSession.currentMode = 'none';
    blockerSession.pairedSessionId = null;
    blockerSession.currentRoomId = null;
    blockerSession.isRecovering = false;
    this.syncSessionToRedis(blockerSession, 60);
  }

  private handleWebRTCSignal(session: EphemeralSession, roomId: string | undefined, signal: any): void {
    if (
      session.currentMode !== '1on1' ||
      !session.pairedSessionId ||
      !session.currentRoomId ||
      (roomId && roomId !== session.currentRoomId)
    ) {
      return;
    }

    if (
      !signal ||
      typeof signal !== 'object' ||
      !['offer', 'answer', 'candidate'].includes(signal.type) ||
      JSON.stringify(signal).length > 128_000
    ) {
      return;
    }

    const partner = this.sessions.get(session.pairedSessionId);
    if (partner) {
      if (
        partner.currentMode !== '1on1' ||
        partner.pairedSessionId !== session.sessionId ||
        partner.currentRoomId !== session.currentRoomId
      ) {
        return;
      }
    }

    console.log(
      `[OLOGLE WebRTC] Relay signal=${signal.type} room=${session.currentRoomId} from=${session.sessionId.slice(0, 10)}`
    );
    this.sendToPartner(session.pairedSessionId, { type: 'WEBRTC_SIGNAL', signal });
  }

  private handleVoiceTranscript(
    session: EphemeralSession,
    text: string,
    language: string,
    isFinal: boolean,
    id?: string,
    timestamp?: number
  ): void {
    if (session.currentMode !== '1on1' || !session.pairedSessionId) {
      return;
    }

    const clean = (text || '').trim();
    if (!clean) return;

    this.sendToPartner(session.pairedSessionId, {
      type: 'VOICE_TRANSCRIPT',
      id: id || `tr_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`,
      text: clean,
      language: language || 'en',
      isFinal: !!isFinal,
      senderSessionId: session.sessionId.substring(0, 6),
      timestamp: timestamp || Date.now(),
    });
  }

  // --- Message Handling & Safety Moderation ---

  private handle1on1Message(session: EphemeralSession, text: string): void {
    if (session.currentMode !== '1on1' || !session.pairedSessionId) {
      return;
    }

    const clean = (text || '').trim();
    if (!clean) return;

    if (clean.length > 2000) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'ERROR',
          message: 'Message exceeds maximum allowable length of 2,000 characters.',
        });
      }
      return;
    }

    const now = Date.now();

    // Check mute
    if (session.isMutedUntil > now) {
      const remainingSec = Math.ceil((session.isMutedUntil - now) / 1000);
      if (session.ws) {
        this.send(session.ws, {
          type: 'RATE_LIMIT_COOLDOWN',
          cooldownSeconds: remainingSec,
          reason: 'You are temporarily muted due to community safety violations.',
        });
      }
      return;
    }

    // Rate limit messages
    session.messageTimestamps = session.messageTimestamps.filter((t) => now - t < this.MESSAGE_WINDOW_MS);
    if (session.messageTimestamps.length >= this.MAX_MESSAGES_IN_WINDOW) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'RATE_LIMIT_COOLDOWN',
          cooldownSeconds: 5,
          reason: 'You are sending messages too quickly. Please slow down.',
        });
      }
      return;
    }
    session.messageTimestamps.push(now);

    // Pass through Safety Agent before relay
    const safetyResult = defaultSafetyAgent.classifyMessage(text);
    const messageId = `msg_${now}_${Math.random().toString(36).substring(2, 6)}`;

    // High confidence violation -> BLOCK
    if (safetyResult.severityTier === 'high') {
      session.violationCount += 1;

      // Instantly route flagged session to live human moderation queue
      this.createEphemeralTicket(
        session,
        safetyResult.category || 'in_flight_violation',
        safetyResult.reason || 'Blocked in-flight message violation',
        safetyResult.confidence
      );

      if (session.ws) {
        this.send(session.ws, {
          type: 'MESSAGE_BLOCKED',
          reason: safetyResult.senderFeedback || 'Message blocked: violated community safety guidelines.',
        });
      }

      // Escalation logic
      if (session.violationCount === 2) {
        session.isMutedUntil = Date.now() + 60 * 1000;
        if (session.ws) {
          this.send(session.ws, {
            type: 'SAFETY_WARNING',
            message: 'Repeated safety violations. You have been muted for 60 seconds.',
          });
        }
      } else if (session.violationCount >= 3) {
        session.isRestrictedUntil = Date.now() + 5 * 60 * 1000;
        this.handleEndConversation(session);
        if (session.ws) {
          this.send(session.ws, {
            type: 'TEMPORARY_RESTRICTION',
            restrictedUntil: session.isRestrictedUntil,
            reason: 'Excessive safety violations. 5-minute session restriction active.',
          });
        }
      }
      return;
    }

    // Medium confidence violation -> Deliver + Show warning banner to sender
    if (safetyResult.severityTier === 'medium') {
      if (session.ws) {
        this.send(session.ws, {
          type: 'SAFETY_WARNING',
          message: safetyResult.senderFeedback || 'Please keep conversations respectful.',
        });
      }
    }

    // Deliver to self & peer
    if (session.ws) {
      this.send(session.ws, {
        type: 'RECEIVE_1ON1_MESSAGE',
        id: messageId,
        text,
        sender: 'self',
        timestamp: now,
      });
    }

    this.sendToPartner(session.pairedSessionId, {
      type: 'RECEIVE_1ON1_MESSAGE',
      id: messageId,
      text,
      sender: 'peer',
      timestamp: now,
    });
  }

  private handleMessageRead(session: EphemeralSession, messageId: string): void {
    if (!messageId || typeof messageId !== 'string') return;
    if (session.currentMode !== '1on1' || !session.pairedSessionId) return;

    this.sendToPartner(session.pairedSessionId, {
      type: 'MESSAGE_READ',
      messageId,
      readAt: Date.now(),
    });
  }

  // --- Community Rooms ---

  private joinCommunity(session: EphemeralSession, communityId: string): void {
    this.cleanCurrentSessionState(session);

    let room = this.communityRooms.get(communityId);
    if (!room) {
      if (session.ws) this.send(session.ws, { type: 'ERROR', message: 'Community room not found.' });
      return;
    }

    if (room.memberSessionIds.size >= room.maxParticipants) {
      if (session.ws) this.send(session.ws, { type: 'ERROR', message: 'Room has reached maximum capacity (30/30).' });
      return;
    }

    room.memberSessionIds.add(session.sessionId);
    session.currentMode = 'community';
    session.currentRoomId = room.id;

    if (session.ws) {
      this.send(session.ws, {
        type: 'COMMUNITY_JOINED',
        room: {
          id: room.id,
          name: room.name,
          topic: room.topic,
          category: room.category,
          participantCount: room.memberSessionIds.size,
          maxParticipants: room.maxParticipants,
        },
      });
    }

    this.broadcastCommunityUpdate(room);
  }

  private createCommunity(session: EphemeralSession, name: string, topic: string, category: string): void {
    const cleanName = name.trim().substring(0, 40);
    const cleanTopic = topic.trim().substring(0, 80);
    if (!cleanName || !cleanTopic) return;

    const id = `comm-custom-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
    const newRoom: CommunityRoom = {
      id,
      name: cleanName,
      topic: cleanTopic,
      description: `Created for discussions on ${cleanTopic}`,
      category: category || 'General',
      maxParticipants: 30,
      memberSessionIds: new Set(),
      isPreset: false,
      createdAt: Date.now(),
    };

    this.communityRooms.set(id, newRoom);
    this.joinCommunity(session, id);
  }

  private leaveCommunity(session: EphemeralSession): void {
    if (session.currentMode !== 'community' || !session.currentRoomId) return;

    const room = this.communityRooms.get(session.currentRoomId);
    if (room) {
      room.memberSessionIds.delete(session.sessionId);
      this.broadcastCommunityUpdate(room);

      // Auto-destroy non-preset rooms when empty
      if (!room.isPreset && room.memberSessionIds.size === 0) {
        this.communityRooms.delete(room.id);
      }
    }

    session.currentMode = 'none';
    session.currentRoomId = null;
  }

  private handleCommunityMessage(session: EphemeralSession, text: string): void {
    if (session.currentMode !== 'community' || !session.currentRoomId) return;
    const room = this.communityRooms.get(session.currentRoomId);
    if (!room) return;

    const now = Date.now();
    if (session.isMutedUntil > now) {
      if (session.ws) {
        this.send(session.ws, {
          type: 'RATE_LIMIT_COOLDOWN',
          cooldownSeconds: Math.ceil((session.isMutedUntil - now) / 1000),
          reason: 'You are muted in community rooms.',
        });
      }
      return;
    }

    const safetyResult = defaultSafetyAgent.classifyMessage(text);
    if (safetyResult.severityTier === 'high') {
      if (session.ws) {
        this.send(session.ws, {
          type: 'MESSAGE_BLOCKED',
          reason: safetyResult.senderFeedback || 'Message blocked by moderation rules.',
        });
      }
      return;
    }

    if (safetyResult.severityTier === 'medium' && session.ws) {
      this.send(session.ws, {
        type: 'SAFETY_WARNING',
        message: safetyResult.senderFeedback || 'Please adhere to community rules.',
      });
    }

    const messageId = `cmsg_${now}_${Math.random().toString(36).substring(2, 6)}`;

    // Broadcast to all active members
    for (const memberId of room.memberSessionIds) {
      const member = this.sessions.get(memberId);
      if (member && member.ws) {
        this.send(member.ws, {
          type: 'RECEIVE_COMMUNITY_MESSAGE',
          id: messageId,
          text,
          senderSessionId: session.sessionId.substring(0, 6),
          isSelf: memberId === session.sessionId,
          timestamp: now,
        });
      }
    }
  }

  private broadcastCommunityUpdate(room: CommunityRoom): void {
    for (const memberId of room.memberSessionIds) {
      const member = this.sessions.get(memberId);
      if (member && member.ws) {
        this.send(member.ws, {
          type: 'COMMUNITY_UPDATE',
          participantCount: room.memberSessionIds.size,
        });
      }
    }
  }

  // --- Classroom Mode ---

  private joinClass(session: EphemeralSession, classId: string): void {
    this.cleanCurrentSessionState(session);

    let room = this.classRooms.get(classId);
    if (!room) {
      if (session.ws) this.send(session.ws, { type: 'ERROR', message: 'Classroom not found.' });
      return;
    }

    room.participantSessionIds.add(session.sessionId);
    session.currentMode = 'class';
    session.currentRoomId = room.id;

    const isHost = room.hostSessionId === session.sessionId;

    if (session.ws) {
      this.send(session.ws, {
        type: 'CLASS_JOINED',
        room: {
          id: room.id,
          name: room.name,
          topic: room.topic,
          isHost,
          participantCount: room.participantSessionIds.size,
          questions: room.questions,
        },
      });
    }

    this.broadcastClassUpdate(room);
  }

  private createClass(session: EphemeralSession, name: string, topic: string): void {
    const cleanName = name.trim().substring(0, 40);
    const cleanTopic = topic.trim().substring(0, 80);
    if (!cleanName || !cleanTopic) return;

    const id = `class-${Date.now()}-${Math.random().toString(36).substring(2, 6)}`;
    const newClass: ClassRoom = {
      id,
      name: cleanName,
      topic: cleanTopic,
      hostSessionId: session.sessionId, // Ephemeral contextual host
      participantSessionIds: new Set(),
      questions: [],
      createdAt: Date.now(),
    };

    this.classRooms.set(id, newClass);
    this.joinClass(session, id);
  }

  private leaveClass(session: EphemeralSession): void {
    if (session.currentMode !== 'class' || !session.currentRoomId) return;

    const room = this.classRooms.get(session.currentRoomId);
    if (room) {
      room.participantSessionIds.delete(session.sessionId);

      // If host leaves and there are participants, designate the next earliest participant as ephemeral host
      if (room.hostSessionId === session.sessionId && room.participantSessionIds.size > 0) {
        const nextHost = Array.from(room.participantSessionIds)[0];
        room.hostSessionId = nextHost;
      }

      this.broadcastClassUpdate(room);

      if (room.participantSessionIds.size === 0 && !room.id.startsWith('class-system')) {
        this.classRooms.delete(room.id);
      }
    }

    session.currentMode = 'none';
    session.currentRoomId = null;
  }

  private handleClassMessage(session: EphemeralSession, text: string): void {
    if (session.currentMode !== 'class' || !session.currentRoomId) return;
    const room = this.classRooms.get(session.currentRoomId);
    if (!room) return;

    const safetyResult = defaultSafetyAgent.classifyMessage(text);
    if (safetyResult.severityTier === 'high') {
      if (session.ws) {
        this.send(session.ws, {
          type: 'MESSAGE_BLOCKED',
          reason: safetyResult.senderFeedback || 'Message blocked.',
        });
      }
      return;
    }

    const now = Date.now();
    const isHost = room.hostSessionId === session.sessionId;
    const messageId = `clmsg_${now}_${Math.random().toString(36).substring(2, 6)}`;

    for (const pId of room.participantSessionIds) {
      const pSession = this.sessions.get(pId);
      if (pSession && pSession.ws) {
        this.send(pSession.ws, {
          type: 'RECEIVE_CLASS_MESSAGE',
          id: messageId,
          text,
          senderSessionId: session.sessionId.substring(0, 6),
          isHost,
          isSelf: pId === session.sessionId,
          timestamp: now,
        });
      }
    }
  }

  private handleSubmitClassQuestion(session: EphemeralSession, question: string): void {
    if (session.currentMode !== 'class' || !session.currentRoomId) return;
    const room = this.classRooms.get(session.currentRoomId);
    if (!room) return;

    const cleanQ = question.trim().substring(0, 200);
    if (!cleanQ) return;

    const qItem: ClassQuestion = {
      id: `q_${Date.now()}_${Math.random().toString(36).substring(2, 6)}`,
      askerSessionId: session.sessionId.substring(0, 6),
      question: cleanQ,
      timestamp: Date.now(),
    };

    room.questions.push(qItem);
    this.broadcastClassUpdate(room);
  }

  private handleAnswerClassQuestion(session: EphemeralSession, questionId: string, answer: string): void {
    if (session.currentMode !== 'class' || !session.currentRoomId) return;
    const room = this.classRooms.get(session.currentRoomId);
    if (!room) return;

    const q = room.questions.find((item) => item.id === questionId);
    if (q) {
      q.answer = answer.trim().substring(0, 300);
      q.answeredAt = Date.now();
      this.broadcastClassUpdate(room);
    }
  }

  private broadcastClassUpdate(room: ClassRoom): void {
    for (const pId of room.participantSessionIds) {
      const pSession = this.sessions.get(pId);
      if (pSession && pSession.ws) {
        this.send(pSession.ws, {
          type: 'CLASS_UPDATE',
          participantCount: room.participantSessionIds.size,
          questions: room.questions,
        });
      }
    }
  }

  // --- Explore / Discovery ---

  public sendExploreData(session: EphemeralSession): void {
    const communities = Array.from(this.communityRooms.values()).map((c) => ({
      id: c.id,
      name: c.name,
      topic: c.topic,
      category: c.category,
      count: c.memberSessionIds.size,
      max: c.maxParticipants,
    }));

    const classes = Array.from(this.classRooms.values()).map((cl) => ({
      id: cl.id,
      name: cl.name,
      topic: cl.topic,
      count: cl.participantSessionIds.size,
    }));

    if (session.ws) {
      this.send(session.ws, {
        type: 'EXPLORE_DATA',
        communities,
        classes,
      });
    }
  }

  // --- Utilities ---

  private cleanCurrentSessionState(session: EphemeralSession): void {
    if (session.currentMode === 'matching') {
      this.matchQueue.remove(session.sessionId);
    } else if (session.currentMode === '1on1' && session.pairedSessionId) {
      this.handleEndConversation(session);
    } else if (session.currentMode === 'community') {
      this.leaveCommunity(session);
    } else if (session.currentMode === 'class') {
      this.leaveClass(session);
    }
  }

  private notifyPartnerLeft(
    partnerId: string,
    reason: 'skipped' | 'ended' | 'disconnected' | 'reported' | 'blocked'
  ): void {
    const preSerialized = PRE_SERIALIZED_PARTNER_LEFT[reason];
    this.sendToPartner(partnerId, preSerialized || { type: 'PARTNER_LEFT', reason });
  }

  private sendToPartner(partnerId: string, message: ServerMessage | string): void {
    const partner = this.sessions.get(partnerId);
    if (partner && partner.ws && partner.ws.readyState === WebSocket.OPEN) {
      this.send(partner.ws, message);
    } else {
      const payload = typeof message === 'string' ? message : JSON.stringify(message);
      this.stateManager.publishToSession(partnerId, payload);
    }
  }

  public broadcastShutdown(reason: string = 'maintenance'): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    if (this.distributedQueueSyncTimer) {
      clearInterval(this.distributedQueueSyncTimer);
      this.distributedQueueSyncTimer = null;
    }
    this.distributedMatchInFlight.clear();
    for (const timer of this.disconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.disconnectTimers.clear();

    const payload = JSON.stringify({ type: 'SERVER_SHUTDOWN', reason });
    for (const session of this.sessions.values()) {
      if (session.ws && session.ws.readyState === WebSocket.OPEN) {
        try {
          session.ws.send(payload);
          session.ws.close(1001, 'Going Away');
        } catch {}
      }
    }
  }

  private send(ws: WebSocket, message: ServerMessage | string): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(typeof message === 'string' ? message : JSON.stringify(message));
    }
  }
}

export const ologleEngine = new OlogleEngine();
export const haylooEngine = ologleEngine;
