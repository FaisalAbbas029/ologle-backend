import express, { Express } from 'express';
import { Server as HttpServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { ologleEngine } from './signaling/serverCore.ts';
import { createIceServerConfig } from './services/iceServers.ts';
import { distributedState } from './services/distributedState.ts';
import { sanitizeText } from './validation/index.ts';
import {
  transcribeAudioWithGemini,
  translateTextWithGemini,
  autoDetectAndTranslateWithGemini,
  generateIcebreakerWithGemini,
  generateAIPracticeReply,
} from './services/geminiService.ts';

const allowedOrigins = new Set([
  'https://ologle-five.vercel.app',
  'https://ologle.network',
  'https://www.ologle.network',
  ...(process.env.ALLOWED_ORIGINS || '').split(',').map((origin) => origin.trim()).filter(Boolean),
]);

export function isOriginAllowed(origin: string | undefined): boolean {
  if (!origin) return true; // non-browser requests (curl, server-to-server, health monitors)
  if (allowedOrigins.has(origin)) return true;

  // Allow Vercel production and preview deployments
  if (/^https:\/\/[a-zA-Z0-9._-]+\.vercel\.app$/.test(origin)) return true;

  // Allow local development origins
  if (/^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin)) return true;

  // Allow hosting self-ping origins
  if (/^https:\/\/[a-zA-Z0-9-]+\.onrender\.com$/.test(origin)) return true;

  return false;
}

// In-memory LRU Response Cache for Gemini endpoints
interface CacheEntry<T> {
  data: T;
  expiry: number;
}
const apiCache = new Map<string, CacheEntry<any>>();
const CACHE_MAX_SIZE = 500;
const CACHE_TTL_MS = 15 * 60 * 1000;

function getCached<T>(key: string): T | null {
  const entry = apiCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiry) {
    apiCache.delete(key);
    return null;
  }
  return entry.data as T;
}

function setCached<T>(key: string, data: T, ttlMs = CACHE_TTL_MS): void {
  if (apiCache.size >= CACHE_MAX_SIZE) {
    const firstKey = apiCache.keys().next().value;
    if (firstKey) apiCache.delete(firstKey);
  }
  apiCache.set(key, { data, expiry: Date.now() + ttlMs });
}

export function sendProblemDetails(
  res: express.Response,
  status: number,
  title: string,
  detail: string,
  typePath: string = 'general-error'
): void {
  res.status(status).setHeader('Content-Type', 'application/problem+json').json({
    type: `https://ologle.vercel.app/errors/${typePath}`,
    title,
    status,
    detail,
  });
}

export function createOlogleApp(): Express {
  const app = express();

  // Security Headers Middleware
  app.use((_req, res, next) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
    res.setHeader('X-XSS-Protection', '1; mode=block');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    next();
  });

  app.use(express.json({ limit: '25mb' }));

  // CORS Middleware for all routes
  app.use((req, res, next) => {
    const origin = req.get('origin');
    if (origin) {
      if (!isOriginAllowed(origin)) {
        return sendProblemDetails(
          res,
          403,
          'Forbidden',
          'Origin not allowed by CORS security policy.',
          'cors-forbidden'
        );
      }
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }

    if (req.method === 'OPTIONS') {
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      return res.status(204).end();
    }

    next();
  });

  // Rate Limiter Middleware (Sliding window per client IP: max 120 requests per minute)
  const rateLimits = new Map<string, { count: number; resetAt: number }>();
  const RATE_LIMIT_WINDOW_MS = 60 * 1000;
  const MAX_REQUESTS_PER_WINDOW = 120;

  app.use('/api', (req, res, next) => {
    const ip = (req.headers['x-forwarded-for'] as string) || req.socket.remoteAddress || 'unknown';
    const now = Date.now();
    let bucket = rateLimits.get(ip);

    if (!bucket || now > bucket.resetAt) {
      bucket = { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS };
      rateLimits.set(ip, bucket);
    } else {
      bucket.count++;
    }

    res.setHeader('X-RateLimit-Limit', MAX_REQUESTS_PER_WINDOW.toString());
    res.setHeader('X-RateLimit-Remaining', Math.max(0, MAX_REQUESTS_PER_WINDOW - bucket.count).toString());
    res.setHeader('X-RateLimit-Reset', Math.ceil(bucket.resetAt / 1000).toString());

    if (bucket.count > MAX_REQUESTS_PER_WINDOW) {
      const retryAfterSec = Math.max(1, Math.ceil((bucket.resetAt - now) / 1000));
      res.setHeader('Retry-After', retryAfterSec.toString());
      return sendProblemDetails(
        res,
        429,
        'Too Many Requests',
        'Rate limit exceeded. Please wait a moment before sending another request.',
        'rate-limit-exceeded'
      );
    }
    next();
  });

  // Health and Status Endpoints
  const handleHealth = (_req: express.Request, res: express.Response) => {
    const isProd = process.env.NODE_ENV === 'production';
    const redisConfigured = distributedState.isConfigured();
    const redisConnected = distributedState.isDistributed();
    const status = (isProd && !redisConnected) ? 'degraded' : 'ok';

    res.json({
      status,
      service: 'ologle-signaling',
      uptime: process.uptime(),
      timestamp: Date.now(),
      activeSessions: ologleEngine.getActiveSessionsCount(),
      queuedUsers: ologleEngine.getQueuedUsersCount(),
      redis: {
        configured: redisConfigured,
        connected: redisConnected,
        mode: redisConnected ? 'distributed' : (isProd ? 'degraded_in_memory' : 'in_memory'),
      },
    });
  };

  app.get('/health', handleHealth);
  app.get('/api/health', handleHealth);

  app.get('/', (_req, res) => {
    res.json({
      service: 'ologle-signaling',
      status: 'healthy',
      description: 'OLOGLE High-Performance WebRTC Signaling & Real-Time Matchmaking Backend',
      endpoints: {
        health: '/health',
        iceServers: '/api/ice-servers',
        websocket: '/ws',
      },
      stats: {
        activeSessions: ologleEngine.getActiveSessionsCount(),
        queuedUsers: ologleEngine.getQueuedUsersCount(),
      },
    });
  });

  // ICE Servers & Credentials
  app.get('/api/ice-servers', (_req, res) => {
    try {
      res.setHeader('Cache-Control', 'private, max-age=300');
      res.json(createIceServerConfig());
    } catch (error: any) {
      console.error('ICE server configuration error:', error);
      sendProblemDetails(
        res,
        503,
        'Service Unavailable',
        'ICE server configuration is temporarily unavailable.',
        'ice-servers-unavailable'
      );
    }
  });

  // API: Icebreaker / Topic Suggestions
  app.post('/api/suggest-icebreaker', async (req, res) => {
    try {
      const { interests, professionA, professionB } = req.body || {};
      const safeProfA = sanitizeText(professionA, 100);
      const safeProfB = sanitizeText(professionB, 100);
      const safeInterests = Array.isArray(interests)
        ? interests.slice(0, 6).map((i) => sanitizeText(i, 50))
        : [];

      const cacheKey = `icebreak:${safeProfA}:${safeProfB}:${safeInterests.sort().join('|')}`;
      const cached = getCached<{ icebreakers: string[] }>(cacheKey);
      if (cached) {
        res.setHeader('X-Cache', 'HIT');
        return res.json(cached);
      }

      const result = await generateIcebreakerWithGemini(safeInterests, safeProfA, safeProfB);
      setCached(cacheKey, result);
      res.setHeader('X-Cache', 'MISS');
      res.json(result);
    } catch (err: any) {
      console.warn('Icebreaker API error:', err.message);
      res.status(500).json({ error: 'Failed to generate conversation icebreakers' });
    }
  });

  // API: AI Practice Partner
  app.post('/api/ai-practice/reply', async (req, res) => {
    try {
      const { partnerId, profession, interests, messages } = req.body || {};
      const safePartnerId = sanitizeText(partnerId, 80);
      const safeProf = sanitizeText(profession, 100);
      const safeInterests = Array.isArray(interests)
        ? interests.slice(0, 6).map((i) => sanitizeText(i, 50))
        : [];
      const safeMessages: Array<{ role: 'user' | 'assistant'; text: string }> = Array.isArray(messages)
        ? messages.slice(-10).map((m: any) => ({
            role: (m?.role === 'user' ? 'user' : 'assistant') as 'user' | 'assistant',
            text: sanitizeText(m?.text, 1000),
          }))
        : [];

      const reply = await generateAIPracticeReply({
        partnerId: safePartnerId,
        profession: safeProf,
        interests: safeInterests,
        messages: safeMessages,
      });

      res.json({ reply });
    } catch (err: any) {
      console.warn('AI practice endpoint error:', err.message);
      res.status(500).json({ error: err.message || 'AI practice reply failed.' });
    }
  });

  // API: Audio Transcription
  app.post('/api/transcribe-audio', async (req, res) => {
    try {
      const { audioBase64, mimeType, targetLanguage } = req.body;
      if (!audioBase64 || typeof audioBase64 !== 'string') {
        return res.status(400).json({ error: 'Valid audioBase64 string is required' });
      }
      const safeMime = sanitizeText(mimeType, 50) || 'audio/webm';
      const safeLang = sanitizeText(targetLanguage, 20) || 'en';

      const result = await transcribeAudioWithGemini(audioBase64, safeMime, safeLang);
      res.json(result);
    } catch {
      res.status(500).json({ error: 'Failed to transcribe audio' });
    }
  });

  // API: Real-time Caption Translation
  app.post('/api/translate', async (req, res) => {
    try {
      const { text, sourceLanguage, targetLanguage } = req.body;
      const safeText = sanitizeText(text, 2000);
      if (!safeText) {
        return res.status(400).json({ error: 'Valid text is required' });
      }
      const safeSrc = sanitizeText(sourceLanguage, 20) || 'auto';
      const safeTgt = sanitizeText(targetLanguage, 20) || 'en';

      const cacheKey = `trans:${safeSrc}:${safeTgt}:${safeText}`;
      const cached = getCached<{ translatedText: string }>(cacheKey);
      if (cached) {
        res.setHeader('X-Cache', 'HIT');
        return res.json(cached);
      }

      const translatedText = await translateTextWithGemini(safeText, safeSrc, safeTgt);
      const result = { translatedText };
      setCached(cacheKey, result);
      res.setHeader('X-Cache', 'MISS');
      res.json(result);
    } catch {
      res.status(500).json({ error: 'Failed to translate caption' });
    }
  });

  // API: Auto-detect and Translate Message
  app.post('/api/translate-autodetect', async (req, res) => {
    try {
      const { text, targetLanguage } = req.body;
      const safeText = sanitizeText(text, 2000);
      if (!safeText) {
        return res.status(400).json({ error: 'Valid text is required' });
      }
      const safeTgt = sanitizeText(targetLanguage, 20) || 'en';

      const cacheKey = `autodetect:${safeTgt}:${safeText}`;
      const cached = getCached(cacheKey);
      if (cached) {
        res.setHeader('X-Cache', 'HIT');
        return res.json(cached);
      }

      const result = await autoDetectAndTranslateWithGemini(safeText, safeTgt);
      setCached(cacheKey, result);
      res.setHeader('X-Cache', 'MISS');
      res.json(result);
    } catch {
      res.status(500).json({ error: 'Failed to translate message' });
    }
  });

  return app;
}

export function attachWebSocketServer(server: HttpServer): WebSocketServer {
  const wss = new WebSocketServer({
    server,
    path: '/ws',
    perMessageDeflate: false,
    clientTracking: false,
    backlog: 512,
    maxPayload: 65536, // 64 KB strict frame payload limit (mitigates memory exhaustion DoS)
  });

  wss.on('connection', (ws: WebSocket, req) => {
    const origin = req.headers.origin;
    if (origin && !isOriginAllowed(origin)) {
      ws.close(1008, 'Origin not allowed');
      return;
    }

    const forwarded = req.headers['x-forwarded-for'];
    const ip = typeof forwarded === 'string'
      ? forwarded.split(',')[0].trim()
      : req.socket.remoteAddress || '127.0.0.1';

    // Parse URL query parameter for idempotent session resumption (15s grace window)
    let existingSessionId: string | undefined;
    try {
      const url = new URL(req.url || '', 'http://localhost');
      existingSessionId = url.searchParams.get('sessionId') || undefined;
    } catch {}

    const session = ologleEngine.registerSession(ws as any, existingSessionId, ip);

    // Heartbeat watchdog initialization
    (ws as any).isAlive = true;
    ws.on('pong', () => {
      (ws as any).isAlive = true;
    });

    const onCleanup = () => {
      ws.removeAllListeners();
      ologleEngine.handleDisconnect(session.sessionId);
    };

    ws.on('message', (data) => {
      (ws as any).isAlive = true;
      ologleEngine.processMessage(session.sessionId, data.toString());
    });

    ws.on('close', onCleanup);
    ws.on('error', onCleanup);
  });

  return wss;
}

