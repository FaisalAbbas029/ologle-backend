import { createOlogleApp, isOriginAllowed } from '../src/app.ts';
import { WebSocketServer, WebSocket } from 'ws';
import { ologleEngine } from '../src/signaling/serverCore.ts';
import { distributedState } from '../src/services/distributedState.ts';

const app = createOlogleApp();

let wss: WebSocketServer | null = null;

function getWebSocketServer(): WebSocketServer {
  if (!wss) {
    wss = new WebSocketServer({
      noServer: true,
      perMessageDeflate: false,
      clientTracking: false,
      maxPayload: 65536, // 64 KB strict frame payload limit
    });

    wss.on('connection', (ws: WebSocket, req) => {
      const origin = (req.headers.origin as string) || (req.headers['sec-websocket-origin'] as string);
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
        session.lastActivity = Date.now();
      });

      const onCleanup = () => {
        ws.removeAllListeners();
        if (session.ws === (ws as any)) {
          ologleEngine.handleDisconnect(session.sessionId);
        }
      };

      ws.on('message', (data) => {
        (ws as any).isAlive = true;
        ologleEngine.processMessage(session.sessionId, data.toString());
      });

      ws.on('close', onCleanup);
      ws.on('error', onCleanup);
    });
  }
  return wss;
}

export default function handler(req: any, res: any) {
  // Normalize original request URL when routed through Vercel rewrites (preserving query parameters)
  const matchedPath = req.headers['x-matched-path'] as string | undefined;
  if (matchedPath && req.url?.startsWith('/api/index')) {
    const queryIndex = req.url.indexOf('?');
    const query = queryIndex !== -1 ? req.url.slice(queryIndex) : '';
    req.url = matchedPath.includes('?') ? matchedPath : `${matchedPath}${query}`;
  }

  const rawUrl = req.url || '';
  const pathname = rawUrl.split('?')[0];
  const origin = (req.headers.origin as string) || (req.headers['sec-websocket-origin'] as string);

  // Helper to send JSON responses with proper CORS headers across both Vercel & Node runtimes
  const sendJson = (statusCode: number, data: any) => {
    if (origin && isOriginAllowed(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Content-Type', 'application/json');
    if (typeof res.status === 'function' && typeof res.json === 'function') {
      return res.status(statusCode).json(data);
    } else {
      res.statusCode = statusCode;
      return res.end(JSON.stringify(data));
    }
  };

  // Handle CORS preflight for all endpoints routed through here
  if (req.method === 'OPTIONS') {
    if (origin && isOriginAllowed(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (typeof res.status === 'function') {
      return res.status(204).end();
    } else {
      res.statusCode = 204;
      return res.end();
    }
  }

  // Handle direct health check endpoint
  if (req.method === 'GET' && (pathname === '/health' || pathname === '/api/health' || matchedPath === '/health' || matchedPath === '/api/health')) {
    const isProd = process.env.NODE_ENV === 'production';
    const redisConfigured = distributedState.isConfigured();
    const redisConnected = distributedState.isDistributed();
    const status = (isProd && !redisConnected) ? 'degraded' : 'ok';

    return sendJson(200, {
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
  }

  // Handle direct root status endpoint
  if (req.method === 'GET' && (pathname === '/' || matchedPath === '/')) {
    return sendJson(200, {
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
  }

  // Handle plain HTTP request to /ws without WebSocket upgrade
  const upgradeHeader = Array.isArray(req.headers.upgrade) ? req.headers.upgrade[0] : req.headers.upgrade;
  const isWebSocketUpgrade = upgradeHeader && upgradeHeader.toLowerCase() === 'websocket';

  if (!isWebSocketUpgrade && (pathname === '/ws' || matchedPath === '/ws')) {
    if (res.writeHead) {
      res.writeHead(426, {
        'Content-Type': 'text/plain',
        Upgrade: 'WebSocket',
      });
      return res.end('Upgrade Required: Connect via WebSocket (ws:// or wss://)');
    }
    res.statusCode = 426;
    res.setHeader('Content-Type', 'text/plain');
    res.setHeader('Upgrade', 'WebSocket');
    return res.end('Upgrade Required: Connect via WebSocket (ws:// or wss://)');
  }

  // Handle WebSocket upgrade request on Vercel Fluid Compute
  if (isWebSocketUpgrade) {
    if (origin && !isOriginAllowed(origin)) {
      if (res.writeHead) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        return res.end('Origin not allowed');
      }
      res.statusCode = 403;
      return res.end('Origin not allowed');
    }

    const socket = req.socket || res.socket;
    if (!socket) {
      if (res.writeHead) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        return res.end('Socket connection unavailable');
      }
      res.statusCode = 500;
      return res.end('Socket connection unavailable');
    }

    const server = getWebSocketServer();
    const head = Buffer.alloc(0);
    server.handleUpgrade(req, socket, head, (ws) => {
      server.emit('connection', ws, req);
    });
    return;
  }

  // Handle standard HTTP requests via Express
  return app(req, res);
}
