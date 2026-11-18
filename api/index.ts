import { createOlogleApp, isOriginAllowed } from '../src/app.ts';
import { WebSocketServer, WebSocket } from 'ws';
import { ologleEngine } from '../src/signaling/serverCore.ts';

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
  }
  return wss;
}

export default function handler(req: any, res: any) {
  // Normalize original request URL when routed through Vercel rewrites
  const matchedPath = req.headers['x-matched-path'] as string | undefined;
  if (matchedPath && req.url?.startsWith('/api/index')) {
    req.url = matchedPath;
  }

  // Handle direct health check endpoint
  if (req.method === 'GET' && (req.url === '/health' || matchedPath === '/health')) {
    res.setHeader('Content-Type', 'application/json');
    return res.status(200).json({
      status: 'ok',
      service: 'ologle-signaling',
      uptime: process.uptime(),
      timestamp: Date.now(),
      activeSessions: ologleEngine.getActiveSessionsCount(),
      queuedUsers: ologleEngine.getQueuedUsersCount(),
    });
  }

  // Handle direct root status endpoint
  if (req.method === 'GET' && (req.url === '/' || matchedPath === '/')) {
    res.setHeader('Content-Type', 'application/json');
    return res.status(200).json({
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

  // Handle WebSocket upgrade request on Vercel Fluid Compute
  if (req.headers.upgrade && req.headers.upgrade.toLowerCase() === 'websocket') {
    const origin = req.headers.origin;
    if (origin && !isOriginAllowed(origin)) {
      if (res.writeHead) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        res.end('Origin not allowed');
      }
      return;
    }

    const server = getWebSocketServer();
    const socket = req.socket;
    const head = Buffer.alloc(0);
    server.handleUpgrade(req, socket, head, (ws) => {
      server.emit('connection', ws, req);
    });
    return;
  }

  // Handle standard HTTP requests
  return app(req, res);
}
