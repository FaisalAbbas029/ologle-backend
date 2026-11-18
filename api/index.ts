import { createOlogleApp, isOriginAllowed } from '../src/app.ts';
import { WebSocketServer } from 'ws';
import { ologleEngine } from '../src/signaling/serverCore.ts';

const app = createOlogleApp();

let wss: WebSocketServer | null = null;

function getWebSocketServer(): WebSocketServer {
  if (!wss) {
    wss = new WebSocketServer({ noServer: true });
    wss.on('connection', (ws, req) => {
      const origin = req.headers.origin;
      if (origin && !isOriginAllowed(origin)) {
        ws.close(1008, 'Origin not allowed');
        return;
      }

      const forwarded = req.headers['x-forwarded-for'];
      const ip = typeof forwarded === 'string'
        ? forwarded.split(',')[0].trim()
        : req.socket.remoteAddress || '127.0.0.1';

      const session = ologleEngine.registerSession(ws as any, undefined, ip);

      ws.on('message', (data) => {
        ologleEngine.processMessage(session.sessionId, data.toString());
      });

      ws.on('close', () => {
        ologleEngine.handleDisconnect(session.sessionId);
      });

      ws.on('error', () => {
        ologleEngine.handleDisconnect(session.sessionId);
      });
    });
  }
  return wss;
}

export default function handler(req: any, res: any) {
  // Handle WebSocket upgrade request on Vercel Fluid Compute
  if (req.headers.upgrade && req.headers.upgrade.toLowerCase() === 'websocket') {
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

