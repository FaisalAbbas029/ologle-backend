import { createServer } from 'http';
import { createOlogleApp, attachWebSocketServer } from './app.js';
import { ologleEngine } from './signaling/serverCore.js';

const PORT = Number(process.env.PORT) || 3000;
const app = createOlogleApp();
const server = createServer(app);

// Disable Node HTTP request/socket timeouts so upgraded WebSocket connections
// remain active indefinitely without any 5-minute or 15-minute ceiling.
server.timeout = 0;
server.requestTimeout = 0;
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;

// Attach the WebSocket protocol handler on /ws
const wss = attachWebSocketServer(server);

function gracefulShutdown(signal: string) {
  console.log(`Received ${signal}. Shutting down OLOGLE backend gracefully...`);

  // Stop accepting new WebSocket connections
  wss.close(() => {
    console.log('WebSocket server closed.');
  });

  // Broadcast shutdown notice to active sessions and terminate cleanly
  ologleEngine.broadcastShutdown('maintenance');

  // Close HTTP server and terminate connections
  if (typeof (server as any).closeAllConnections === 'function') {
    (server as any).closeAllConnections();
  }
  server.close(() => {
    console.log('HTTP server closed. Exiting process.');
    process.exit(0);
  });

  // Force close after 10s if connections refuse to terminate
  setTimeout(() => {
    console.error('Graceful shutdown timed out. Forcing process exit.');
    process.exit(1);
  }, 10000).unref();
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

server.on('error', (err: any) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Port ${PORT} is already in use.`);
  } else {
    console.error('Server error:', err);
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`OLOGLE Real-Time Backend running on http://0.0.0.0:${PORT}`);
  console.log(`WebSocket endpoint active on ws://0.0.0.0:${PORT}/ws`);

  // Optional Keep-Alive self-ping (for free-tier hosting platforms like Render)
  if (process.env.RENDER === 'true' || process.env.NODE_ENV === 'production') {
    const selfUrl = process.env.RENDER_EXTERNAL_URL
      ? `${process.env.RENDER_EXTERNAL_URL}/health`
      : `http://localhost:${PORT}/health`;

    const PING_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes
    setInterval(() => {
      const protocolModule = selfUrl.startsWith('https:') ? 'https' : 'http';
      import(protocolModule).then(({ default: client }) => {
        const req = client.get(selfUrl, (res: any) => {
          res.resume();
          console.log(`[keep-alive] Self-ping status: ${res.statusCode}`);
        });
        req.on('error', (err: any) => console.warn('[keep-alive] Self-ping failed:', err.message));
        req.end();
      }).catch(() => {});
    }, PING_INTERVAL_MS).unref();
  }
});

export { server, app };

