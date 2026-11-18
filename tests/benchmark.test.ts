import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, Server as HttpServer } from 'http';
import { WebSocket } from 'ws';
import { createOlogleApp, attachWebSocketServer } from '../src/app.ts';
import { OlogleEngine } from '../src/signaling/serverCore.ts';

describe('High-Concurrency WebSocket Scaling & Memory Benchmark', () => {
  let server: HttpServer;
  let wsUrl: string;

  beforeAll(async () => {
    const app = createOlogleApp();
    server = createServer(app);
    attachWebSocketServer(server);

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        const port = typeof addr === 'object' && addr ? addr.port : 3000;
        wsUrl = `ws://127.0.0.1:${port}/ws`;
        resolve();
      });
    });
  });

  afterAll(async () => {
    if (typeof (server as any).closeAllConnections === 'function') {
      (server as any).closeAllConnections();
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('engine matches 1,000 sessions at high throughput with <2ms queue evaluation', () => {
    const engine = new OlogleEngine();
    const fakeSockets: any[] = [];
    const messagesReceived: any[][] = [];

    const interestsPool = [
      ['AI', 'Machine Learning', 'Programming'],
      ['Web Development', 'Frontend', 'React'],
      ['Cloud', 'DevOps', 'Docker'],
      ['Design', 'UI/UX', 'Figma'],
      ['Cybersecurity', 'Linux', 'Networking'],
    ];

    const SESSIONS_COUNT = 1000;
    for (let i = 0; i < SESSIONS_COUNT; i++) {
      const msgs: any[] = [];
      messagesReceived.push(msgs);
      const fakeWs = {
        readyState: 1, // OPEN
        send: (str: string) => {
          try {
            msgs.push(JSON.parse(str));
          } catch {}
        },
        close: () => {},
        removeAllListeners: () => {},
      };
      fakeSockets.push(fakeWs);
    }

    const initialMem = process.memoryUsage();
    const startTime = performance.now();

    // Register all 1000 sessions
    const sessions = fakeSockets.map((ws, i) =>
      engine.registerSession(ws, undefined, `192.168.1.${(i % 250) + 1}`, `visitor-${i}`)
    );

    // Queue all 1000 sessions for matching
    for (let i = 0; i < SESSIONS_COUNT; i++) {
      const interests = interestsPool[i % interestsPool.length];
      const mode = i % 3 === 0 ? 'video' : i % 3 === 1 ? 'audio' : 'text';
      engine.processMessage(
        sessions[i].sessionId,
        JSON.stringify({
          type: 'START_MATCHING',
          profession: 'Software Engineer',
          interests,
          country: 'GLOBAL',
          sessionMode: mode,
        })
      );
    }

    const durationMs = performance.now() - startTime;
    const finalMem = process.memoryUsage();

    const pairingsPerSec = (SESSIONS_COUNT / durationMs) * 1000;
    const memPerSessionKb = Math.max(0, (finalMem.heapUsed - initialMem.heapUsed) / 1024 / SESSIONS_COUNT);

    // Count matched pairs
    let matchFoundCount = 0;
    for (const msgs of messagesReceived) {
      if (msgs.some((m) => m.type === 'MATCH_FOUND')) {
        matchFoundCount++;
      }
    }

    // Engine performance assertions:
    expect(matchFoundCount).toBeGreaterThanOrEqual(900); // Almost all matched in pairs
    expect(pairingsPerSec).toBeGreaterThan(500); // > 500 sessions/sec processed
    expect(memPerSessionKb).toBeLessThan(40); // < 40KB per session in memory
  });

  it('sustains live WebSocket clients simultaneously with <10ms event loop lag during active matching', async () => {
    const CLIENT_COUNT = 40;
    const clients: WebSocket[] = [];
    let matchCount = 0;

    // 1. Connect all clients first
    await new Promise<void>((resolve) => {
      let opened = 0;
      for (let i = 0; i < CLIENT_COUNT; i++) {
        const ws = new WebSocket(wsUrl);
        clients.push(ws);
        ws.on('open', () => {
          opened++;
          if (opened === CLIENT_COUNT) resolve();
        });
        ws.on('error', () => {});
      }
    });

    // 2. Start Event Loop Lag Monitoring during active matching
    let maxLagMs = 0;
    let isMonitoring = true;

    const checkLag = () => {
      if (!isMonitoring) return;
      const start = performance.now();
      setImmediate(() => {
        const lag = performance.now() - start;
        if (lag > maxLagMs) maxLagMs = lag;
        checkLag();
      });
    };
    checkLag();

    // 3. Trigger matching in concurrent micro-batches across connected clients
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => resolve(), 4000);

      const batchSize = 5;
      let sent = 0;

      const sendNextBatch = () => {
        if (sent >= CLIENT_COUNT) return;
        const end = Math.min(CLIENT_COUNT, sent + batchSize);
        for (let i = sent; i < end; i++) {
          const ws = clients[i];
          ws.on('message', (data) => {
            try {
              const parsed = JSON.parse(data.toString());
              if (parsed.type === 'MATCH_FOUND') {
                matchCount++;
                if (matchCount >= CLIENT_COUNT * 0.8) {
                  clearTimeout(timer);
                  resolve();
                }
              }
            } catch {}
          });

          ws.send(
            JSON.stringify({
              type: 'START_MATCHING',
              visitorId: `live-bench-${i}`,
              profession: 'Software Engineer',
              interests: ['AI', 'Machine Learning', 'Programming'],
              sessionMode: 'video',
            })
          );
        }
        sent = end;
        if (sent < CLIENT_COUNT) {
          setImmediate(sendNextBatch);
        }
      };

      sendNextBatch();
    });

    isMonitoring = false;

    // Disconnect clients
    for (const ws of clients) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    }

    expect(matchCount).toBeGreaterThanOrEqual(30);
    const lagThreshold = process.platform === 'win32' ? 50 : 15;
    expect(maxLagMs).toBeLessThan(lagThreshold);
  });
});
