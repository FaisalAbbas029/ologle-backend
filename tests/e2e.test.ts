import { describe, expect, it, beforeAll, afterAll } from 'vitest';
import { createServer, Server as HttpServer } from 'http';
import { WebSocket } from 'ws';
import { createOlogleApp, attachWebSocketServer } from '../src/app.js';

describe('OLOGLE Backend Full Integration & E2E Signaling', () => {
  let server: HttpServer;
  let serverPort: number;
  let wsUrl: string;
  let httpUrl: string;

  beforeAll(async () => {
    const app = createOlogleApp();
    server = createServer(app);
    attachWebSocketServer(server);

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        const address = server.address();
        if (typeof address === 'object' && address) {
          serverPort = address.port;
          wsUrl = `ws://127.0.0.1:${serverPort}/ws`;
          httpUrl = `http://127.0.0.1:${serverPort}`;
        }
        resolve();
      });
    });
  });

  afterAll(async () => {
    if (typeof (server as any).closeAllConnections === 'function') {
      (server as any).closeAllConnections();
    }
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  });

  it('GET /health returns 200 OK with uptime, status, activeSessions and queuedUsers', async () => {
    const res = await fetch(`${httpUrl}/health`);
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.status).toBe('ok');
    expect(json.service).toBe('ologle-signaling');
    expect(typeof json.uptime).toBe('number');
    expect(typeof json.activeSessions).toBe('number');
    expect(typeof json.queuedUsers).toBe('number');
  });

  it('GET /api/ice-servers returns STUN configuration with caching headers', async () => {
    const res = await fetch(`${httpUrl}/api/ice-servers`);
    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toContain('max-age=300');
    const json = await res.json();
    expect(Array.isArray(json.iceServers)).toBe(true);
    expect(json.iceServers[0].urls).toContain('stun:stun.l.google.com:19302');
  });

  it('establishes WebSocket connection and registers session', async () => {
    const ws = new WebSocket(wsUrl);

    const message = await new Promise<any>((resolve, reject) => {
      ws.on('open', () => {});
      ws.on('message', (data) => {
        resolve(JSON.parse(data.toString()));
      });
      ws.on('error', reject);
    });

    expect(message.type).toBe('SESSION_REGISTERED');
    expect(typeof message.sessionId).toBe('string');
    ws.close();
  });

  it('matches two users, relays WebRTC signaling, exchanges messages, and cleans up on disconnect', async () => {
    const ws1 = new WebSocket(wsUrl);
    const ws2 = new WebSocket(wsUrl);

    const messages1: any[] = [];
    const messages2: any[] = [];

    ws1.on('message', (d) => messages1.push(JSON.parse(d.toString())));
    ws2.on('message', (d) => messages2.push(JSON.parse(d.toString())));

    // Wait for SESSION_REGISTERED on both
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (
          messages1.some((m) => m.type === 'SESSION_REGISTERED') &&
          messages2.some((m) => m.type === 'SESSION_REGISTERED')
        ) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });

    // Both send START_MATCHING with shared interest (requires minimum 3 interests)
    ws1.send(
      JSON.stringify({
        type: 'START_MATCHING',
        profession: 'Software Engineer',
        interests: ['Artificial Intelligence', 'WebRTC', 'Programming'],
        country: 'GLOBAL',
      })
    );

    ws2.send(
      JSON.stringify({
        type: 'START_MATCHING',
        profession: 'AI Researcher',
        interests: ['Artificial Intelligence', 'Cloud', 'Programming'],
        country: 'GLOBAL',
      })
    );

    // Wait for MATCH_FOUND on both
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (
          messages1.some((m) => m.type === 'MATCH_FOUND') &&
          messages2.some((m) => m.type === 'MATCH_FOUND')
        ) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });

    const match1 = messages1.find((m) => m.type === 'MATCH_FOUND');
    const match2 = messages2.find((m) => m.type === 'MATCH_FOUND');

    expect(match1.roomId).toBe(match2.roomId);
    expect(match1.sharedInterests).toContain('Artificial Intelligence');
    // One peer must be initiator, the other not
    expect(match1.isInitiator).not.toBe(match2.isInitiator);

    // Test WebRTC signaling relay (Offer -> Answer -> ICE Candidate)
    const mockOffer = { type: 'offer', sdp: 'v=0\r\no=mockOffer...' };
    ws1.send(
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId: match1.roomId,
        signal: mockOffer,
      })
    );

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (messages2.some((m) => m.type === 'WEBRTC_SIGNAL')) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });

    const receivedOffer = messages2.find((m) => m.type === 'WEBRTC_SIGNAL');
    expect(receivedOffer.signal).toEqual(mockOffer);

    // Test Text Chat Message Relay
    ws2.send(
      JSON.stringify({
        type: 'SEND_1ON1_MESSAGE',
        text: 'Hello from Peer 2!',
      })
    );

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (messages1.some((m) => m.type === 'RECEIVE_1ON1_MESSAGE')) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });

    const chatMsg = messages1.find((m) => m.type === 'RECEIVE_1ON1_MESSAGE');
    expect(chatMsg.text).toBe('Hello from Peer 2!');

    // Test Temporary Disconnect -> Reconnect with ?sessionId=... Preserves Active Chat
    const session1Id = messages1.find((m) => m.type === 'SESSION_REGISTERED').sessionId;
    ws1.close();

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (messages2.some((m) => m.type === 'PARTNER_RECONNECTING')) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });
    expect(messages2.some((m) => m.type === 'PARTNER_LEFT')).toBe(false);

    // Reconnect Peer 1 using existing sessionId
    const ws1Resumed = new WebSocket(`${wsUrl}?sessionId=${encodeURIComponent(session1Id)}`);
    ws1Resumed.on('message', (d) => messages1.push(JSON.parse(d.toString())));

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (
          messages1.some((m) => m.type === 'SESSION_RESTORED') &&
          messages2.some((m) => m.type === 'PARTNER_RECONNECTED')
        ) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });

    // Test Manual Disconnect (END_CONVERSATION) and Partner Notification
    ws1Resumed.send(JSON.stringify({ type: 'END_CONVERSATION' }));

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (messages2.some((m) => m.type === 'PARTNER_LEFT')) {
          clearInterval(check);
          resolve();
        }
      }, 20);
    });

    const leftMsg = messages2.find((m) => m.type === 'PARTNER_LEFT');
    expect(leftMsg.reason).toBe('ended');

    ws1Resumed.close();
    ws2.close();
  });
});

