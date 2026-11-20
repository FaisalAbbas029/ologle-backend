import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';
import { OlogleEngine } from '../src/signaling/serverCore.js';
import { createOlogleApp } from '../src/app.js';

describe('Privacy-by-Design & Zero Disk Persistence Verification', () => {
  it('strictly prohibits disk write APIs (fs.writeFile, appendFile, createWriteStream) across src/', () => {
    const srcDir = path.resolve(__dirname, '../src');

    function scanFiles(dir: string): string[] {
      let results: string[] = [];
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          results = results.concat(scanFiles(fullPath));
        } else if (entry.isFile() && fullPath.endsWith('.ts')) {
          results.push(fullPath);
        }
      }
      return results;
    }

    const files = scanFiles(srcDir);
    expect(files.length).toBeGreaterThan(0);

    const forbiddenPatterns = [
      /fs\.writeFile/,
      /fs\.appendFile/,
      /fs\.createWriteStream/,
      /require\(['"]sqlite/,
      /require\(['"]mongodb/,
      /require\(['"]pg/,
      /require\(['"]level/,
    ];

    for (const filePath of files) {
      const content = fs.readFileSync(filePath, 'utf-8');
      for (const pattern of forbiddenPatterns) {
        expect(content).not.toMatch(pattern);
      }
    }
  });

  it('relays 1:1 text messages in volatile RAM without storing chat history', () => {
    const engine = new OlogleEngine();
    const receivedA: any[] = [];
    const receivedB: any[] = [];

    const socketA = {
      readyState: 1,
      send: (str: string) => receivedA.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const socketB = {
      readyState: 1,
      send: (str: string) => receivedB.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'visitor-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'visitor-b');

    // Pair sessions into 1:1 mode
    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room-privacy-test';

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room-privacy-test';

    // Session A sends a private ephemeral message
    const secretMessage = 'Ephemeral-Token-XYZ-Never-Persist';
    engine.processMessage(
      sessionA.sessionId,
      JSON.stringify({
        type: 'SEND_1ON1_MESSAGE',
        text: secretMessage,
      })
    );

    // Verify message arrived at partner socket in RAM
    const receivedMsg = receivedB.find((m) => m.type === 'RECEIVE_1ON1_MESSAGE');
    expect(receivedMsg).toBeDefined();
    expect(receivedMsg?.text).toBe(secretMessage);

    // Verify engine has ZERO message history or message store properties
    expect((engine as any).messages).toBeUndefined();
    expect((engine as any).chatHistory).toBeUndefined();
    expect((engine as any).messageStore).toBeUndefined();
    expect((sessionA as any).messages).toBeUndefined();
    expect((sessionB as any).messages).toBeUndefined();
  });

  it('scrubs all session memory and sets upon destroySession', () => {
    const engine = new OlogleEngine();
    const socket = {
      readyState: 1,
      send: () => {},
      close: () => {},
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(socket as any, undefined, '192.168.1.100', 'visitor-scrub');
    session.blockedSessionIds.add('blocked-sess-1');
    session.blockedVisitorIds.add('blocked-vis-1');

    expect(session.blockedSessionIds.size).toBe(1);
    expect(session.blockedVisitorIds.size).toBe(1);

    // Destroy session
    engine.destroySession(session.sessionId);

    // Verifies session is completely purged from engine
    expect(engine.getActiveSessionsCount()).toBe(0);
    // Verifies sets are cleared
    expect(session.blockedSessionIds.size).toBe(0);
    expect(session.blockedVisitorIds.size).toBe(0);
    expect(session.ws).toBeNull();
  });

  it('hashes client IP with rotating salt and never retains raw IP addresses', () => {
    const engine = new OlogleEngine();
    const rawIp = '203.0.113.195';
    const socket = {
      readyState: 1,
      send: () => {},
      close: () => {},
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(socket as any, undefined, rawIp, 'vis-hash');

    // Hashed IP is 16-character hex
    expect(session.ipAddress).not.toBe(rawIp);
    expect(session.ipAddress).toHaveLength(16);
    expect(session.ipAddress).toMatch(/^[0-9a-f]{16}$/);
  });

  it('health endpoint exposes only aggregate telemetry without personal identifiers', async () => {
    const app = createOlogleApp();
    const req = {} as any;
    let jsonResult: any = null;

    const res = {
      json: (data: any) => {
        jsonResult = data;
      },
      setHeader: () => res,
      status: () => res,
    } as any;

    // Simulate GET /health handler
    const healthRoutes = (app as any)._router.stack.filter(
      (layer: any) => layer.route && layer.route.path === '/health'
    );
    expect(healthRoutes.length).toBe(1);

    healthRoutes[0].route.stack[0].handle(req, res);

    expect(jsonResult).toBeDefined();
    expect(jsonResult.status).toBe('ok');
    expect(typeof jsonResult.activeSessions).toBe('number');
    expect(typeof jsonResult.queuedUsers).toBe('number');
    expect(typeof jsonResult.uptime).toBe('number');

    // Must NEVER contain user or IP data
    expect(jsonResult.ip).toBeUndefined();
    expect(jsonResult.users).toBeUndefined();
    expect(jsonResult.sessions).toBeUndefined();
    expect(jsonResult.visitorId).toBeUndefined();
  });
});

