import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, Server as HttpServer } from 'http';
import { WebSocket } from 'ws';
import { createOlogleApp, attachWebSocketServer } from '../src/app.js';
import { OlogleEngine } from '../src/signaling/serverCore.js';
import { validateProfession, validateInterests } from '../src/validation/index.js';
import { defaultSafetyAgent } from '../src/services/safetyAgent.js';

describe('Security & Abuse Prevention Suite', () => {
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

  it('validates professions strictly against length, script injections and occupational dictionary', () => {
    // Valid professions
    expect(validateProfession('Software Engineer').isValid).toBe(true);
    expect(validateProfession('Data Scientist').isValid).toBe(true);
    expect(validateProfession('Cardiologist').isValid).toBe(true);

    // Invalid: Script / code injection
    expect(validateProfession('<script>alert("xss")</script>').isValid).toBe(false);
    expect(validateProfession('Software Engineer ${eval()}').isValid).toBe(false);

    // Invalid: Too short / too long
    expect(validateProfession('A').isValid).toBe(false);
    expect(validateProfession('X'.repeat(65)).isValid).toBe(false);

    // Invalid: Conversational phrase
    expect(validateProfession('hello how are you').isValid).toBe(false);
    expect(validateProfession('wanna talk').isValid).toBe(false);
  });

  it('validates interests array strictly (3-15 items, trims control chars, max 40 chars)', () => {
    // Valid: 3 clean interests
    const valid = validateInterests(['AI', 'Machine Learning', 'Programming']);
    expect(valid.isValid).toBe(true);
    expect(valid.sanitized).toEqual(['AI', 'Machine Learning', 'Programming']);

    // Invalid: Less than 3
    expect(validateInterests(['AI', 'Web']).isValid).toBe(false);

    // Invalid: More than 15
    const tooMany = Array.from({ length: 16 }, (_, i) => `Interest ${i}`);
    expect(validateInterests(tooMany).isValid).toBe(false);

    // Sanitization: Strips control chars and enforces 40 chars
    const dirty = validateInterests([
      'Clean Interest 1',
      'Clean Interest 2',
      'Interest with \x00\x1F control characters',
      'A'.repeat(50),
    ]);
    expect(dirty.isValid).toBe(true);
    expect(dirty.sanitized[2]).toBe('Interest with  control characters');
    expect(dirty.sanitized[3].length).toBe(40);
  });

  it('blocks severe harassment and threats via RuleBasedSafetyAgent', () => {
    // Direct physical threats -> High severity block
    const threatResult = defaultSafetyAgent.classifyMessage('I will kill you');
    expect(threatResult.isSafe).toBe(false);
    expect(threatResult.severityTier).toBe('high');
    expect(threatResult.category).toBe('threats_violence');

    // Hate speech slur -> High severity block
    const slurResult = defaultSafetyAgent.classifyMessage('faggot');
    expect(slurResult.isSafe).toBe(false);
    expect(slurResult.severityTier).toBe('high');
    expect(slurResult.category).toBe('hate_speech');

    // Normal safe message
    const safeResult = defaultSafetyAgent.classifyMessage('Hello! Excited to chat about TypeScript and React.');
    expect(safeResult.isSafe).toBe(true);
    expect(safeResult.severityTier).toBe('safe');
  });

  it('enforces maximum 2,000 character limit on 1:1 messages', () => {
    const engine = new OlogleEngine();
    const sentA: any[] = [];
    const socketA = {
      readyState: 1,
      send: (str: string) => sentA.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'vis-len-a');
    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = 'vis-len-b';

    // Send message exceeding 2000 chars
    const oversized = 'A'.repeat(2500);
    engine.processMessage(
      sessionA.sessionId,
      JSON.stringify({
        type: 'SEND_1ON1_MESSAGE',
        text: oversized,
      })
    );

    const errorMsg = sentA.find((m) => m.type === 'ERROR');
    expect(errorMsg).toBeDefined();
    expect(errorMsg?.message).toContain('exceeds maximum allowable length');
  });

  it('issues WARNING packet when per-socket frame rate limit is exceeded', () => {
    const engine = new OlogleEngine();
    const sent: any[] = [];
    let isClosed = false;

    const socket = {
      readyState: 1,
      send: (str: string) => sent.push(JSON.parse(str)),
      close: () => {
        isClosed = true;
      },
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(socket as any, undefined, '10.0.0.1', 'vis-rate');

    // Send 26 messages rapidly (exceeding 25 per 5s limit)
    for (let i = 0; i < 27; i++) {
      engine.processMessage(session.sessionId, JSON.stringify({ type: 'TYPING_INDICATOR', isTyping: true }));
    }

    const warning = sent.find((m) => m.type === 'WARNING');
    expect(warning).toBeDefined();
    expect(warning?.message).toContain('Message rate too high');

    // Continued flooding terminates socket
    for (let i = 0; i < 10; i++) {
      engine.processMessage(session.sessionId, JSON.stringify({ type: 'TYPING_INDICATOR', isTyping: true }));
    }
    expect(isClosed).toBe(true);
  });
});

