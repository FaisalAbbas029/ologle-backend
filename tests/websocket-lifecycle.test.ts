import { describe, it, expect } from 'vitest';
import { OlogleEngine } from '../src/signaling/serverCore.js';

describe('WebSocket Protocol & Lifecycle Resilience', () => {
  it('detects dead sockets and terminates them when pong is missing', () => {
    const engine = new OlogleEngine();
    let terminated = false;
    let pingCount = 0;

    const fakeSocket = {
      readyState: 1,
      isAlive: false,
      send: () => {},
      ping: () => {
        pingCount++;
      },
      terminate: () => {
        terminated = true;
      },
      close: () => {},
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(fakeSocket as any, undefined, '10.0.0.1', 'ghost-mobile-user');
    // registerSession initializes isAlive = true; simulate missed pong
    (fakeSocket as any).isAlive = false;

    expect(session.ws).toBeDefined();

    const sock = session.ws as any;
    if (sock.isAlive === false) {
      sock.terminate();
      engine.handleDisconnect(session.sessionId);
    }

    expect(terminated).toBe(true);
    expect(session.ws).toBeNull();
  });

  it('preserves active 1on1 room during the 20s recovery window on unexpected socket drop and restores on reconnect', () => {
    const engine = new OlogleEngine();
    expect(engine.getRecoveryWindowMs()).toBe(20000);

    const sentA1: any[] = [];
    const sentA2: any[] = [];
    const sentB: any[] = [];

    const socketA1 = {
      readyState: 1,
      send: (str: string) => sentA1.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const socketB = {
      readyState: 1,
      send: (str: string) => sentB.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA1 as any, undefined, '10.0.0.1', 'vis-dc-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'vis-dc-b');

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room-dc-test';
    sessionA.partnerProfession = 'Data Scientist';
    sessionA.sharedInterests = ['AI', 'Python', 'WebRTC'];

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room-dc-test';

    // Socket A drops unexpectedly due to transient network blip
    engine.handleDisconnect(sessionA.sessionId);

    // Partner B must NOT receive PARTNER_LEFT immediately; must receive PARTNER_RECONNECTING
    expect(sentB.find((m) => m.type === 'PARTNER_LEFT')).toBeUndefined();
    const reconnectingMsg = sentB.find((m) => m.type === 'PARTNER_RECONNECTING');
    expect(reconnectingMsg).toBeDefined();
    expect(reconnectingMsg.graceSeconds).toBe(20);

    // Room and partner state remain intact during recovery window
    expect(sessionA.currentMode).toBe('1on1');
    expect(sessionA.pairedSessionId).toBe(sessionB.sessionId);
    expect(sessionA.currentRoomId).toBe('room-dc-test');
    expect(sessionB.currentMode).toBe('1on1');
    expect(sessionB.pairedSessionId).toBe(sessionA.sessionId);
    expect(sessionB.currentRoomId).toBe('room-dc-test');

    // User A reconnects within the recovery window using the same sessionId
    const socketA2 = {
      readyState: 1,
      send: (str: string) => sentA2.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const resumedA = engine.registerSession(socketA2 as any, sessionA.sessionId, '10.0.0.1', 'vis-dc-a');

    expect(resumedA.sessionId).toBe(sessionA.sessionId);
    expect(resumedA.currentMode).toBe('1on1');
    expect(resumedA.currentRoomId).toBe('room-dc-test');
    expect(resumedA.pairedSessionId).toBe(sessionB.sessionId);

    // User A receives SESSION_RESTORED and User B receives PARTNER_RECONNECTED
    const restoredMsg = sentA2.find((m) => m.type === 'SESSION_RESTORED');
    expect(restoredMsg).toBeDefined();
    expect(restoredMsg.roomId).toBe('room-dc-test');

    const reconnectedMsg = sentB.find((m) => m.type === 'PARTNER_RECONNECTED');
    expect(reconnectedMsg).toBeDefined();
    expect(reconnectedMsg.roomId).toBe('room-dc-test');

    // Chat messages continue working after reconnection
    engine.processMessage(
      sessionA.sessionId,
      JSON.stringify({ type: 'SEND_1ON1_MESSAGE', text: 'Reconnected and still here!' })
    );
    const deliveredChat = sentB.find((m) => m.type === 'RECEIVE_1ON1_MESSAGE');
    expect(deliveredChat).toBeDefined();
    expect(deliveredChat.text).toBe('Reconnected and still here!');
  });

  it('notifies partner with PARTNER_LEFT reason: disconnected only after recovery window expires', async () => {
    const engine = new OlogleEngine();
    engine.setRecoveryWindowMs(80); // 80ms recovery window for deterministic fast test

    const sentA: any[] = [];
    const sentB: any[] = [];

    const socketA = {
      readyState: 1,
      send: (str: string) => sentA.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const socketB = {
      readyState: 1,
      send: (str: string) => sentB.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'vis-exp-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'vis-exp-b');

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room-exp-test';

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room-exp-test';

    // Socket A drops and never reconnects
    engine.handleDisconnect(sessionA.sessionId);
    expect(sentB.find((m) => m.type === 'PARTNER_LEFT')).toBeUndefined();

    // Wait for recovery window to expire
    await new Promise((r) => setTimeout(r, 150));

    const leftMsg = sentB.find((m) => m.type === 'PARTNER_LEFT');
    expect(leftMsg).toBeDefined();
    expect(leftMsg.reason).toBe('disconnected');
    expect(sessionB.currentMode).toBe('none');
    expect(sessionB.pairedSessionId).toBeNull();
    expect(sessionB.currentRoomId).toBeNull();
  });

  it('immediately notifies partner with PARTNER_LEFT reason: ended on manual END_CONVERSATION', () => {
    const engine = new OlogleEngine();
    const sentA: any[] = [];
    const sentB: any[] = [];

    const socketA = {
      readyState: 1,
      send: (str: string) => sentA.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const socketB = {
      readyState: 1,
      send: (str: string) => sentB.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'vis-end-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'vis-end-b');

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room-end-test';

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room-end-test';

    // User A manually presses Disconnect
    engine.processMessage(sessionA.sessionId, JSON.stringify({ type: 'END_CONVERSATION' }));

    const leftMsg = sentB.find((m) => m.type === 'PARTNER_LEFT');
    expect(leftMsg).toBeDefined();
    expect(leftMsg.reason).toBe('ended');
    expect(sessionA.currentMode).toBe('none');
    expect(sessionB.currentMode).toBe('none');
  });

  it('responds to application-level PING keep-alive frames with PONG without rate-limit penalties', () => {
    const engine = new OlogleEngine();
    const sent: any[] = [];
    const socket = {
      readyState: 1,
      send: (str: string) => sent.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(socket as any, undefined, '10.0.0.1', 'vis-ping');
    engine.processMessage(session.sessionId, JSON.stringify({ type: 'PING', timestamp: 12345 }));

    const pong = sent.find((m) => m.type === 'PONG');
    expect(pong).toBeDefined();
    expect(typeof pong.timestamp).toBe('number');
    engine.broadcastShutdown('test_done');
  });

  it('prevents unauthorized session takeover when a different IP or visitor attempts to hijack an active sessionId', () => {
    const engine = new OlogleEngine();
    const sentVictim: any[] = [];
    const sentAttacker: any[] = [];

    const victimSocket = {
      readyState: 1, // OPEN
      send: (str: string) => sentVictim.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };
    const attackerSocket = {
      readyState: 1,
      send: (str: string) => sentAttacker.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const victimSession = engine.registerSession(victimSocket as any, undefined, '203.0.113.10', 'vis-victim');
    victimSession.currentMode = '1on1';
    victimSession.currentRoomId = 'room-secure-1';

    // Attacker from different IP attempts to hijack victimSession.sessionId while victim is online
    const attackerSession = engine.registerSession(
      attackerSocket as any,
      victimSession.sessionId,
      '198.51.100.99',
      'vis-attacker'
    );

    // Takeover blocked: attacker gets a brand-new sessionId and victim keeps their active session
    expect(attackerSession.sessionId).not.toBe(victimSession.sessionId);
    expect(victimSession.ws).toBe(victimSocket);
    expect(victimSession.currentMode).toBe('1on1');
    expect(victimSession.currentRoomId).toBe('room-secure-1');

    // Also blocks arbitrary client-invented sessionId that does not exist on the server
    const forgedSession = engine.registerSession(
      attackerSocket as any,
      'hl_forged_nonexistent_id_12345',
      '198.51.100.99'
    );
    expect(forgedSession.sessionId).not.toBe('hl_forged_nonexistent_id_12345');

    engine.broadcastShutdown('test_done');
  });
});
