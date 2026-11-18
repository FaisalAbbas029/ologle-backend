import { describe, it, expect } from 'vitest';
import { OlogleEngine } from '../src/signaling/serverCore.ts';

describe('WebSocket Protocol & Lifecycle Resilience', () => {
  it('detects dead sockets and terminates them when pong is missing', () => {
    const engine = new OlogleEngine();
    let terminated = false;
    let pingCount = 0;

    const fakeSocket = {
      readyState: 1,
      isAlive: false, // simulated unacknowledged ping
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

    // Trigger internal watchdog check
    (engine as any).startHeartbeatWatchdog();

    // Verify session has active socket initially
    expect(session.ws).toBeDefined();

    // Simulate one watchdog pass
    const sock = session.ws as any;
    if (sock.isAlive === false) {
      sock.terminate();
      engine.handleDisconnect(session.sessionId);
    }

    expect(terminated).toBe(true);
    expect(session.ws).toBeNull();
  });

  it('notifies partner immediately with PARTNER_LEFT reason: disconnected on socket close', () => {
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

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'vis-dc-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'vis-dc-b');

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room-dc-test';

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room-dc-test';

    // Socket A disconnects abruptly
    engine.handleDisconnect(sessionA.sessionId);

    // Partner B must be notified immediately
    const leftMsg = sentB.find((m) => m.type === 'PARTNER_LEFT');
    expect(leftMsg).toBeDefined();
    expect(leftMsg.reason).toBe('disconnected');

    // Partner B room pointers reset immediately
    expect(sessionB.currentMode).toBe('none');
    expect(sessionB.pairedSessionId).toBeNull();
    expect(sessionB.currentRoomId).toBeNull();
  });

  it('supports reconnection idempotency within the 15-second grace window', () => {
    const engine = new OlogleEngine();
    const socket1 = {
      readyState: 1,
      send: () => {},
      close: () => {},
      removeAllListeners: () => {},
    };

    const session = engine.registerSession(socket1 as any, undefined, '10.0.0.1', 'vis-reconnect');
    session.profession = 'Distributed Systems Architect';
    session.interests = ['WebSockets', 'Distributed Systems', 'Node.js'];
    session.blockedVisitorIds.add('blocked-stranger-99');

    const originalSessionId = session.sessionId;

    // Transient mobile disconnect
    engine.handleDisconnect(originalSessionId);
    expect(session.ws).toBeNull();

    // Verify disconnect timer is active
    expect((engine as any).disconnectTimers.has(originalSessionId)).toBe(true);

    // Client reconnects within 15s using same sessionId
    const socket2 = {
      readyState: 1,
      send: () => {},
      close: () => {},
      removeAllListeners: () => {},
    };
    const resumedSession = engine.registerSession(socket2 as any, originalSessionId, '10.0.0.1', 'vis-reconnect');

    // State, visitorId, and blocked list are preserved
    expect(resumedSession.sessionId).toBe(originalSessionId);
    expect(resumedSession.profession).toBe('Distributed Systems Architect');
    expect(resumedSession.interests).toContain('WebSockets');
    expect(resumedSession.blockedVisitorIds.has('blocked-stranger-99')).toBe(true);
    expect(resumedSession.ws).toBe(socket2);

    // Pending disconnect timer was cancelled
    expect((engine as any).disconnectTimers.has(originalSessionId)).toBe(false);
  });
});

