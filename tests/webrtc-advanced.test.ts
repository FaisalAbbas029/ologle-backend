import { describe, it, expect } from 'vitest';
import { OlogleEngine } from '../src/signaling/serverCore.ts';
import { createIceServerConfig } from '../src/services/iceServers.ts';

describe('WebRTC Advanced Signaling, Trickle ICE & Glare Prevention', () => {
  it('relays SDP offer, SDP answer, and trickle ICE candidates between paired peers', () => {
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

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'webrtc-vis-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'webrtc-vis-b');

    // Matching flow: Both send START_MATCHING
    engine.processMessage(
      sessionA.sessionId,
      JSON.stringify({
        type: 'START_MATCHING',
        profession: 'Software Engineer',
        interests: ['WebRTC', 'Video Streaming', 'Audio'],
        sessionMode: 'video',
      })
    );

    engine.processMessage(
      sessionB.sessionId,
      JSON.stringify({
        type: 'START_MATCHING',
        profession: 'Software Engineer',
        interests: ['WebRTC', 'Video Streaming', 'Audio'],
        sessionMode: 'video',
      })
    );

    // Verify MATCH_FOUND with glare prevention (one initiator, one receiver)
    const matchA = sentA.find((m) => m.type === 'MATCH_FOUND');
    const matchB = sentB.find((m) => m.type === 'MATCH_FOUND');
    expect(matchA).toBeDefined();
    expect(matchB).toBeDefined();
    expect(matchA.roomId).toBe(matchB.roomId);
    expect(matchA.isInitiator !== matchB.isInitiator).toBe(true);

    const initiator = matchA.isInitiator ? sessionA : sessionB;
    const receiver = matchA.isInitiator ? sessionB : sessionA;
    const receiverSent = matchA.isInitiator ? sentB : sentA;
    const initiatorSent = matchA.isInitiator ? sentA : sentB;

    const roomId = matchA.roomId;

    // 1. Initiator sends SDP Offer
    const offerSignal = {
      type: 'offer',
      sdp: { type: 'offer', sdp: 'v=0\r\no=- 12345 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n' },
    };
    engine.processMessage(
      initiator.sessionId,
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId,
        signal: offerSignal,
      })
    );

    const receivedOffer = receiverSent.find(
      (m: any) => m.type === 'WEBRTC_SIGNAL' && m.signal?.type === 'offer'
    );
    expect(receivedOffer).toBeDefined();
    expect(receivedOffer.signal.sdp.sdp).toBe(offerSignal.sdp.sdp);

    // 2. Receiver sends SDP Answer
    const answerSignal = {
      type: 'answer',
      sdp: { type: 'answer', sdp: 'v=0\r\no=- 67890 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n' },
    };
    engine.processMessage(
      receiver.sessionId,
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId,
        signal: answerSignal,
      })
    );

    const receivedAnswer = initiatorSent.find(
      (m: any) => m.type === 'WEBRTC_SIGNAL' && m.signal?.type === 'answer'
    );
    expect(receivedAnswer).toBeDefined();

    // 3. Trickle ICE candidate exchange
    const iceCandidate = {
      type: 'candidate',
      candidate: {
        candidate: 'candidate:1 1 UDP 2130706431 192.168.1.50 50000 typ host',
        sdpMid: '0',
        sdpMLineIndex: 0,
      },
    };
    engine.processMessage(
      initiator.sessionId,
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId,
        signal: iceCandidate,
      })
    );

    const receivedIce = receiverSent.find(
      (m: any) => m.type === 'WEBRTC_SIGNAL' && m.signal?.type === 'candidate'
    );
    expect(receivedIce).toBeDefined();
    expect(receivedIce.signal.candidate.candidate).toContain('192.168.1.50');
  });

  it('silently drops signals targeting stale rooms or departed partners', () => {
    const engine = new OlogleEngine();
    const sentB: any[] = [];
    const socketA = {
      readyState: 1,
      send: () => {},
      close: () => {},
      removeAllListeners: () => {},
    };
    const socketB = {
      readyState: 1,
      send: (str: string) => sentB.push(JSON.parse(str)),
      close: () => {},
      removeAllListeners: () => {},
    };

    const sessionA = engine.registerSession(socketA as any, undefined, '10.0.0.1', 'stale-a');
    const sessionB = engine.registerSession(socketB as any, undefined, '10.0.0.2', 'stale-b');

    // Clear initial SESSION_REGISTERED
    sentB.length = 0;

    sessionA.currentMode = '1on1';
    sessionA.pairedSessionId = sessionB.sessionId;
    sessionA.currentRoomId = 'room_active_123';

    sessionB.currentMode = '1on1';
    sessionB.pairedSessionId = sessionA.sessionId;
    sessionB.currentRoomId = 'room_active_123';

    // Stale signal from past room
    engine.processMessage(
      sessionA.sessionId,
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId: 'room_stale_999',
        signal: { type: 'offer', sdp: 'v=0' },
      })
    );

    expect(sentB.filter((m) => m.type === 'WEBRTC_SIGNAL').length).toBe(0);

    // Partner leaves room
    engine.handleDisconnect(sessionA.sessionId);

    // Signal sent after disconnect
    engine.processMessage(
      sessionB.sessionId,
      JSON.stringify({
        type: 'WEBRTC_SIGNAL',
        roomId: 'room_active_123',
        signal: { type: 'answer', sdp: 'v=0' },
      })
    );

    // Partner A received nothing because room was invalidated
    expect(sentB.filter((m) => m.type === 'WEBRTC_SIGNAL').length).toBe(0);
  });

  it('supports Metered and Coturn TURN environment variable configurations', () => {
    const config = createIceServerConfig({
      METERED_URLS: 'turn:relay.metered.ca:80?transport=udp,turns:relay.metered.ca:443?transport=tcp',
      METERED_SECRET_KEY: 'test-metered-secret',
    });

    expect(config.relayAvailable).toBe(true);
    expect(config.iceServers[1].urls).toEqual([
      'turn:relay.metered.ca:80?transport=udp',
      'turns:relay.metered.ca:443?transport=tcp',
    ]);
    expect(config.iceServers[1].username).toBeDefined();
    expect(config.iceServers[1].credential).toBeDefined();
  });
});
