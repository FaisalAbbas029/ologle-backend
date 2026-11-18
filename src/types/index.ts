/**
 * OLOGLE Backend — Ephemeral Shared Types
 * Strictly zero-persistence definitions for real-time WebRTC, signaling, and in-memory rooms.
 */

export interface ScoreBreakdown {
  sharedInterests: string[];
  sharedTopics: string[];
  professionBonus: boolean;
  countryBonus: boolean;
  totalScore: number;
}

export interface EphemeralLiveTicket {
  ticketId: string;
  roomId: string;
  sessionHash: string;
  reason: string;
  category: string;
  confidence: number;
  flaggedAt: number;
  expiresAt: number;
  status: 'PENDING_REVIEW' | 'INSPECTING' | 'RESOLVED';
}

export interface AIPracticePartner {
  id: string;
  name: string;
  initials: string;
  avatar: string;
  profession: string;
  bio: string;
  interests: string[];
  style: string;
  country: string;
  accentColor: string;
}

export interface WebRtcCandidatePayload {
  candidate: string;
  sdpMid?: string | null;
  sdpMLineIndex?: number | null;
  usernameFragment?: string | null;
}

export interface WebRtcSignalPayload {
  type?: 'offer' | 'answer' | 'candidate';
  sdp?: any;
  candidate?: WebRtcCandidatePayload;
}

export interface WebRtcSignalMessage {
  type: 'WEBRTC_SIGNAL';
  roomId?: string;
  signal: WebRtcSignalPayload;
}

