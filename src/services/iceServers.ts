import crypto from 'crypto';

const STUN_URLS = [
  'stun:stun.l.google.com:19302',
  'stun:stun1.l.google.com:19302',
  'stun:stun2.l.google.com:19302',
  'stun:stun3.l.google.com:19302',
];

export interface IceServerConfig {
  iceServers: RTCIceServer[];
  relayAvailable: boolean;
  expiresAt: number | null;
}

/**
 * Expands a list of raw TURN URLs to include both UDP and TCP transport variants
 * if no transport is explicitly specified in the query parameters.
 */
function expandTurnTransports(rawUrls: string[]): string[] {
  const result: string[] = [];
  for (const raw of rawUrls) {
    const trimmed = raw.trim();
    if (!trimmed) continue;
    if (trimmed.includes('?transport=')) {
      result.push(trimmed);
    } else {
      // Add both UDP and TCP transports for maximum NAT traversal
      const separator = trimmed.includes('?') ? '&' : '?';
      result.push(`${trimmed}${separator}transport=udp`);
      result.push(`${trimmed}${separator}transport=tcp`);
    }
  }
  return result;
}

export function createIceServerConfig(
  env: NodeJS.ProcessEnv = process.env,
  nowMs = Date.now()
): IceServerConfig {
  const rawTurnUrls = (env.TURN_URLS || env.METERED_URLS || env.COTURN_URLS || '')
    .split(',')
    .map((url) => url.trim())
    .filter(Boolean);

  const sharedSecret = (
    env.TURN_SHARED_SECRET ||
    env.METERED_SECRET_KEY ||
    env.METERED_API_KEY ||
    env.COTURN_SHARED_SECRET ||
    env.TURN_SECRET
  )?.trim();

  const staticUsername = (env.TURN_USERNAME || env.TURN_USER)?.trim();
  const staticCredential = (env.TURN_CREDENTIAL || env.TURN_PASSWORD || env.TURN_PASS)?.trim();

  // If neither TURN URLs nor credentials provided, fallback to STUN
  if (rawTurnUrls.length === 0 && !sharedSecret && !staticUsername) {
    return {
      iceServers: [{ urls: STUN_URLS }],
      relayAvailable: false,
      expiresAt: null,
    };
  }

  // If a shared secret is provided without TURN URLs
  if (sharedSecret && rawTurnUrls.length === 0) {
    throw new Error('TURN_URLS and TURN_SHARED_SECRET must both be configured.');
  }

  // Validate that all URLs use turn: or turns: scheme
  if (rawTurnUrls.some((url) => !/^turns?:[^\s,]+$/i.test(url))) {
    throw new Error('TURN_URLS must contain only valid turn: or turns: URLs.');
  }

  const expandedUrls = expandTurnTransports(rawTurnUrls);

  // Strategy 1: Ephemeral HMAC-SHA1 credentials via shared secret (REST API spec)
  if (sharedSecret) {
    const ttlSeconds = Number(env.TURN_CREDENTIAL_TTL_SECONDS || 600);
    const validTtl = Number.isInteger(ttlSeconds) && ttlSeconds >= 60 && ttlSeconds <= 86400 ? ttlSeconds : 600;
    const expiresAt = Math.floor(nowMs / 1000) + validTtl;
    const username = `${expiresAt}:${crypto.randomBytes(12).toString('hex')}`;
    const credential = crypto.createHmac('sha1', sharedSecret).update(username).digest('base64');

    return {
      iceServers: [
        { urls: STUN_URLS },
        { urls: expandedUrls, username, credential },
      ],
      relayAvailable: true,
      expiresAt,
    };
  }

  // Strategy 2: Static username and password/credential
  if (staticUsername && staticCredential) {
    return {
      iceServers: [
        { urls: STUN_URLS },
        { urls: expandedUrls, username: staticUsername, credential: staticCredential },
      ],
      relayAvailable: true,
      expiresAt: null,
    };
  }

  // If TURN URLs provided without credentials, return STUN + public/unauthenticated TURN
  return {
    iceServers: [
      { urls: STUN_URLS },
      { urls: expandedUrls },
    ],
    relayAvailable: true,
    expiresAt: null,
  };
}

