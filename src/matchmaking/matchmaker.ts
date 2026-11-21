/**
 * OLOGLE Compatibility & Matchmaking Engine
 * Interests are the PRIMARY matching factor with server-authoritative scoring.
 * Profession is secondary optional context only and never penalized if missing.
 */

export interface QueuedSession {
  sessionId: string;
  visitorId?: string;
  ipAddress?: string;
  isLoopback?: boolean;
  profession: string;
  strictProfessionFilter?: boolean; // When true, ONLY match users with the exact same normalized profession
  interests: string[];
  country?: string; // 'GLOBAL' or country code/name
  mode?: string; // 'video' | 'audio' | 'text'
  queuedAt: number;
  blockedSessionIds: Set<string>; // Sessions this user has skipped, reported, or blocked
  blockedVisitorIds?: Set<string>; // Persistent visitor IDs blocked by this user
  matchedSessionId: string | null;
}

export interface MatchScoreResult {
  score: number;
  sharedInterests: string[];
  sharedTopics: string[];
  isSharedProfession: boolean;
  isComplementaryProfession: boolean;
}

// Actual Scoring Weights
export const EXACT_MATCH_WEIGHT = 100;
export const CUMULATIVE_INTEREST_BONUS_WEIGHT = 15; // Additional weight for multiple shared interests
export const RELATED_MATCH_WEIGHT = 20;
export const TOPIC_MATCH_WEIGHT = 40; // Category/topic affinity bonus
export const PROFESSION_BONUS_WEIGHT = 15; // Shared profession bonus
export const COMPLEMENTARY_PROFESSION_BONUS_WEIGHT = 10; // Complementary role bonus
export const COUNTRY_MATCH_WEIGHT = 25; // Bonus when country-wise match requested and matches

/**
 * Practical Synonym & Alias Mapping Table
 * Maps abbreviations, synonyms, and subfields to canonical interest buckets.
 * Note: This acts as a reliable rule-based normalization layer that can later
 * be substituted or complemented with an embedding-based semantic similarity model.
 */
export const INTEREST_SYNONYMS: Record<string, string> = {
  // Artificial Intelligence & Machine Learning
  'ai': 'AI',
  'artificial intelligence': 'AI',
  'genai': 'AI',
  'generative ai': 'AI',
  'llm': 'AI',
  'llms': 'AI',
  'machine learning': 'AI',
  'ml': 'AI',
  'deep learning': 'AI',
  'neural networks': 'AI',
  'gpt': 'AI',
  'rag': 'AI',

  // Programming & Software Engineering
  'programming': 'Programming',
  'coding': 'Programming',
  'software engineering': 'Programming',
  'software development': 'Programming',
  'dev': 'Programming',
  'computer science': 'Programming',
  'cs': 'Programming',
  'algorithms': 'Programming',
  'rust': 'Programming',
  'python': 'Programming',
  'golang': 'Programming',
  'go': 'Programming',
  'c++': 'Programming',
  'java': 'Programming',

  // Web Development
  'web dev': 'Web Development',
  'web development': 'Web Development',
  'frontend': 'Web Development',
  'front end': 'Web Development',
  'front-end': 'Web Development',
  'backend': 'Web Development',
  'back end': 'Web Development',
  'back-end': 'Web Development',
  'fullstack': 'Web Development',
  'full stack': 'Web Development',
  'full-stack': 'Web Development',
  'react': 'Web Development',
  'nextjs': 'Web Development',
  'vue': 'Web Development',
  'javascript': 'Web Development',
  'js': 'Web Development',
  'typescript': 'Web Development',
  'ts': 'Web Development',
  'node': 'Web Development',
  'nodejs': 'Web Development',
  'html': 'Web Development',
  'css': 'Web Development',
  'tailwind': 'Web Development',

  // Data Science
  'data science': 'Data Science',
  'data analytics': 'Data Science',
  'data analysis': 'Data Science',
  'big data': 'Data Science',
  'statistics': 'Data Science',
  'stats': 'Data Science',
  'sql': 'Data Science',

  // Mathematics & Science
  'math': 'Mathematics',
  'maths': 'Mathematics',
  'mathematics': 'Mathematics',
  'calculus': 'Mathematics',
  'algebra': 'Mathematics',
  'linear algebra': 'Mathematics',
  'discrete math': 'Mathematics',
  'science': 'Science',
  'physics': 'Science',
  'theoretical physics': 'Science',
  'astronomy': 'Science',
  'astrophysics': 'Science',
  'quantum': 'Science',
  'quantum computing': 'Science',
  'chemistry': 'Science',
  'biology': 'Science',
  'biotech': 'Science',
  'neuroscience': 'Science',

  // Design & UI/UX
  'design': 'Design',
  'ui': 'Design',
  'ux': 'Design',
  'ui/ux': 'Design',
  'ui/ux design': 'Design',
  'product design': 'Design',
  'graphic design': 'Design',
  'typography': 'Design',
  'interaction design': 'Design',

  // Business & Entrepreneurship
  'business': 'Business',
  'startups': 'Startups',
  'startup': 'Startups',
  'entrepreneurship': 'Startups',
  'founder': 'Startups',
  'venture capital': 'Business',
  'vc': 'Business',
  'marketing': 'Business',
  'economics': 'Business',
  'finance': 'Business',

  // Languages & Education
  'languages': 'Languages',
  'language learning': 'Languages',
  'linguistics': 'Languages',
  'polyglot': 'Languages',
  'education': 'Education',
  'teaching': 'Education',
  'learning': 'Education',
  'pedagogy': 'Education',

  // Creative & Media
  'books': 'Books',
  'reading': 'Books',
  'literature': 'Books',
  'writing': 'Books',
  'creative writing': 'Books',
  'music': 'Music',
  'music production': 'Music',
  'audio engineering': 'Music',
  'movies': 'Movies',
  'cinema': 'Movies',
  'film': 'Movies',
  'filmmaking': 'Movies',
  'photography': 'Photography',
  'photo': 'Photography',

  // Hobbies & Lifestyle (Strictly educational/lifestyle/hobbies, no dating/adult)
  'gaming': 'Gaming',
  'video games': 'Gaming',
  'game dev': 'Gaming',
  'game development': 'Gaming',
  'sports': 'Sports',
  'fitness': 'Sports',
  'athletics': 'Sports',
  'travel': 'Travel',
  'backpacking': 'Travel',
  'philosophy': 'Philosophy',
  'history': 'History',
};

// Broad topic categorizations
export const INTEREST_TOPIC_MAP: Record<string, string> = {
  'AI': 'Technology',
  'Programming': 'Technology',
  'Web Development': 'Technology',
  'Data Science': 'Technology',
  'Startups': 'Business & Innovation',
  'Business': 'Business & Innovation',
  'Mathematics': 'STEM',
  'Science': 'STEM',
  'Philosophy': 'Humanities',
  'History': 'Humanities',
  'Design': 'Creative & Arts',
  'Photography': 'Creative & Arts',
  'Music': 'Creative & Arts',
  'Movies': 'Media & Arts',
  'Books': 'Media & Arts',
  'Education': 'Learning & Growth',
  'Languages': 'Learning & Growth',
  'Travel': 'Lifestyle & Culture',
  'Gaming': 'Entertainment & Tech',
  'Sports': 'Athletics & Health',
};

// Related interest clusters (medium weight)
export const RELATED_INTEREST_PAIRS: Array<[string, string]> = [
  ['AI', 'Data Science'],
  ['AI', 'Programming'],
  ['AI', 'Mathematics'],
  ['Programming', 'Web Development'],
  ['Programming', 'Data Science'],
  ['Design', 'Web Development'],
  ['Startups', 'Business'],
  ['Startups', 'Programming'],
  ['Mathematics', 'Data Science'],
  ['Mathematics', 'Science'],
  ['Books', 'Philosophy'],
  ['Books', 'History'],
  ['Movies', 'Music'],
  ['Photography', 'Design'],
  ['Education', 'Languages'],
  ['Travel', 'Languages'],
  ['Gaming', 'Programming'],
  ['Science', 'Philosophy'],
];

/**
 * Normalizes an interest string by trimming, lowercasing,
 * and checking for known synonyms or aliases.
 */
export function normalizeInterest(raw: string): string {
  const clean = raw.trim().toLowerCase();
  if (!clean) return '';
  if (INTEREST_SYNONYMS[clean]) {
    return INTEREST_SYNONYMS[clean];
  }
  // Return cleaned version with preserved casing formatting if not in synonym map
  return raw.trim();
}

/**
 * Normalizes profession input for comparison:
 * Lowercases, trims, and maps common synonyms.
 */
export function normalizeProfession(raw?: string): string {
  if (!raw) return '';
  const clean = raw.trim().toLowerCase();
  if (!clean) return '';

  const professionSynonyms: Record<string, string> = {
    'software engineer': 'developer',
    'software developer': 'developer',
    'programmer': 'developer',
    'coder': 'developer',
    'web developer': 'developer',
    'frontend developer': 'developer',
    'backend developer': 'developer',
    'full stack developer': 'developer',
    'dev': 'developer',
    'engineer': 'engineer',
    'educator': 'teacher',
    'teacher': 'teacher',
    'professor': 'teacher',
    'instructor': 'teacher',
    'student': 'student',
    'undergrad': 'student',
    'graduate student': 'student',
    'phd candidate': 'student',
    'designer': 'designer',
    'ui designer': 'designer',
    'ux designer': 'designer',
    'product designer': 'designer',
    'founder': 'entrepreneur',
    'entrepreneur': 'entrepreneur',
    'scientist': 'researcher',
    'researcher': 'researcher',
    'writer': 'writer',
    'author': 'writer',
    'journalist': 'writer',
  };

  return professionSynonyms[clean] || clean;
}

/**
 * Helper to extract exact interest matches between two lists
 */
export function getExactInterestMatches(interestsA: string[], interestsB: string[]): string[] {
  const normMapA = new Map<string, string>();
  for (const item of interestsA) {
    const norm = normalizeInterest(item).toLowerCase();
    if (norm) normMapA.set(norm, item);
  }

  const matches = new Set<string>();
  for (const item of interestsB) {
    const normB = normalizeInterest(item).toLowerCase();
    if (normB && normMapA.has(normB)) {
      matches.add(item);
    }
  }

  return Array.from(matches);
}

/**
 * Helper to extract related interest matches
 */
export function getRelatedInterestMatches(
  interestsA: string[],
  interestsB: string[],
  exactMatches: string[]
): Array<[string, string]> {
  const exactSet = new Set(exactMatches.map((m) => normalizeInterest(m).toLowerCase()));
  const setA = new Set(interestsA.map((i) => normalizeInterest(i)));
  const setB = new Set(interestsB.map((i) => normalizeInterest(i)));

  const related: Array<[string, string]> = [];

  for (const [int1, int2] of RELATED_INTEREST_PAIRS) {
    const aHas1 = setA.has(int1);
    const aHas2 = setA.has(int2);
    const bHas1 = setB.has(int1);
    const bHas2 = setB.has(int2);

    const isExact1 = exactSet.has(int1.toLowerCase());
    const isExact2 = exactSet.has(int2.toLowerCase());

    if (aHas1 && bHas2 && !isExact1 && !isExact2) {
      related.push([int1, int2]);
    } else if (aHas2 && bHas1 && !isExact1 && !isExact2) {
      related.push([int2, int1]);
    }
  }

  return related;
}

/**
 * Calculates genuine compatibility score between two anonymous users.
 * Priority:
 * 1. Exact shared interests (highest weight: EXACT_MATCH_WEIGHT = 40)
 * 2. Multiple shared interests (additional cumulative bonus: CUMULATIVE_INTEREST_BONUS_WEIGHT = 15)
 * 3. Related/semantically similar interests (medium weight: RELATED_MATCH_WEIGHT = 20)
 * 4. Shared topic, if applicable (additional weight: TOPIC_MATCH_WEIGHT = 10)
 * 5. Same profession (+15) or complementary role (+10), never required, never primary.
 * 6. Different or missing profession -> no penalty, no reduction in score.
 */
export function isComplementaryProfessionPair(professionA?: string, professionB?: string): boolean {
  const normProfA = normalizeProfession(professionA);
  const normProfB = normalizeProfession(professionB);

  if (!normProfA || !normProfB || normProfA === normProfB) {
    return false;
  }

  const pairKeyA = `${normProfA}:${normProfB}`;
  const pairKeyB = `${normProfB}:${normProfA}`;
  const complementaryPairs = new Set([
    'developer:designer',
    'designer:developer',
    'student:teacher',
    'teacher:student',
    'writer:researcher',
    'researcher:writer',
    'engineer:designer',
    'designer:engineer',
  ]);

  return complementaryPairs.has(pairKeyA) || complementaryPairs.has(pairKeyB);
}

export function calculateCompatibility(
  sessionA: QueuedSession,
  sessionB: QueuedSession
): MatchScoreResult {
  let score = 0;

  // 1. Exact Shared Interests
  const exactMatches = getExactInterestMatches(sessionA.interests, sessionB.interests);
  score += exactMatches.length * EXACT_MATCH_WEIGHT;

  // 2. Cumulative bonus for multiple exact shared interests (2+)
  if (exactMatches.length > 1) {
    score += (exactMatches.length - 1) * CUMULATIVE_INTEREST_BONUS_WEIGHT;
  }

  // 3. Related / Semantically Similar Interests
  const relatedMatches = getRelatedInterestMatches(sessionA.interests, sessionB.interests, exactMatches);
  score += relatedMatches.length * RELATED_MATCH_WEIGHT;

  // 4. Shared Topics (Broad Category)
  const sharedTopics = new Set<string>();
  const topicsA = new Set(
    sessionA.interests
      .map((i) => INTEREST_TOPIC_MAP[normalizeInterest(i)])
      .filter(Boolean)
  );

  for (const interestB of sessionB.interests) {
    const topicB = INTEREST_TOPIC_MAP[normalizeInterest(interestB)];
    if (topicB && topicsA.has(topicB)) {
      sharedTopics.add(topicB);
    }
  }
  score += sharedTopics.size * TOPIC_MATCH_WEIGHT;

  // 5. Profession Match / Complementary bonus
  const normProfA = normalizeProfession(sessionA.profession);
  const normProfB = normalizeProfession(sessionB.profession);
  let isSharedProfession = false;
  let isComplementaryProfession = false;

  if (normProfA && normProfB && normProfA === normProfB) {
    score += PROFESSION_BONUS_WEIGHT;
    isSharedProfession = true;
  } else if (isComplementaryProfessionPair(normProfA, normProfB)) {
    score += COMPLEMENTARY_PROFESSION_BONUS_WEIGHT;
    isComplementaryProfession = true;
  }
  // No penalty branch for missing/mismatched profession — intentionally absent.

  // 6. Country Match bonus (when country-wise matching is active)
  const countryA = (sessionA.country || 'GLOBAL').trim().toUpperCase();
  const countryB = (sessionB.country || 'GLOBAL').trim().toUpperCase();
  if (countryA !== 'GLOBAL' && countryB !== 'GLOBAL' && countryA === countryB) {
    score += COUNTRY_MATCH_WEIGHT;
  }

  return {
    score,
    sharedInterests: exactMatches,
    sharedTopics: Array.from(sharedTopics),
    isSharedProfession,
    isComplementaryProfession,
  };
}

export class MatchmakerQueue {
  private queue: Map<string, QueuedSession> = new Map();
  // Secondary O(1) indices for zero-lag filtering
  private modeIndex: Map<string, Set<string>> = new Map();
  private countryIndex: Map<string, Set<string>> = new Map();
  private blockedPairsRef: Set<string>;
  private blockedIpPairsRef: Set<string>;

  constructor(blockedPairs?: Set<string>, blockedIpPairs?: Set<string>) {
    this.blockedPairsRef = blockedPairs || new Set();
    this.blockedIpPairsRef = blockedIpPairs || new Set();
  }

  public enqueue(session: QueuedSession): void {
    this.queue.set(session.sessionId, session);

    const mode = (session.mode || 'video').toLowerCase();
    let modeSet = this.modeIndex.get(mode);
    if (!modeSet) {
      modeSet = new Set();
      this.modeIndex.set(mode, modeSet);
    }
    modeSet.add(session.sessionId);

    const country = (session.country || 'GLOBAL').trim().toUpperCase();
    let countrySet = this.countryIndex.get(country);
    if (!countrySet) {
      countrySet = new Set();
      this.countryIndex.set(country, countrySet);
    }
    countrySet.add(session.sessionId);
  }

  public dequeue(sessionId: string): QueuedSession | undefined {
    const session = this.queue.get(sessionId);
    if (session) {
      this.remove(sessionId);
    }
    return session;
  }

  public remove(sessionId: string): void {
    const session = this.queue.get(sessionId);
    if (!session) return;

    this.queue.delete(sessionId);

    const mode = (session.mode || 'video').toLowerCase();
    const modeSet = this.modeIndex.get(mode);
    if (modeSet) {
      modeSet.delete(sessionId);
      if (modeSet.size === 0) this.modeIndex.delete(mode);
    }

    const country = (session.country || 'GLOBAL').trim().toUpperCase();
    const countrySet = this.countryIndex.get(country);
    if (countrySet) {
      countrySet.delete(sessionId);
      if (countrySet.size === 0) this.countryIndex.delete(country);
    }
  }

  public size(): number {
    return this.queue.size;
  }

  /**
   * Finds the best compatible partner currently in the queue.
   * Leverages mode index for O(1) candidate pool retrieval, checks blocks/skips,
   * prioritizes highest compatibility score, and caps execution to <2ms to avoid event-loop lag.
   */
  public findBestMatch(currentSession: QueuedSession): {
    matchedSession: QueuedSession;
    scoreResult: MatchScoreResult;
  } | null {
    const targetMode = (currentSession.mode || 'video').toLowerCase();
    const candidateIds = this.modeIndex.get(targetMode);

    if (!candidateIds || candidateIds.size === 0) {
      return null;
    }

    let bestMatch: QueuedSession | null = null;
    let bestResult: MatchScoreResult | null = null;

    const startTime = typeof performance !== 'undefined' ? performance.now() : Date.now();
    const MAX_EVAL_TIME_MS = 2.0;
    let evalCount = 0;

    for (const candidateId of candidateIds) {
      if (candidateId === currentSession.sessionId) continue;

      const candidateSession = this.queue.get(candidateId);
      if (!candidateSession) continue;

      // Event loop starvation guard: Check elapsed time every 32 evaluations
      evalCount++;
      if ((evalCount & 31) === 0) {
        const elapsed = (typeof performance !== 'undefined' ? performance.now() : Date.now()) - startTime;
        if (elapsed >= MAX_EVAL_TIME_MS) {
          break;
        }
      }

      if (
        !(currentSession.isLoopback && candidateSession.isLoopback) &&
        currentSession.visitorId &&
        currentSession.visitorId === candidateSession.visitorId
      ) {
        continue;
      }

      // 1. Session ID check (skips/recent matches)
      if (
        currentSession.blockedSessionIds.has(candidateId) ||
        candidateSession.blockedSessionIds.has(currentSession.sessionId)
      ) {
        continue;
      }

      // 2. Persistent Visitor ID check: Exclude any pair if either appears in other's block history or global in-memory store
      if (currentSession.visitorId && candidateSession.visitorId) {
        const pairKeyA = `${currentSession.visitorId}:${candidateSession.visitorId}`;
        const pairKeyB = `${candidateSession.visitorId}:${currentSession.visitorId}`;

        if (
          this.blockedPairsRef.has(pairKeyA) ||
          this.blockedPairsRef.has(pairKeyB) ||
          currentSession.blockedVisitorIds?.has(candidateSession.visitorId) ||
          candidateSession.blockedVisitorIds?.has(currentSession.visitorId)
        ) {
          continue;
        }
      }

      // 3. Respect explicit IP blocks, but do not treat a shared network as the same user.
      if (currentSession.ipAddress && candidateSession.ipAddress) {
        const ipPairKeyA = `${currentSession.ipAddress}:${candidateSession.ipAddress}`;
        const ipPairKeyB = `${candidateSession.ipAddress}:${currentSession.ipAddress}`;

        if (this.blockedIpPairsRef.has(ipPairKeyA) || this.blockedIpPairsRef.has(ipPairKeyB)) {
          continue;
        }
      }

      // 4. Strict Profession Filter Check (Only applied when explicitly enabled by either user)
      if (currentSession.strictProfessionFilter || candidateSession.strictProfessionFilter) {
        const profA = normalizeProfession(currentSession.profession);
        const profB = normalizeProfession(candidateSession.profession);
        if (!profA || !profB || profA !== profB) {
          continue;
        }
      }

      const scoreResult = calculateCompatibility(currentSession, candidateSession);

      // Progressive relaxation for small user pools:
      // If either user has been in the queue for > 6 seconds, allow match even if interest overlap is low/0,
      // providing an open discovery baseline affinity score.
      const now = Date.now();
      const waitTimeMs = Math.max(
        currentSession.queuedAt ? now - currentSession.queuedAt : 0,
        candidateSession.queuedAt ? now - candidateSession.queuedAt : 0
      );

      let effectiveScore = scoreResult.score;
      if (effectiveScore === 0 && waitTimeMs >= 6000) {
        effectiveScore = 60; // Open discovery baseline score
        scoreResult.score = 60;
      }

      // Select candidate with the highest compatibility score
      if (!bestResult || effectiveScore > bestResult.score) {
        bestMatch = candidateSession;
        bestResult = scoreResult;

        // If high affinity score threshold is already achieved (>140), short-circuit to preserve CPU
        if (effectiveScore >= 140) {
          break;
        }
      }
    }

    if (bestMatch && bestResult) {
      // Remove candidate from queue using O(1) multi-index cleanup
      this.remove(bestMatch.sessionId);
      return {
        matchedSession: bestMatch,
        scoreResult: bestResult,
      };
    }

    return null;
  }
}
