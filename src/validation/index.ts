/**
 * OLOGLE Backend — Input Validation & Sanitization
 */

import { RECOGNIZED_PROFESSIONS, OCCUPATIONAL_STEMS, NON_PROFESSION_WORDS } from '../data/professionsData.js';

const LOWERCASE_PROFESSIONS_SET = new Set<string>(
  RECOGNIZED_PROFESSIONS.map((p) => p.toLowerCase().trim())
);

export function validateProfession(profession: string): { isValid: boolean; error?: string } {
  if (!profession) {
    return { isValid: false, error: 'Please enter your profession to continue.' };
  }

  const trimmed = profession.trim();
  
  if (!trimmed) {
    return { isValid: false, error: 'Please enter your profession to continue.' };
  }

  if (trimmed.length < 2) {
    return { isValid: false, error: 'Profession must be at least 2 characters.' };
  }

  if (trimmed.length > 60) {
    return { isValid: false, error: 'Profession must not exceed 60 characters.' };
  }

  if (/[<>{}[\]\\`^$|~]/.test(trimmed)) {
    return { isValid: false, error: 'Please enter a valid profession without special code symbols.' };
  }

  const lettersCount = (trimmed.match(/[a-zA-Z\u00C0-\u024F\u1E00-\u1EFF\u0400-\u04FF\u0600-\u06FF\u4E00-\u9FFF\u3040-\u30FF]/g) || []).length;
  if (lettersCount < 2) {
    return { isValid: false, error: 'Please enter a valid profession.' };
  }

  if (/(.)\1{3,}/i.test(trimmed)) {
    return { isValid: false, error: 'Please enter a valid profession.' };
  }

  const lower = trimmed.toLowerCase();

  const keyboardMashes = [
    'asdfgh', 'sdfghj', 'dfghjk', 'fghjkl',
    'qwerty', 'wertyu', 'ertyui', 'rtyuio', 'tyuiop',
    'zxcvbn', 'xcvbnm',
    'qazwsx', 'wsxedc', 'edcrfv',
    '12345', '23456', '34567', '45678', '56789',
  ];
  for (const mash of keyboardMashes) {
    if (lower.includes(mash)) {
      return { isValid: false, error: 'Please enter a valid profession.' };
    }
  }

  const disallowedPatterns: RegExp[] = [
    /\b(sex|sexy|porn|porno|pornography|nude|nudes|escort|camgirl|camboy|sugar\s*baby|sugar\s*daddy|onlyfans|stripper|hooker|prostitute|fetish|erotic|nsfw|dating|single|singles|horny|milf|fap|dildo|pimp|playboy|playgirl|gigolo|brothel|hookup|tinder|bumble|grindr)\b/i,
    /\b(insta|instagram|snap|snapchat|tiktok|onlyfans|telegram|whatsapp|twitter|facebook|discord|reddit|hmu|dm\s*me|follow\s*me)\b/i,
    /@|http:\/\//i,
    /\b(nigg(er|a|az)?|fagg(ot)?|kike|chink|gook|wetback|spic|tranny|cunt|whore|slut|dick|cock|pussy|tits|penis|vagina|asshole|bitch|bastard|retard|nazi|hitler|kkk|white\s*supremacist)\b/i,
    /\b(terrorist|hitman|murderer|killer|assassin|cartel|drug\s*dealer|scammer|doxx|pedophile|pedo)\b/i,
    /^(we|she|he|they|them|him|her|you|your|me|my|us|our|isreal|israel|palestine)$/i,
  ];

  for (const pattern of disallowedPatterns) {
    if (pattern.test(lower)) {
      return { isValid: false, error: 'Only legitimate professional occupations are allowed. 18+, abusive, social media, and non-profession terms are strictly blocked.' };
    }
  }

  const conversationalPattern = /\b(hello|hey|hi|good\s+morning|good\s+night|how\s+are\s+you|what's\s+up|sup|wanna\s+talk|chat\s+with|looking\s+for|want\s+to|wants\s+to|i\s+am|i'm|my\s+name|who\s+are\s+you|call\s+me|text\s+me|bored|lonely|anyone\s+here|just\s+looking|dating|friendship|meet\s+me|hit\s+me\s+up|please\s+help|what\s+is\s+this|tell\s+me|just\s+chilling|talk\s+to\s+me)\b/i;
  if (conversationalPattern.test(lower)) {
    return {
      isValid: false,
      error: 'Irrelevant text. Please enter a genuine profession or occupation, not conversational phrases.',
    };
  }

  const normalizedText = lower.replace(/[^\w\s-]/g, ' ').replace(/\s+/g, ' ').trim();
  if (
    LOWERCASE_PROFESSIONS_SET.has(lower) ||
    LOWERCASE_PROFESSIONS_SET.has(trimmed.toLowerCase()) ||
    LOWERCASE_PROFESSIONS_SET.has(normalizedText)
  ) {
    return { isValid: true };
  }

  if (/(\s+(&|and|\+|\/)\s+)/i.test(trimmed)) {
    const segments = trimmed.split(/(\s+(&|and|\+|\/)\s+)/i).filter((s) => s.trim().length > 1 && !/^[&+/]|and$/i.test(s.trim()));
    if (segments.length >= 2) {
      const anySegmentRecognized = segments.some((seg) => {
        const segLower = seg.toLowerCase().trim();
        const segNorm = segLower.replace(/[^\w\s-]/g, ' ').replace(/\s+/g, ' ').trim();
        if (LOWERCASE_PROFESSIONS_SET.has(segLower) || LOWERCASE_PROFESSIONS_SET.has(segNorm)) return true;
        const segWords = segNorm.split(/[\s-]+/);
        return segWords.some((w) => OCCUPATIONAL_STEMS.has(w));
      });
      if (anySegmentRecognized) {
        return { isValid: true };
      }
    }
  }

  const words = normalizedText.split(/[\s-]+/).filter((w) => w.length > 0);

  if (words.length > 5) {
    return {
      isValid: false,
      error: 'Irrelevant text. Please enter a concise profession title (e.g. Software Engineer, AI Researcher, UI/UX Designer).',
    };
  }

  const allNonProfWords = words.length > 0 && words.every((w) => NON_PROFESSION_WORDS.has(w));
  if (allNonProfWords) {
    return {
      isValid: false,
      error: 'Irrelevant text. Please enter a genuine profession or occupation (e.g. Software Engineer, AI Researcher, UI/UX Designer).',
    };
  }

  let hasOccupationalStem = false;

  for (const w of words) {
    if (OCCUPATIONAL_STEMS.has(w)) {
      hasOccupationalStem = true;
      break;
    }
  }

  if (!hasOccupationalStem) {
    for (const stem of OCCUPATIONAL_STEMS) {
      if (stem.includes(' ') && normalizedText.includes(stem)) {
        hasOccupationalStem = true;
        break;
      }
    }
  }

  if (!hasOccupationalStem) {
    const professionalSuffixes = [/^[a-z]{3,}(ist|ologist|ician|iatrician|iatrist|grapher|urgen)$/i];
    for (const w of words) {
      if (professionalSuffixes.some((rx) => rx.test(w)) && !NON_PROFESSION_WORDS.has(w)) {
        hasOccupationalStem = true;
        break;
      }
    }
  }

  if (!hasOccupationalStem) {
    return {
      isValid: false,
      error: 'Irrelevant text. Please enter a genuine profession or occupation (e.g. Software Engineer, AI Researcher, UI/UX Designer).',
    };
  }

  return { isValid: true };
}

export function validateTargetInterest(tag: string): { isValid: boolean; error?: string } {
  if (!tag || tag.trim().length < 2) {
    return { isValid: false, error: 'Interest tag must be at least 2 characters.' };
  }
  const clean = tag.trim().toLowerCase();
  if (clean.length > 35) {
    return { isValid: false, error: 'Interest tag must not exceed 35 characters.' };
  }
  return { isValid: true };
}

export function validateInterests(interests: unknown): { isValid: boolean; sanitized: string[]; error?: string } {
  if (!Array.isArray(interests)) {
    return { isValid: false, sanitized: [], error: 'Interests must be provided as an array.' };
  }

  if (interests.length < 3) {
    return { isValid: false, sanitized: [], error: 'Please select at least 3 interests to match.' };
  }

  if (interests.length > 15) {
    return { isValid: false, sanitized: [], error: 'Maximum 15 interests allowed.' };
  }

  const sanitized: string[] = [];
  for (const item of interests) {
    if (typeof item !== 'string') continue;
    // Strip control characters, trim whitespace, limit length to 40 characters
    const clean = item.replace(/[\x00-\x1F\x7F-\x9F]/g, '').trim().slice(0, 40);
    if (clean.length >= 2) {
      sanitized.push(clean);
    }
  }

  if (sanitized.length < 3) {
    return { isValid: false, sanitized: [], error: 'Please select at least 3 valid interests to match.' };
  }

  return { isValid: true, sanitized: sanitized.slice(0, 15) };
}

export function sanitizeText(str: unknown, maxLen = 1000): string {
  if (typeof str !== 'string') return '';
  return str.trim().slice(0, maxLen);
}
