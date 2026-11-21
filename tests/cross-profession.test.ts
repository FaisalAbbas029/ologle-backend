import { describe, expect, it } from 'vitest';
import { calculateCompatibility, MatchmakerQueue, QueuedSession } from '../src/matchmaking/matchmaker.js';

describe('Cross-profession matching & progressive relaxation', () => {
  it('matches Student and Farmer who share an interest', () => {
    const student: QueuedSession = {
      sessionId: 'sess_student',
      visitorId: 'vis_student',
      profession: 'Student',
      interests: ['AI', 'Books', 'Technology'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const farmer: QueuedSession = {
      sessionId: 'sess_farmer',
      visitorId: 'vis_farmer',
      profession: 'Farmer',
      interests: ['Agriculture', 'Books', 'Gardening'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const res = calculateCompatibility(student, farmer);
    expect(res.sharedInterests).toContain('Books');
    expect(res.score).toBeGreaterThanOrEqual(100);

    const queue = new MatchmakerQueue();
    queue.enqueue(student);
    const match = queue.findBestMatch(farmer);
    expect(match).not.toBeNull();
    expect(match?.matchedSession.sessionId).toBe('sess_student');
    expect(match?.scoreResult.score).toBeGreaterThanOrEqual(100);
  });

  it('progressively relaxes criteria after 6+ seconds so users with zero shared interests still match in small pools', () => {
    const doctor: QueuedSession = {
      sessionId: 'sess_doctor',
      visitorId: 'vis_doctor',
      profession: 'Doctor',
      interests: ['Medicine', 'Surgery', 'Anatomy'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now() - 7000, // 7 seconds in queue
      matchedSessionId: null,
    };

    const designer: QueuedSession = {
      sessionId: 'sess_designer',
      visitorId: 'vis_designer',
      profession: 'UI/UX Designer',
      interests: ['Typography', 'Color', 'Branding'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const queue = new MatchmakerQueue();
    queue.enqueue(doctor);
    const match = queue.findBestMatch(designer);
    expect(match).not.toBeNull();
    expect(match?.matchedSession.sessionId).toBe('sess_doctor');
    expect(match?.scoreResult.score).toBe(60); // Discovery baseline score
  });

  it('respects strict profession filter when explicitly requested', () => {
    const devStrict: QueuedSession = {
      sessionId: 'sess_dev_strict',
      visitorId: 'vis_dev',
      profession: 'Software Engineer',
      strictProfessionFilter: true, // Strict filter enabled
      interests: ['Programming', 'AI', 'Web Development'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const teacher: QueuedSession = {
      sessionId: 'sess_teacher',
      visitorId: 'vis_teacher',
      profession: 'Teacher',
      interests: ['Programming', 'AI', 'Education'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const queue = new MatchmakerQueue();
    queue.enqueue(devStrict);
    const match = queue.findBestMatch(teacher);
    expect(match).toBeNull(); // Blocked because strict filter requires matching profession
  });

  it('matches Developer and Teacher who share an interest', () => {
    const dev: QueuedSession = {
      sessionId: 'sess_dev_open',
      visitorId: 'vis_dev_open',
      profession: 'Software Developer',
      interests: ['AI', 'Programming', 'Books'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const teacher: QueuedSession = {
      sessionId: 'sess_teacher_open',
      visitorId: 'vis_teacher_open',
      profession: 'Teacher',
      interests: ['Education', 'Books', 'Philosophy'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const res = calculateCompatibility(dev, teacher);
    expect(res.sharedInterests).toContain('Books');

    const queue = new MatchmakerQueue();
    queue.enqueue(dev);
    const match = queue.findBestMatch(teacher);
    expect(match).not.toBeNull();
    expect(match?.matchedSession.sessionId).toBe('sess_dev_open');
  });

  it('CANNOT match blocked users, even after progressive relaxation wait time (10+ seconds)', () => {
    const blocker: QueuedSession = {
      sessionId: 'sess_blocker',
      visitorId: 'vis_user_1',
      profession: 'Accountant',
      interests: ['Finance', 'Tax', 'Excel'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(['sess_target']),
      blockedVisitorIds: new Set(['vis_user_2']),
      queuedAt: Date.now() - 15000, // 15 seconds in queue
      matchedSessionId: null,
    };

    const blockedTarget: QueuedSession = {
      sessionId: 'sess_target',
      visitorId: 'vis_user_2',
      profession: 'Consultant',
      interests: ['Strategy', 'Management', 'Business'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now() - 15000, // 15 seconds in queue
      matchedSessionId: null,
    };

    const queue = new MatchmakerQueue();
    queue.enqueue(blocker);
    const match = queue.findBestMatch(blockedTarget);
    expect(match).toBeNull(); // Block check strictly enforced before relaxation
  });

  it('CANNOT self-match same user/visitorId, even after progressive relaxation wait time', () => {
    const session1: QueuedSession = {
      sessionId: 'sess_same_tab1',
      visitorId: 'vis_same_person',
      profession: 'Writer',
      interests: ['Fiction', 'Novels', 'Publishing'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now() - 20000,
      matchedSessionId: null,
    };

    const session2: QueuedSession = {
      sessionId: 'sess_same_tab2',
      visitorId: 'vis_same_person',
      profession: 'Writer',
      interests: ['Fiction', 'Novels', 'Publishing'],
      country: 'GLOBAL',
      mode: 'video',
      blockedSessionIds: new Set(),
      queuedAt: Date.now(),
      matchedSessionId: null,
    };

    const queue = new MatchmakerQueue();
    queue.enqueue(session1);
    const match = queue.findBestMatch(session2);
    expect(match).toBeNull(); // Self-match prevention strictly enforced before relaxation
  });
});
