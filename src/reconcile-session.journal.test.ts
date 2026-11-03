/**
 * A writer killed mid-transaction (host reboot, OOM, Docker restart) leaves a
 * hot outbound.db-journal. The host's read-only opens cannot roll it back, so
 * every mailbox read for that session throws until a read-write open happens.
 */
import { spawn, spawnSync } from 'child_process';
import fs from 'fs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const runtime = vi.hoisted(() => ({
  noContainer: true,
  wake: vi.fn().mockResolvedValue(true),
}));

vi.mock('./container-runner.js', () => ({
  wakeContainer: runtime.wake,
  isContainerRunning: vi.fn(() => !runtime.noContainer),
  confirmNoContainer: vi.fn(async () => runtime.noContainer),
  getContainerStartedAtMs: vi.fn(),
  killContainer: vi.fn(),
  buildAgentGroupImage: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('./config.js', async () => {
  const actual = await vi.importActual<typeof import('./config.js')>('./config.js');
  return { ...actual, DATA_DIR: '/tmp/nanoclaw-test-journal', GROUPS_DIR: '/tmp/nanoclaw-test-journal/groups' };
});

const TEST_DIR = '/tmp/nanoclaw-test-journal';

import { confirmNoContainer } from './container-runner.js';
import { closeDb, createAgentGroup, createMessagingGroup, initTestDb, runMigrations } from './db/index.js';
import { outboundDbPath } from './mailbox/sqlite/paths.js';
import { rollBackOutboundJournal } from './mailbox/sqlite/session-db.js';
import { reconcileSession } from './reconcile-session.js';
import { resolveSession, withExistingMailboxSession, withMailboxSession } from './session-manager.js';

const AGENT_GROUP = 'ag-journal';

/** Node script: open a transaction big enough to spill to disk, then run `next`. */
const writerScript = (next: string) => `
  const Database = require('better-sqlite3');
  const db = new Database(process.argv[1]);
  db.pragma('cache_size = 1');
  db.exec('BEGIN IMMEDIATE');
  const insert = db.prepare("INSERT INTO messages_out (id, seq, timestamp, kind, content) VALUES (?, ?, '2026-01-01T00:00:00.000Z', 'chat', ?)");
  for (let i = 0; i < 500; i++) insert.run('row-' + i, 100001 + i * 2, 'x'.repeat(4096));
  ${next}
`;

/** Kill a real writer process inside an open transaction, as a reboot would. */
function strandJournal(dbPath: string): void {
  const script = writerScript("process.kill(process.pid, 'SIGKILL');");
  const result = spawnSync(process.execPath, ['-e', script, dbPath], { cwd: process.cwd() });
  expect(result.signal).toBe('SIGKILL');
  expect(fs.existsSync(`${dbPath}-journal`)).toBe(true);
}

async function createSession() {
  await createMessagingGroup({
    id: 'mg-journal',
    channel_type: 'test',
    platform_id: 'chat-journal',
    name: 'journal',
    is_group: 0,
    unknown_sender_policy: 'public',
    created_at: new Date().toISOString(),
  });
  const { session } = await resolveSession(AGENT_GROUP, 'mg-journal', null, 'shared');
  return { session, dbPath: outboundDbPath(AGENT_GROUP, session.id) };
}

describe('stranded outbound journal', () => {
  beforeEach(async () => {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
    fs.mkdirSync(TEST_DIR, { recursive: true });
    await runMigrations(await initTestDb());
    await createAgentGroup({
      id: AGENT_GROUP,
      name: 'journal',
      folder: 'journal',
      agent_provider: null,
      created_at: new Date().toISOString(),
    });
    runtime.noContainer = true;
    runtime.wake.mockClear();
    vi.mocked(confirmNoContainer).mockClear();
  });

  afterEach(async () => {
    await closeDb();
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
  });

  const readOutbound = (sessionId: string) =>
    withExistingMailboxSession(AGENT_GROUP, sessionId, (mailbox) => mailbox.getDueMessages());

  it('makes every read-only mailbox read throw', async () => {
    const { session, dbPath } = await createSession();
    strandJournal(dbPath);
    await expect(readOutbound(session.id)).rejects.toThrow(/readonly/i);
  });

  it('rolls the torn write back on the next sweep, and wakes overdue work in that sweep', async () => {
    const { session, dbPath } = await createSession();
    await withMailboxSession(AGENT_GROUP, session.id, (mailbox) =>
      mailbox.insertMessage({
        id: 'overdue',
        kind: 'task',
        timestamp: '2026-01-01T00:00:00.000Z',
        platformId: null,
        channelType: null,
        threadId: null,
        content: JSON.stringify({ prompt: 'run' }),
        processAfter: null,
        recurrence: null,
      }),
    );
    strandJournal(dbPath);

    await reconcileSession(session.id);

    expect(fs.existsSync(`${dbPath}-journal`)).toBe(false);
    await expect(readOutbound(session.id)).resolves.toEqual([]);
    expect(runtime.wake).toHaveBeenCalledTimes(1);
  });

  it('leaves the journal alone while a container may own it', async () => {
    const { session, dbPath } = await createSession();
    strandJournal(dbPath);
    runtime.noContainer = false;

    await reconcileSession(session.id);

    expect(fs.existsSync(`${dbPath}-journal`)).toBe(true);
  });

  it('asks about containers only when there is a journal', async () => {
    const { session } = await createSession();
    await reconcileSession(session.id);
    expect(confirmNoContainer).not.toHaveBeenCalled();
  });

  it('throws at once on a lock held by a live writer, and the writer still commits', async () => {
    const { session, dbPath } = await createSession();
    const script = writerScript(`
      console.log('ready');
      process.stdin.once('data', () => { db.exec('COMMIT'); db.close(); process.exit(0); });
    `);
    const writer = spawn(process.execPath, ['-e', script, dbPath], { stdio: ['pipe', 'pipe', 'inherit'] });
    await new Promise((resolve) => writer.stdout.once('data', resolve));

    const startedAt = Date.now();
    expect(() => rollBackOutboundJournal(dbPath)).toThrow(/locked/);
    expect(Date.now() - startedAt).toBeLessThan(1000);

    const exited = new Promise((resolve) => writer.once('exit', resolve));
    writer.stdin.write('commit');
    await exited;
    await expect(readOutbound(session.id)).resolves.toHaveLength(500);
  }, 30_000);
});
