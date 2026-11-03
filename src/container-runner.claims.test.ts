/**
 * Session-claim writes through the real lifecycle: adoption claims a session
 * (incarnation bump + CAS), finalization releases the claim. (Originally
 * written for the shadow phase; the rows are now the fencing authority and
 * every assertion carried unchanged.)
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import type { SupervisedHandle, SupervisedSnapshot } from './drivers/session-events.js';

const snapshots: SupervisedSnapshot[] = [];
const listHook: { run?: () => void } = {};
vi.mock('./drivers/index.js', () => ({
  getSessionDriver: () => ({
    listSessions: async () => {
      listHook.run?.();
      return snapshots;
    },
    capabilities: () => ({}),
  }),
  isSessionEventsDriver: () => false,
}));

import {
  abortGatewaySessionObservers,
  adoptRunningSessions,
  confirmNoContainer,
  _resetAdoptionRetryStateForTesting,
  _setFinishFenceScheduleForTesting,
  isContainerRunning,
  killContainer,
  wakeContainer,
} from './container-runner.js';
import { type GatewaySessionInput, resetGatewayProvider } from './gateway-providers/index.js';
const release = vi.fn(async () => {});
const ensure = vi.fn(async (_input: GatewaySessionInput) => ({
  contribution: {
    networkAccess: { endpoint: 'http://proxy:8080', target: { kind: 'runtime' as const, identity: 'proxy' } },
  },
  release,
}));
const reapOrphans = vi.fn(async () => {});
import { getSessionClaim } from './db/coordination.js';
import { initTestDb, closeDb, runMigrations, createAgentGroup, createSession, getSession } from './db/index.js';

function now(): string {
  return new Date().toISOString();
}

function fakeHandle(sessionId: string, name: string): SupervisedHandle {
  const terminalCallbacks: Array<(failure?: unknown) => void> = [];
  return {
    key: { installSlug: 'test-install', agentGroupId: 'ag-1', sessionId },
    name,
    async start() {},
    async stop() {
      for (const callback of terminalCallbacks) callback(undefined);
    },
    async status() {
      return { phase: 'running' };
    },
    onTerminal(callback: (failure?: unknown) => void) {
      terminalCallbacks.push(callback);
    },
  } as unknown as SupervisedHandle;
}

beforeEach(async () => {
  snapshots.length = 0;
  delete listHook.run;
  release.mockClear();
  ensure.mockClear();
  reapOrphans.mockClear();
  resetGatewayProvider({
    kind: 'test-lifecycle',
    agentSkills: [],
    sessions: { ensure, reapOrphans },
    approvals: { subscribe: async () => {} },
  });
  const db = await initTestDb();
  await runMigrations(db);
  await createAgentGroup({
    id: 'ag-1',
    name: 'Test Agent',
    folder: 'test-agent',
    agent_provider: null,
    created_at: now(),
  });
  await createSession({
    id: 'sess-1',
    agent_group_id: 'ag-1',
    messaging_group_id: null,
    thread_id: null,
    agent_provider: null,
    status: 'active',
    container_status: 'running',
    last_active: now(),
    created_at: now(),
  });
});

afterEach(async () => {
  // Drain any runtime left registered so cross-test state cannot leak.
  if (isContainerRunning('sess-1')) {
    killContainer('sess-1', 'test-teardown');
    await vi.waitFor(() => expect(isContainerRunning('sess-1')).toBe(false));
  }
  resetGatewayProvider();
  await closeDb();
});

describe('confirmNoContainer', () => {
  const snapshot = (sessionId: string, phase: string) =>
    ({ handle: fakeHandle(sessionId, 'container'), phase }) as SupervisedSnapshot;

  it.each([
    ['nothing listed', [], true],
    ['a terminal container', [['sess-1', 'terminal']], true],
    ["another session's container", [['sess-2', 'running']], true],
    ['a running container this host did not adopt', [['sess-1', 'running']], false],
    ['a starting container', [['sess-1', 'starting']], false],
  ] as const)('with %s', async (_name, listed, expected) => {
    snapshots.push(...listed.map(([sessionId, phase]) => snapshot(sessionId, phase)));
    expect(await confirmNoContainer('sess-1')).toBe(expected);
  });

  it('is false when the runtime cannot be listed', async () => {
    listHook.run = () => {
      throw new Error('docker down');
    };
    expect(await confirmNoContainer('sess-1')).toBe(false);
  });

  it('is false from adoption until the container has finished', async () => {
    snapshots.push(snapshot('sess-1', 'running'));
    await adoptRunningSessions();
    snapshots.length = 0;
    expect(await confirmNoContainer('sess-1')).toBe(false);

    killContainer('sess-1', 'test-stop');
    await vi.waitFor(async () => expect(await confirmNoContainer('sess-1')).toBe(true));
  });

  it('is false for a surviving container whose adoption claim could not be written', async () => {
    const coordination = await import('./db/coordination.js');
    vi.spyOn(coordination, 'tryClaimSession').mockRejectedValueOnce(new Error('store down'));
    snapshots.push(snapshot('sess-1', 'running'));
    await adoptRunningSessions();
    snapshots.length = 0;
    expect(await confirmNoContainer('sess-1')).toBe(false);
    _resetAdoptionRetryStateForTesting();
  });

  it('is false while a wake is in flight, and if one starts during the listing', async () => {
    const session = (await getSession('sess-1'))!;
    const wake = wakeContainer(session);
    expect(await confirmNoContainer('sess-1')).toBe(false);
    await wake;

    let lateWake: Promise<boolean> | undefined;
    listHook.run = () => {
      lateWake = wakeContainer(session);
    };
    expect(await confirmNoContainer('sess-1')).toBe(false);
    await lateWake;
  });
});

describe('session claim lifecycle', () => {
  it('adoption shadow-claims the session and finalization releases it', async () => {
    snapshots.push({ handle: fakeHandle('sess-1', 'container-a'), phase: 'running' } as SupervisedSnapshot);
    const { adopted } = await adoptRunningSessions();
    expect(adopted).toBe(1);
    expect(ensure.mock.calls[0]?.[0]).toEqual(expect.objectContaining({ disposition: 'adopt' }));
    expect(reapOrphans).toHaveBeenCalledOnce();

    const claim = await getSessionClaim('sess-1');
    expect(claim?.incarnation).toBe(1);
    expect(claim?.claimed_by).toMatch(/:\d+$/);
    expect(claim?.container_ref).toBe('container-a');

    killContainer('sess-1', 'test-stop');
    await vi.waitFor(async () => {
      const released = await getSessionClaim('sess-1');
      expect(released?.claimed_by).toBeNull();
      expect(released?.incarnation).toBe(1);
    });
    await vi.waitFor(() => expect(release).toHaveBeenCalledWith({ kind: 'session-ended', reason: 'test-stop' }));
  });

  it('a re-adopted session bumps the incarnation via CAS', async () => {
    snapshots.push({ handle: fakeHandle('sess-1', 'container-a'), phase: 'running' } as SupervisedSnapshot);
    await adoptRunningSessions();
    killContainer('sess-1', 'test-stop');
    await vi.waitFor(async () => expect((await getSessionClaim('sess-1'))?.claimed_by).toBeNull());

    snapshots.length = 0;
    snapshots.push({ handle: fakeHandle('sess-1', 'container-b'), phase: 'running' } as SupervisedSnapshot);
    await adoptRunningSessions();
    const claim = await getSessionClaim('sess-1');
    expect(claim?.incarnation).toBe(2);
    expect(claim?.container_ref).toBe('container-b');
  });
  it('retains the claim and lease until a failed runtime stop can be retried', async () => {
    _setFinishFenceScheduleForTesting([], 10);
    const handle = fakeHandle('sess-1', 'container-a');
    const stop = handle.stop.bind(handle);
    const failedStop = vi.fn(async () => {
      throw new Error('runtime unavailable');
    });
    handle.stop = failedStop;
    snapshots.push({ handle, phase: 'running' } as SupervisedSnapshot);
    await adoptRunningSessions();
    killContainer('sess-1', 'test-stop');
    await vi.waitFor(() => expect(failedStop).toHaveBeenCalled());
    expect(release).not.toHaveBeenCalled();
    expect((await getSessionClaim('sess-1'))?.claimed_by).not.toBeNull();
    handle.stop = stop;
    await vi.waitFor(() => expect(release).toHaveBeenCalledWith({ kind: 'session-ended', reason: 'test-stop' }));
    _setFinishFenceScheduleForTesting();
  });

  it('awaits detachment on host replacement while leaving the session running', async () => {
    snapshots.push({ handle: fakeHandle('sess-1', 'container-a'), phase: 'running' } as SupervisedSnapshot);
    await adoptRunningSessions();
    await abortGatewaySessionObservers();
    expect(release).toHaveBeenCalledWith({ kind: 'host-detached', reason: 'host-shutdown' });
    expect(isContainerRunning('sess-1')).toBe(true);
  });
});
