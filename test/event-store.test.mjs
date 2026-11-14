import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {
  EventStore, EVENT_TYPES, fingerprint, pairKey, parsePairKey,
  replay, applyEvent, createEmptyState,
} from '../src/event-store.mjs';

async function tempStore(syncWrites = false) {
  const directory = await mkdtemp(path.join(tmpdir(), 'eventstore-'));
  const store = await new EventStore(directory, 'run1', {syncWrites}).open();
  return {directory, store};
}

test('fingerprint 与 pairKey 稳定且可还原', () => {
  const record = {header: '@a/1', sequence: 'AC', quality: '!!'};
  assert.equal(fingerprint(record).length, 64);
  assert.equal(fingerprint(record), fingerprint({...record}));
  assert.notEqual(fingerprint(record), fingerprint({...record, sequence: 'AG'}));
  const key = pairKey('run1', 'lane1', 'read-a');
  assert.deepEqual(parsePairKey(key), {runId: 'run1', laneId: 'lane1', readId: 'read-a'});
});

test('同一 (file, ordinal) 的到达类事件幂等，重放不重复', async () => {
  const {directory, store} = await tempStore();
  try {
    const payload = {file: 'f.gz', ordinal: 3, pairKey: 'k', end: 1, fingerprint: 'x'};
    const first = await store.append(EVENT_TYPES.RECORD, payload);
    const second = await store.append(EVENT_TYPES.RETRANSMIT, payload);
    assert.ok(first);
    assert.equal(second, null);
    let count = 0;
    for await (const _ of store.readEvents()) count++;
    assert.equal(count, 1);
  } finally {
    await store.close();
    await rm(directory, {recursive: true, force: true});
  }
});

test('PAIR 以 pairKey 幂等且重放后从 arrivals 移除', async () => {
  const state = createEmptyState();
  const pair = {pairKey: pairKey('r', 'l', 'a'), run: 'r', lane: 'l', read: 'a'};
  applyEvent(state, {type: EVENT_TYPES.RECORD, payload: {file: 'f', ordinal: 0, pairKey: pair.pairKey, end: 1, fingerprint: 'x'}});
  assert.ok(state.arrivals.has(pair.pairKey));
  applyEvent(state, {type: EVENT_TYPES.PAIR, payload: pair});
  applyEvent(state, {type: EVENT_TYPES.PAIR, payload: pair}); // 重复不计数
  assert.equal(state.pairs.size, 1);
  assert.equal(state.pairOrder.length, 1);
  assert.equal(state.arrivals.has(pair.pairKey), false);
});

test('中断后重放账本可完整重建状态', async () => {
  const {directory, store} = await tempStore();
  try {
    await store.append(EVENT_TYPES.CLAIM, {file: 'f.gz', start: 0, end: 10});
    await store.append(EVENT_TYPES.STREAM_DONE, {file: 'f.gz', totalBytes: 10});
    await store.append(EVENT_TYPES.RECORD, {file: 'f.gz', ordinal: 0, pairKey: pairKey('r', 'l', 'a'), end: 1, fingerprint: 'x'});
    await store.append(EVENT_TYPES.BAD_RECORD, {file: 'f.gz', ordinal: 1, start: 5, end: 9});
    await store.append(EVENT_TYPES.BAD_TAIL, {file: 'f.gz', start: 100, end: 110, reason: 'x'});
    await store.close();

    const reopened = await new EventStore(directory, 'run1').open();
    const events = await collect(reopened.readEvents());
    const state = replay(events);
    assert.equal(events.length, 5);
    assert.equal(state.files.get('f.gz').streamDone, true);
    assert.equal(state.badRecords.length, 1);
    assert.ok(state.files.get('f.gz').badTail);
    await reopened.close();
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('同一文件不同起点的坏尾事件分别保留', async () => {
  const {directory, store} = await tempStore();
  try {
    await store.append(EVENT_TYPES.BAD_TAIL, {file: 'f.gz', start: 100, end: 110});
    const again = await store.append(EVENT_TYPES.BAD_TAIL, {file: 'f.gz', start: 100, end: 110});
    const different = await store.append(EVENT_TYPES.BAD_TAIL, {file: 'f.gz', start: 200, end: 220});
    assert.equal(again, null);
    assert.ok(different);
  } finally {
    await store.close();
    await rm(directory, {recursive: true, force: true});
  }
});

async function collect(iterable) {
  const out = [];
  for await (const item of iterable) out.push(item);
  return out;
}
