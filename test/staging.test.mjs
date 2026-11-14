import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {SpillableStaging} from '../src/staging.mjs';

async function tempStaging(budgetBytes) {
  const directory = await mkdtemp(path.join(tmpdir(), 'staging-'));
  const staging = new SpillableStaging({directory, budgetBytes});
  return {directory, staging};
}

function entry(size) {
  return {1: {payload: 'x'.repeat(size)}};
}

test('内存超预算时溢出到有序分块，读回内容一致', async () => {
  const {directory, staging} = await tempStaging(200);
  try {
    for (let i = 0; i < 20; i++) {
      await staging.put(`key-${String(i).padStart(3, '0')}`, entry(60));
    }
    assert.ok(staging.spillCount >= 1, '应触发至少一次落盘');
    assert.ok(staging.memoryBytes <= 200);
    const got = await staging.get('key-007');
    assert.equal(got[1].payload, 'x'.repeat(60));
    const files = await readdir(directory);
    assert.ok(files.some(f => f.endsWith('.jsonl')));
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('tombstone 后配对身份不再出现在 keys 中', async () => {
  const {directory, staging} = await tempStaging(120);
  try {
    for (let i = 0; i < 10; i++) await staging.put(`k${i}`, entry(50));
    await staging.delete('k3');
    await staging.delete('k7');
    const keys = [];
    for await (const key of staging.keys()) keys.push(key);
    assert.ok(!keys.includes('k3'));
    assert.ok(!keys.includes('k7'));
    assert.equal(await staging.get('k3'), undefined);
    assert.ok(keys.length >= 6);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('带 lane 的身份键互不覆盖（同名 read 跨 lane）', async () => {
  const {directory, staging} = await tempStaging(1024 * 1024);
  try {
    await staging.put('run\u0000lane1\u0000read-a', {1: {lane: 'lane1'}});
    await staging.put('run\u0000lane2\u0000read-a', {1: {lane: 'lane2'}});
    assert.equal((await staging.get('run\u0000lane1\u0000read-a'))[1].lane, 'lane1');
    assert.equal((await staging.get('run\u0000lane2\u0000read-a'))[1].lane, 'lane2');
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('更新已有条目不产生重复身份', async () => {
  const {directory, staging} = await tempStaging(1024 * 1024);
  try {
    await staging.put('k', {1: {end: 1}});
    await staging.put('k', {1: {end: 1}, 2: {end: 2}});
    const keys = [];
    for await (const key of staging.keys()) keys.push(key);
    assert.deepEqual(keys, ['k']);
    assert.deepEqual(Object.keys(await staging.get('k')).sort(), ['1', '2']);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});
