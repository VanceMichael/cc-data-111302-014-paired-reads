import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {openReceiver} from '../src/receiver.mjs';
import {createMockSource, standardManifest, standardFiles, multiMemberGzip} from './mock-source.mjs';

async function tempDir() {
  return mkdtemp(path.join(tmpdir(), 'receiver-'));
}

const openReceivers = [];
test.after(async () => {
  for (const receiver of openReceivers) await receiver.close().catch(() => {});
});

async function buildReceiver(directory, manifest, files, options = {}) {
  const source = createMockSource(manifest, files, options.source);
  const receiver = await openReceiver(directory, manifest.run, manifest, {
    fetchImpl: source.fetchImpl,
    syncWrites: false,
    budgetBytes: options.budgetBytes ?? 4096,
    ...options.receiver,
  });
  openReceivers.push(receiver);
  return {source, receiver};
}

test('完整运行：乱序两端在小块投递下正确配对，两种端标识写法等价', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const {source, receiver} = await buildReceiver(directory, manifest, files, {source: {chunkBytes: 7}});
    await receiver.receiveRun('http://source.test');

    const pairs = receiver.pairs();
    assert.equal(pairs.length, 3, 'lane1 两对 + lane2 一对');
    const lane1 = receiver.pairs('lane1').map(p => p.read).sort();
    assert.deepEqual(lane1, ['read-a', 'read-b']);
    // read-a 的 R1 用 /1，R2 用 /2；read-b 的 R2 用 casava 写法，仍能配上。
    const readB = pairs.find(p => p.read === 'read-b' && p.lane === 'lane1');
    assert.match(readB.r1.header, /\/1$/);
    assert.match(readB.r2.header, / 2:N:0:1$/);
    assert.equal(readB.r1.sequence, 'AG');
    assert.equal(readB.r2.sequence, 'CT');
    assert.deepEqual(await receiver.gaps(), []);
    assert.deepEqual(receiver.badRecords(), []);
    assert.deepEqual(receiver.conflicts(), []);

    const report = await receiver.completeness();
    assert.equal(report.streamsDone, true);
    assert.equal(report.recordsValid, true);
    assert.equal(report.complete, true);
    assert.ok(source.served().length >= 4);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('同名 read 跨 lane 互不覆盖，分别配对', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const {receiver} = await buildReceiver(directory, manifest, files);
    await receiver.receiveRun('http://source.test');
    const lane1A = receiver.pairs('lane1').find(p => p.read === 'read-a');
    const lane2A = receiver.pairs('lane2').find(p => p.read === 'read-a');
    assert.ok(lane1A && lane2A);
    assert.notEqual(lane1A.pairKey, lane2A.pairKey);
    assert.equal(lane1A.r1.sequence, 'ACGT');
    assert.equal(lane2A.r1.sequence, 'AA');
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('坏压缩尾：流结束但记录不合法，抢救输出并标记完整度为否', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    files['lane1-R1.gz'].badTail = true; // 截掉最后 8 字节
    const {receiver} = await buildReceiver(directory, manifest, files);
    await receiver.receiveRun('http://source.test');

    const tails = receiver.badTails();
    assert.ok(tails.some(t => t.file === 'lane1-R1.gz'), '应上报坏压缩尾');
    const report = await receiver.completeness();
    assert.equal(report.streamsDone, true, '流确实结束了');
    assert.equal(report.recordsValid, false, '但存在坏压缩尾，记录不都合法');
    assert.equal(report.complete, false, '运行不完整');
    // lane2 不受影响，仍可完整配对。
    assert.equal(receiver.pairs('lane2').length, 1);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('同端相同内容重复到达认作重传，不产生重复配对', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    // lane2-R1 追加一份完全相同的记录（多一个 member）。
    files['lane2-R1.gz'].gzip = multiMemberGzip([
      '@read-a/1\nAA\n+\n!!\n',
      '@read-a/1\nAA\n+\n!!\n',
    ]);
    const {receiver} = await buildReceiver(directory, manifest, files);
    await receiver.receiveRun('http://source.test');
    assert.ok(receiver.retransmits().some(r => r.readId === 'read-a' && r.lane === 'lane2'));
    assert.equal(receiver.pairs('lane2').length, 1, '重传不产生第二对');
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('同身份不同内容必须隔离，不能任选第一条', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    // lane2-R1 的 read-a 换成不同序列，与后续重传/同身份内容冲突。
    // 先放原始 AA，再放不同内容 GG，同 read 同端不同指纹。
    files['lane2-R1.gz'].gzip = multiMemberGzip([
      '@read-a/1\nAA\n+\n!!\n',
      '@read-a/1\nGG\n+\n!!\n',
    ]);
    const {receiver} = await buildReceiver(directory, manifest, files);
    await receiver.receiveRun('http://source.test');
    const conflicts = receiver.conflicts();
    assert.equal(conflicts.length, 1);
    assert.equal(conflicts[0].readId, 'read-a');
    assert.notEqual(conflicts[0].fingerprint, conflicts[0].existingFingerprint);
    // 第一条保留并仍可与 R2 配对，冲突的第二条不覆盖。
    const pair = receiver.pairs('lane2').find(p => p.read === 'read-a');
    assert.equal(pair.r1.sequence, 'AA');
    const report = await receiver.completeness();
    assert.equal(report.complete, false, '存在冲突隔离，运行不能判完整');
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('端标识与文件所属端不一致按坏记录隔离', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    files['lane2-R1.gz'].gzip = multiMemberGzip(['@read-a/2\nAA\n+\n!!\n']); // R1 文件里出现 /2
    const {receiver} = await buildReceiver(directory, manifest, files);
    await receiver.receiveRun('http://source.test');
    assert.ok(receiver.badRecords().some(b => b.code === 'END_MISMATCH'));
    assert.deepEqual(receiver.pairs('lane2'), []);
    const gaps = await receiver.gaps();
    assert.ok(gaps.some(g => g.lane === 'lane2' && g.missingEnds.includes(1)));
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('中断恢复：已领取字节与已发布配对在重开后可说明且不重复', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const source = createMockSource(manifest, files, {chunkBytes: 5});

    // 第一次：只领取并处理第一个文件后模拟中断（不调用 receiveRun）。
    const first = await openReceiver(directory, manifest.run, manifest, {
      fetchImpl: source.fetchImpl, syncWrites: false, budgetBytes: 4096,
    });
    openReceivers.push(first);
    await first.claimFile('http://source.test', 'lane1-R1.gz');
    await first.processFile('lane1-R1.gz');
    const progressAfterCrash = first.byteProgress()['lane1-R1.gz'];
    assert.equal(progressAfterCrash.streamDone, true);
    const spoolBefore = await readFile(path.join(directory, 'spool', 'lane1-R1.gz'));
    // 没有可关闭的外部句柄；直接丢弃 first，模拟进程终止。

    // 第二次：重开接收器，继续整轮接收。
    const resumedSource = createMockSource(manifest, files, {chunkBytes: 5});
    const second = await openReceiver(directory, manifest.run, manifest, {
      fetchImpl: resumedSource.fetchImpl, syncWrites: false, budgetBytes: 4096,
    });
    openReceivers.push(second);
    await second.receiveRun('http://source.test');

    // 第一个文件走 Range 续传（或幂等缓存），spool 内容不变。
    const spoolAfter = await readFile(path.join(directory, 'spool', 'lane1-R1.gz'));
    assert.deepEqual(spoolAfter, spoolBefore);
    assert.equal(second.pairs().length, 3, '恢复后仍得到全部三对，无重复');
    const report = await second.completeness();
    assert.equal(report.complete, true);

    // 账本中每个 pairKey 只出现一次。
    const log = await readFile(path.join(directory, 'events.log'), 'utf8');
    const pairEvents = log.split('\n').filter(Boolean).map(JSON.parse).filter(e => e.type === 'pair');
    const keys = pairEvents.map(e => e.payload.pairKey);
    assert.equal(keys.length, new Set(keys).size);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('有界内存：小预算下触发 spill 分块，结果与大内存一致', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const {receiver} = await buildReceiver(directory, manifest, files, {budgetBytes: 512});
    await receiver.receiveRun('http://source.test');
    assert.ok(receiver.staging.spillCount >= 1 || receiver.staging.memoryBytes <= 512);
    assert.equal(receiver.pairs().length, 3);
    assert.deepEqual(await receiver.gaps(), []);
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('三层判断：流未结束时既非合法也不完整', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const source = createMockSource(manifest, files, {chunkBytes: 5});
    const receiver = await openReceiver(directory, manifest.run, manifest, {
      fetchImpl: source.fetchImpl, syncWrites: false,
    });
    openReceivers.push(receiver);
    // 只领取一个文件，其余未开始。
    await receiver.claimFile('http://source.test', 'lane1-R1.gz');
    await receiver.processFile('lane1-R1.gz');
    const report = await receiver.completeness();
    assert.equal(report.streamsDone, false);
    assert.equal(report.complete, false);
    const lane1 = report.lanes.find(l => l.lane === 'lane1');
    assert.ok(lane1.problems.some(p => p.includes('流未结束')));
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('损坏记录导致错位时隔离坏区间并在后续标题重新同步，不污染后续配对', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    // lane2-R1：第一条质量长度不符（坏），第二条合法；坏记录不能让其后错位。
    files['lane2-R1.gz'].gzip = multiMemberGzip([
      '@broken/1\nACGT\n+\n!!\n',
      '@read-a/1\nAA\n+\n!!\n',
    ]);
    const {receiver} = await buildReceiver(directory, manifest, files, {source: {chunkBytes: 3}});
    await receiver.receiveRun('http://source.test');
    const bad = receiver.badRecords();
    assert.equal(bad.length, 1);
    assert.equal(bad[0].code, 'QUALITY_LENGTH_MISMATCH');
    assert.ok(bad[0].byteEnd > bad[0].byteStart);
    // 坏记录之后的 read-a 仍被正常解析并与 R2 配对。
    const pair = receiver.pairs('lane2').find(p => p.read === 'read-a');
    assert.ok(pair, '坏记录之后的合法记录应重新同步并成功配对');
    assert.equal(pair.r1.sequence, 'AA');
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});

test('CRC 损坏 member 的解压内容不进入解析，上报坏尾且不产生配对', async () => {
  const directory = await tempDir();
  try {
    const manifest = standardManifest();
    const files = standardFiles();
    const good = multiMemberGzip(['@read-a/1\nAA\n+\n!!\n']);
    const corrupted = Buffer.from(good);
    corrupted[corrupted.length - 8] ^= 0xff; // 破坏 CRC 首字节
    files['lane2-R1.gz'].gzip = corrupted;
    const {receiver} = await buildReceiver(directory, manifest, files, {source: {chunkBytes: 4}});
    await receiver.receiveRun('http://source.test');
    assert.ok(receiver.badTails().some(t => t.file === 'lane2-R1.gz'));
    assert.deepEqual(receiver.pairs('lane2'), [], '损坏 member 的记录不得配对');
    const gaps = await receiver.gaps();
    assert.ok(gaps.some(g => g.lane === 'lane2' && g.missingEnds.includes(1)));
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});
