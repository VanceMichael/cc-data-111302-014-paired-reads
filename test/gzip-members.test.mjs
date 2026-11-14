import test from 'node:test';
import assert from 'node:assert/strict';
import {gzipSync} from 'node:zlib';
import {GzipMemberReader, headerEnd, bufferChunks} from '../src/gzip-members.mjs';

async function readByChunks(buffer, chunkSize, options) {
  const reader = new GzipMemberReader(options);
  const structural = [];
  let offset = 0;
  for (const chunk of bufferChunks(buffer, chunkSize)) {
    const events = await reader.push(chunk, offset);
    offset += chunk.length;
    structural.push(...events);
  }
  const tail = await reader.finish();
  if (tail) structural.push(tail);
  return {structural, bufferedBytes: reader.bufferedBytes};
}

test('headerEnd 解析固定头部与边界', () => {
  const member = gzipSync('abc');
  assert.equal(headerEnd(member, 0), 10);
  assert.equal(headerEnd(member.subarray(0, 9), 0), null);
  const badMagic = Buffer.from([0, 0, 8, 0, 0, 0, 0, 0, 0, 0]);
  assert.throws(() => headerEnd(badMagic, 0));
});

test('多 member 拼接被逐一切分且 CRC/ISIZE 正确', async () => {
  const m1 = gzipSync('hello world '.repeat(50));
  const m2 = gzipSync('second member '.repeat(30));
  const both = Buffer.concat([m1, m2]);
  // 用比 member 小得多的块投递，验证跨块缓冲与线性切分。
  const {structural} = await readByChunks(both, 7);
  const memberEvents = structural.filter(e => e.type === 'member');
  assert.equal(memberEvents.length, 2);
  assert.deepEqual(memberEvents.map(e => e.ordinal), [0, 1]);
  assert.equal(memberEvents[0].end, m1.length);
  assert.equal(memberEvents[1].start, m1.length);
  assert.equal(memberEvents[1].end, both.length);
  assert.ok(memberEvents.every(e => e.crcOk && e.sizeOk));
  const output = Buffer.concat(memberEvents[0].parts).toString();
  assert.equal(output, 'hello world '.repeat(50));
});

test('坏压缩尾：截断 8 字节产生 truncated 并抢救输出', async () => {
  const member = gzipSync('salvageable '.repeat(20));
  const truncated = member.subarray(0, member.length - 8);
  const {structural} = await readByChunks(truncated, 5);
  const tail = structural.find(e => e.type === 'truncated');
  assert.ok(tail, '应识别截断');
  assert.equal(tail.salvagedBytes, 'salvageable '.length * 20);
  assert.ok(tail.parts.length >= 1 && tail.salvagedBytes > 0);
});

test('member 边界在投递块正中间时缓冲被正确消费', async () => {
  const m1 = gzipSync('A'.repeat(100));
  const m2 = gzipSync('B'.repeat(100));
  const both = Buffer.concat([m1, m2]);
  const cut = m1.length - 3; // 第一块切在第一个 member 尾部中间
  const reader = new GzipMemberReader();
  const first = await reader.push(both.subarray(0, cut), 0);
  assert.equal(first.length, 0, '边界未到齐不应产生事件');
  assert.equal(reader.bufferedBytes, cut);
  const second = await reader.push(both.subarray(cut), cut);
  const memberEvents = second.filter(e => e.type === 'member');
  assert.equal(memberEvents.length, 2);
  assert.equal(reader.bufferedBytes, 0);
  const out = Buffer.concat(memberEvents.flatMap(e => e.parts)).toString();
  assert.equal(out, 'A'.repeat(100) + 'B'.repeat(100));
});

test('垃圾字节段被识别且不影响后续 member', async () => {
  const member = gzipSync('after garbage');
  const mixed = Buffer.concat([Buffer.from([1, 2, 3, 4]), member]);
  const {structural} = await readByChunks(mixed, 16);
  assert.ok(structural.some(e => e.type === 'garbage'));
  assert.ok(structural.some(e => e.type === 'member' && e.crcOk));
});

test('CRC 被篡改时 crcOk=false 但输出块仍随事件提供，由调用方决定拒收', async () => {
  const member = Buffer.from(gzipSync('@r/1\nAC\n+\n!!\n'));
  const corrupted = Buffer.from(member);
  // 篡改 CRC 尾的第一个字节（倒数第 8 个），deflate 仍可解压但校验失败。
  corrupted[corrupted.length - 8] ^= 0xff;
  const {structural} = await readByChunks(corrupted, 9);
  const memberEvent = structural.find(e => e.type === 'member');
  assert.ok(memberEvent, '仍按完整 member 切分（尾部结构在）');
  assert.equal(memberEvent.crcOk, false);
  assert.ok(memberEvent.parts.length >= 1, '解压输出块仍可取得');
});
