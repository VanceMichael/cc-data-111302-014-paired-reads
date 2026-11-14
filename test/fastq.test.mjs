import test from 'node:test';
import assert from 'node:assert/strict';
import {parseHeader, sameReadIdentity, FastqParser, parseFastq} from '../src/fastq.mjs';

test('两种端标识写法被等价解析', () => {
  const slash = parseHeader(Buffer.from('@read-a/1'));
  const casava = parseHeader(Buffer.from('@read-a 1:N:0:1'));
  assert.deepEqual([slash.id, slash.end, slash.endNotation], ['read-a', 1, 'slash']);
  assert.deepEqual([casava.id, casava.end, casava.endNotation], ['read-a', 1, 'casava']);
  assert.ok(sameReadIdentity(slash, casava));
  const r2 = parseHeader(Buffer.from('@read-b 2:N:0:1'));
  assert.equal(r2.end, 2);
  assert.equal(parseHeader(Buffer.from('@no-end')).end, null);
});

test('多行序列与多行质量、长度相符', () => {
  const text = '@r/1\nAC\nGT\n+\n!!\n!!\n';
  const events = parseFastq(Buffer.from(text));
  const record = events.find(e => e.type === 'record');
  assert.ok(record);
  assert.equal(record.sequence, 'ACGT');
  assert.equal(record.quality, '!!!!');
  assert.equal(record.byteStart, 0);
  assert.equal(record.byteEnd, text.length - 1);
});

test('质量长度不符报坏记录且后续记录可重新同步', () => {
  const text = '@r/1\nACGT\n+\n!!\n@s/1\nA\n+\n!\n';
  const events = parseFastq(Buffer.from(text));
  const bad = events.find(e => e.type === 'bad');
  assert.equal(bad.code, 'QUALITY_LENGTH_MISMATCH');
  const records = events.filter(e => e.type === 'record');
  assert.equal(records.length, 1);
  assert.equal(records[0].id, 's');
});

test('错位（缺少 +）被拦截并在下一个标题重新同步', () => {
  const text = '@r/1\nACGT\n@@garbage\n@s/1\nA\n+\n!\n';
  const events = parseFastq(Buffer.from(text));
  assert.ok(events.some(e => e.type === 'bad' && e.code === 'MISSING_PLUS'));
  assert.ok(events.some(e => e.type === 'record' && e.id === 's'));
});

test('跨块残片的偏移仍是解压文本绝对坐标', () => {
  const text = '@r/1\nACGT\n+\n!!!!\n';
  const parser = new FastqParser();
  const events = [];
  // 1 字节一块，强制所有行都跨块。
  for (let i = 0; i < text.length; i++) events.push(...parser.push(Buffer.from(text[i], 'utf8'), i));
  events.push(...parser.finish());
  const record = events.find(e => e.type === 'record');
  assert.equal(record.sequence, 'ACGT');
  assert.equal(record.byteStart, 0);
  assert.equal(record.byteEnd, text.length - 1);
});

test('未闭合记录在流结束时报坏记录', () => {
  const events = parseFastq(Buffer.from('@r/1\nAC\n+\n'));
  assert.ok(events.some(e => e.type === 'bad' && e.code === 'UNTERMINATED_RECORD'));
});

test('ordinal 对合法与坏记录都稳定且唯一', () => {
  const text = '@a/1\nA\n+\n!\n@b/1\nAC\n+\n!\n@c/1\nC\n+\n#\n';
  const events = parseFastq(Buffer.from(text));
  const ordinals = events.map(e => e.ordinal);
  assert.deepEqual([...new Set(ordinals)].sort((a, b) => a - b), [0, 1, 2]);
});
