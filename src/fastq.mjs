// 流式 FASTQ 记录解析器。
//
// 约定（见 docs/source-contract.md）：
// - 记录由标题行（@ 开头）、一行或多行序列、以 + 开头的分隔行、
//   以及与序列行数对应的一行或多行质量值组成；
// - 质量总长度必须与序列总长度相符；
// - 标题中端标识有两种等价写法：@id/1 与 @id 1:N:0:1（2 同理）；
// - 输入按块喂入，解析器只保留跨行残片与当前记录，内存与单行/单条记录
//   大小相关，不随文件记录数增长。
//
// 一条坏记录不得让后续行永久错位：识别到结构错误时记录坏区间，并以下一个
// 以 @ 开头的标题行重新同步。所有偏移均为“解压后文本”中的绝对字节位置，
// 调用方另外保留压缩 member 起点，两套坐标都可查询。

const END_NOTATION = /^([12])(?::[^\s]*)?$/;

// 解析标题行，返回 {id, end, endNotation, raw}。
// id 不含端标识；end 为 1/2；无法识别端标识时 end 为 null。
export function parseHeader(line) {
  const text = line.toString('utf8');
  if (!text.startsWith('@')) throw new Error('标题行必须以 @ 开头');
  const body = text.slice(1);
  const slash = body.match(/^(.*)\/([12])$/);
  if (slash) {
    return {id: slash[1], end: Number(slash[2]), endNotation: 'slash', raw: text};
  }
  const spaceIndex = body.indexOf(' ');
  if (spaceIndex !== -1) {
    const id = body.slice(0, spaceIndex);
    const match = body.slice(spaceIndex + 1).match(END_NOTATION);
    if (match) {
      return {id, end: Number(match[1]), endNotation: 'casava', raw: text};
    }
  }
  return {id: body, end: null, endNotation: null, raw: text};
}

// 两条标题是否指向同一 read 与同一端。
export function sameReadIdentity(a, b) {
  return a.id === b.id && a.end === b.end;
}

// 逐块喂入的状态机。事件：
//   record {type:'record', header,id,end,endNotation,sequence,quality,start,end,ordinal}
//   bad    {type:'bad', code,message,start,end,lines,ordinal}
export class FastqParser {
  #state = 'header'; // header | sequence | quality
  #header = null;
  #ordinal = null;       // 当前记录的稳定序号
  #counter = 0;          // 已开始的记录计数
  #sequenceLines = [];
  #qualityLines = [];
  #recordStart = 0;
  #carry = Buffer.alloc(0); // 上一块末尾未换行的残片
  #carryOffset = 0;
  #maxLineBytes;

  constructor({maxLineBytes = 8 * 1024 * 1024} = {}) {
    this.#maxLineBytes = maxLineBytes;
  }

  get ordinal() {
    return this.#counter;
  }

  // 喂入一块解压字节，chunkBase 为本块首字节在解压文本中的绝对偏移。
  push(chunk, chunkBase) {
    // 残片与新块在同一坐标系拼接。
    const parts = this.#carry.length ? [this.#carry, chunk] : [chunk];
    const base = this.#carry.length ? this.#carryOffset : chunkBase;
    const data = Buffer.concat(parts);
    this.#carry = Buffer.alloc(0);
    const events = [];
    let lineStart = 0;
    for (let i = 0; i < data.length; i++) {
      if (data[i] !== 0x0a) continue;
      let lineEnd = i;
      if (lineEnd > lineStart && data[lineEnd - 1] === 0x0d) lineEnd--;
      const line = data.subarray(lineStart, lineEnd);
      events.push(...this.#line(line, base + lineStart));
      lineStart = i + 1;
    }
    if (lineStart < data.length) {
      this.#carry = data.subarray(lineStart);
      this.#carryOffset = base + lineStart;
      if (this.#carry.length > this.#maxLineBytes) {
        events.push(this.#bad('LINE_TOO_LONG', '单行超过有界内存上限', this.#carryOffset, this.#carryOffset + this.#carry.length, [this.#carry.subarray(0, 64).toString('utf8')]));
        this.#carry = Buffer.alloc(0);
      }
    }
    return events;
  }

  #line(line, lineOffset) {
    const events = [];
    const text = line.toString('utf8');
    if (this.#state === 'header') {
      if (text.length === 0) return events; // 容忍记录间空行
      if (!text.startsWith('@')) {
        events.push(this.#bad('UNEXPECTED_LINE', '记录外出现非标题行', lineOffset, lineOffset + line.length, [text]));
        return events;
      }
      this.#header = parseHeader(line);
      this.#ordinal = this.#counter++;
      this.#recordStart = lineOffset;
      this.#sequenceLines = [];
      this.#qualityLines = [];
      this.#state = 'sequence';
      return events;
    }
    if (this.#state === 'sequence') {
      if (text.startsWith('+')) {
        if (this.#sequenceLines.length === 0) {
          events.push(this.#bad('EMPTY_SEQUENCE', '缺少序列行', this.#recordStart, lineOffset + line.length, [this.#header?.raw, text]));
          return events;
        }
        this.#state = 'quality';
        return events;
      }
      if (text.startsWith('@')) {
        events.push(this.#bad('MISSING_PLUS', '未遇到 + 分隔行即出现新标题，判定错位并重新同步', this.#recordStart, lineOffset, this.#captureLines()));
        events.push(...this.#line(line, lineOffset)); // 以新标题重新同步
        return events;
      }
      this.#sequenceLines.push(text);
      return events;
    }
    // quality
    this.#qualityLines.push(text);
    if (this.#qualityLines.length === this.#sequenceLines.length) {
      events.push(...this.#emitRecord(lineOffset + line.length));
      this.#state = 'header';
    }
    return events;
  }

  #captureLines() {
    return [this.#header?.raw, ...this.#sequenceLines, ...this.#qualityLines].filter(Boolean);
  }

  #emitRecord(endOffset) {
    const sequence = this.#sequenceLines.join('');
    const quality = this.#qualityLines.join('');
    if (sequence.length !== quality.length) {
      return [this.#bad('QUALITY_LENGTH_MISMATCH', `序列长度 ${sequence.length} 与质量长度 ${quality.length} 不符`, this.#recordStart, endOffset, this.#captureLines())];
    }
    return [{
      type: 'record',
      header: this.#header.raw,
      id: this.#header.id,
      end: this.#header.end,
      endNotation: this.#header.endNotation,
      sequence,
      quality,
      byteStart: this.#recordStart,
      byteEnd: endOffset,
      ordinal: this.#ordinal,
    }];
  }

  #bad(code, message, start, end, lines) {
    const event = {type: 'bad', code, message, byteStart: start, byteEnd: end, lines, ordinal: this.#ordinal};
    this.#state = 'header';
    this.#header = null;
    this.#ordinal = null;
    this.#sequenceLines = [];
    this.#qualityLines = [];
    return event;
  }

  // 流结束：未闭合记录按坏记录上报，而不是静默丢弃。
  finish() {
    const events = [];
    if (this.#carry.length) {
      const line = this.#carry;
      events.push(...this.#line(line, this.#carryOffset));
      this.#carry = Buffer.alloc(0);
    }
    if (this.#state !== 'header' || this.#header !== null) {
      events.push(this.#bad('UNTERMINATED_RECORD', '流结束时记录未闭合', this.#recordStart, this.#carryOffset + this.#carry.length, this.#captureLines()));
    }
    return events;
  }
}

// 便捷函数：解析一整段文本（主要供测试使用）。
export function parseFastq(buffer, baseOffset = 0) {
  const parser = new FastqParser();
  const events = [];
  for (let offset = 0; offset < buffer.length;) {
    const size = Math.min(64, buffer.length - offset);
    events.push(...parser.push(buffer.subarray(offset, offset + size), offset + baseOffset));
    offset += size;
  }
  events.push(...parser.finish());
  return events;
}
