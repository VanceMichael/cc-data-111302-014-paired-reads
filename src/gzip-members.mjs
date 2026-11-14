// 有界内存的 gzip member 流式读取器（RFC1952）。
//
// 背景与边界：
// - 测序交付的 .gz 常由多个 gzip member 拼接，member 是天然的校验与恢复点；
// - Node 的 zlib 只在 end() 后才保证处理完喂入字节，无法用单个长生命周期
//   inflater 跨 member 连续解析。本读取器对“当前 member 头部到缓冲末尾”的
//   字节在每次缓冲增长时运行一次独立 inflater：
//     * deflate 尚未结束 -> Z_BUF_ERROR，丢弃该 inflater，等后续字节；
//     * deflate 已结束   -> 'end'，该 inflater 的输出即 member 完整解压内容，
//       作为 parts 挂在结构事件上，并据 bytesWritten 线性定位尾部边界。
//   调用方在收到事件、确认 CRC32/ISIZE 后才消费 parts，因此“先校验、后解析”
//   天然成立；读取器不缓存历史 member 的输出。内存上限是单个未完成 member 的
//   压缩字节，不随文件总长度或运行记录数增长。投递块越大，同一 member 的
//   重复解压次数越少（默认 64KiB 块时近似线性）。
//
// 事件（offset 均为相对整个压缩文件的绝对字节位置）：
//   member    {type:'member', ordinal, start, end, crcOk, sizeOk, outputBytes, parts}
//   truncated {type:'truncated', ordinal, start, end, salvagedBytes, reason, parts}
//   garbage   {type:'garbage', start, end, reason}
import {createInflateRaw, crc32} from 'node:zlib';

export const GZIP_ID1 = 0x1f;
export const GZIP_ID2 = 0x8b;
const FEXTRA = 4;
const FNAME = 8;
const FCOMMENT = 16;
const FHCRC = 2;
const TRAILER_BYTES = 8;
const FIXED_HEADER = 10;

// 计算 gzip 头部结束偏移；头部不完整返回 null；魔数错误抛错。
export function headerEnd(buffer, start = 0) {
  if (buffer.length - start < FIXED_HEADER) return null;
  if (buffer[start] !== GZIP_ID1 || buffer[start + 1] !== GZIP_ID2) {
    throw new Error(`gzip 魔数错误（偏移 ${start}）`);
  }
  const flg = buffer[start + 3];
  let p = start + FIXED_HEADER;
  if (flg & FEXTRA) {
    if (buffer.length < p + 2) return null;
    p += 2 + buffer.readUInt16LE(p);
  }
  if (flg & FNAME) {
    while (p < buffer.length && buffer[p] !== 0) p++;
    if (p >= buffer.length) return null;
    p++;
  }
  if (flg & FCOMMENT) {
    while (p < buffer.length && buffer[p] !== 0) p++;
    if (p >= buffer.length) return null;
    p++;
  }
  if (flg & FHCRC) p += 2;
  return p > buffer.length ? null : p;
}

// 对一段候选字节运行一次完整 inflater，返回结果与全部输出块。
function inflateOnce(candidate) {
  return new Promise(resolve => {
    const inflater = createInflateRaw();
    const parts = [];
    let rollingCrc;
    inflater.on('data', part => {
      parts.push(part);
      rollingCrc = crc32(part, rollingCrc);
    });
    inflater.once('end', () => {
      resolve({done: true, parts, rollingCrc, consumed: inflater.bytesWritten});
      inflater.destroy();
    });
    inflater.once('error', error => {
      resolve({done: false, parts, rollingCrc, error, consumed: inflater.bytesWritten});
      inflater.destroy();
    });
    inflater.end(candidate);
  });
}

function partsLength(parts) {
  return parts.reduce((sum, part) => sum + part.length, 0);
}

export class GzipMemberReader {
  #chunks = [];      // 尚未消费的压缩字节
  #length = 0;
  #base = 0;         // #chunks 首字节的绝对偏移
  #ordinal = -1;
  #maxMemberBytes;

  constructor({maxMemberBytes = 512 * 1024 * 1024} = {}) {
    this.#maxMemberBytes = maxMemberBytes;
  }

  get bufferedBytes() {
    return this.#length;
  }

  #merged() {
    const merged = Buffer.concat(this.#chunks, this.#length);
    this.#chunks = [merged];
    return merged;
  }

  #consume(bytes) {
    let remaining = bytes;
    while (remaining > 0) {
      const head = this.#chunks[0];
      if (head.length <= remaining) {
        remaining -= head.length;
        this.#chunks.shift();
      } else {
        this.#chunks[0] = head.subarray(remaining);
        remaining = 0;
      }
    }
    this.#length -= bytes;
    this.#base += bytes;
  }

  // 喂入一块压缩字节，base 为本块首字节绝对偏移。
  // 返回本次识别出的结构事件；member/truncated 事件的 parts 携带解压输出块。
  async push(chunk, base) {
    if (!Buffer.isBuffer(chunk) || chunk.length === 0) return [];
    if (this.#length > 0 && base !== this.#base + this.#length) {
      throw new Error(`压缩字节不连续：期望偏移 ${this.#base + this.#length}，收到 ${base}`);
    }
    if (this.#length === 0) this.#base = base;
    this.#chunks.push(chunk);
    this.#length += chunk.length;
    const events = [];
    for (;;) {
      const buffer = this.#merged();
      if (buffer.length < FIXED_HEADER) break;
      if (buffer[0] !== GZIP_ID1 || buffer[1] !== GZIP_ID2) {
        let next = 2;
        while (next < buffer.length - 1 && !(buffer[next] === GZIP_ID1 && buffer[next + 1] === GZIP_ID2)) next++;
        const garbageEnd = next >= buffer.length - 1 ? buffer.length : next;
        events.push({type: 'garbage', start: this.#base, end: this.#base + garbageEnd, reason: 'gzip 魔数错误的垃圾字节'});
        this.#consume(garbageEnd);
        continue;
      }
      const hEnd = headerEnd(buffer, 0);
      if (hEnd === null) break; // 头部跨块未到齐
      if (buffer.length - hEnd > this.#maxMemberBytes) {
        throw new Error(`单个 gzip member 超过有界上限 ${this.#maxMemberBytes} 字节`);
      }
      const ordinal = this.#ordinal + 1;
      const result = await inflateOnce(buffer.subarray(hEnd));
      if (!result.done) {
        // Z_BUF_ERROR 表示数据不足，等待后续字节；其它错误码说明 deflate 损坏，
        // 上报 truncated 并把已抢救输出块随事件交给调用方决定是否解析。
        if (result.error?.code === 'Z_BUF_ERROR') break;
        this.#ordinal = ordinal;
        events.push({
          type: 'truncated', ordinal, start: this.#base, end: this.#base + buffer.length,
          salvagedBytes: partsLength(result.parts), reason: `deflate 数据损坏：${result.error?.code}`,
          parts: result.parts,
        });
        this.#consume(buffer.length);
        break;
      }
      const memberLength = hEnd + result.consumed + TRAILER_BYTES;
      if (memberLength > buffer.length) break; // 尾部尚未到齐，等后续字节
      this.#ordinal = ordinal;
      const storedCrc = buffer.readUInt32LE(memberLength - 8);
      const storedSize = buffer.readUInt32LE(memberLength - 4);
      const outputBytes = partsLength(result.parts);
      events.push({
        type: 'member', ordinal, start: this.#base, end: this.#base + memberLength,
        crcOk: result.rollingCrc === storedCrc,
        sizeOk: (outputBytes >>> 0) === storedSize,
        outputBytes,
        parts: result.parts,
      });
      this.#consume(memberLength);
    }
    return events;
  }

  // 流结束：缓冲中仍有字节意味着最后一个 member 不完整（坏压缩尾）。
  async finish() {
    if (this.#length === 0) return null;
    const buffer = this.#merged();
    const start = this.#base;
    let hEnd;
    try {
      hEnd = headerEnd(buffer, 0);
    } catch {
      const event = {type: 'garbage', start, end: start + buffer.length, reason: 'gzip 魔数错误'};
      this.#consume(buffer.length);
      return event;
    }
    const ordinal = this.#ordinal + 1;
    if (hEnd === null) {
      const event = {
        type: 'truncated', ordinal, start, end: start + buffer.length,
        salvagedBytes: 0, reason: 'gzip 头部不完整即结束', parts: [],
      };
      this.#consume(buffer.length);
      return event;
    }
    const result = await inflateOnce(buffer.subarray(hEnd));
    const reason = result.done
      ? 'gzip 尾部（CRC32/ISIZE）缺失或 member 被截断'
      : `deflate 在流结束前截断（${result.error?.code}），仅抢救完整输出块`;
    const event = {
      type: 'truncated', ordinal, start, end: start + buffer.length,
      salvagedBytes: partsLength(result.parts), reason, parts: result.parts,
    };
    this.#consume(buffer.length);
    this.#ordinal = ordinal;
    return event;
  }
}

// 便捷封装：压缩字节异步可迭代 -> 结构事件异步可迭代（parts 随事件携带）。
export async function* members(compressedChunks, options = {}) {
  const reader = new GzipMemberReader(options);
  let offset = 0;
  for await (const chunk of compressedChunks) {
    const events = await reader.push(chunk, offset);
    offset += chunk.length;
    for (const event of events) yield event;
  }
  const tail = await reader.finish();
  if (tail) yield tail;
}

export function* chunkBuffer(buffer, size) {
  for (let offset = 0; offset < buffer.length; offset += size) {
    yield buffer.subarray(offset, offset + size);
  }
}

export function bufferChunks(buffer, size) {
  return Array.from(chunkBuffer(buffer, size));
}
