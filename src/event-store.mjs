// 只追加事件账本与状态检查点。
//
// 接收过程中的每一个不可逆事实都先写 events.log 并 fsync，再更新内存状态：
// 字节领取、坏压缩尾、坏记录、单端到达、重传、冲突隔离、配对发布。
// 进程中断后，重放 events.log 即可重建全部可查询状态；检查点 state.json
// 只是重放加速缓存，丢失也不影响正确性。
//
// 账本按“目标目录 + 运行”隔离，同一运行重复写入是幂等的：事件携带
// (file, ordinal) 或 pairKey 作为去重键，重放与重传不会重复计数。
import {open, mkdir, readFile, rename} from 'node:fs/promises';
import {createWriteStream} from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';

export const EVENT_TYPES = Object.freeze({
  CLAIM: 'claim',                 // 某端文件的一段压缩字节已持久化到 spool
  STREAM_DONE: 'stream-done',     // 某端文件的 HTTP 流正常结束
  BAD_TAIL: 'bad-tail',           // 最后一个 gzip member 截断/损坏
  BAD_RECORD: 'bad-record',       // FASTQ 结构性坏记录及其字节范围
  RECORD: 'record',               // 一条合法记录被解析出来（单端到达）
  RETRANSMIT: 'retransmit',       // 同身份同端同内容重复出现
  CONFLICT: 'conflict',           // 同身份同端不同内容，隔离
  PAIR: 'pair',                   // 一对双端记录发布
  QUARANTINE_NOTE: 'quarantine-note',
});

// 内容寻址：记录正文（标题/序列/质量）哈希，作为重传与冲突判定依据。
export function fingerprint(record) {
  const hash = createHash('sha256');
  hash.update(record.header);
  hash.update('\u0000');
  hash.update(record.sequence);
  hash.update('\u0000');
  hash.update(record.quality);
  return hash.digest('hex');
}

export function pairKey(runId, laneId, readId) {
  return `${runId}\u0000${laneId}\u0000${readId}`;
}

export function parsePairKey(key) {
  const [runId, laneId, readId] = key.split('\u0000');
  return {runId, laneId, readId};
}

export class EventStore {
  #directory;
  #runId;
  #writeStream = null;
  #fileHandle = null;
  #sequence = 0;
  #seen = new Set(); // 幂等键
  #syncWrites;

  constructor(directory, runId, {syncWrites = true} = {}) {
    this.#directory = directory;
    this.#runId = runId;
    this.#syncWrites = syncWrites;
  }

  get runId() {
    return this.#runId;
  }

  get directory() {
    return this.#directory;
  }

  async open() {
    await mkdir(path.join(this.#directory, 'spool'), {recursive: true});
    await mkdir(path.join(this.#directory, 'spill'), {recursive: true});
    const logPath = this.logPath;
    let exists = false;
    try {
      await readFile(logPath);
      exists = true;
    } catch {}
    // WriteStream 自行打开并在 end() 时关闭 fd；FileHandle 仅用于 fsync，
    // 因此两者各自独立，避免重复关闭同一描述符导致 EBADF。
    this.#fileHandle = await open(logPath, 'a');
    this.#writeStream = createWriteStream(logPath, {flags: 'a'});
    if (exists) {
      // 重放已有账本，恢复 sequence 与幂等集合。
      const text = await readFile(logPath, 'utf8');
      const lines = text.split('\n').filter(Boolean);
      for (const line of lines) {
        const event = JSON.parse(line);
        this.#sequence = Math.max(this.#sequence, event.seq);
        if (event.idempotencyKey) this.#seen.add(event.idempotencyKey);
      }
    }
    return this;
  }

  get logPath() {
    return path.join(this.#directory, 'events.log');
  }

  get checkpointPath() {
    return path.join(this.#directory, 'state.json');
  }

  #idempotencyKey(type, payload) {
    switch (type) {
      // 同一文件同一记录序号只允许产生一个到达类事件（record/retransmit/
      // conflict/bad-record 互斥），恢复重放或重试不会产生第二条。
      case EVENT_TYPES.RECORD:
      case EVENT_TYPES.RETRANSMIT:
      case EVENT_TYPES.CONFLICT:
      case EVENT_TYPES.BAD_RECORD:
        return `record:${payload.file}:${payload.ordinal}`;
      case EVENT_TYPES.PAIR:
        return `${EVENT_TYPES.PAIR}:${payload.pairKey}`;
      case EVENT_TYPES.CLAIM:
        return `${EVENT_TYPES.CLAIM}:${payload.file}:${payload.start}`;
      // 同一文件可能有多个坏 member/垃圾段，用字节起点区分。
      case EVENT_TYPES.BAD_TAIL:
        return `${EVENT_TYPES.BAD_TAIL}:${payload.file}:${payload.start}`;
      case EVENT_TYPES.STREAM_DONE:
        return `${EVENT_TYPES.STREAM_DONE}:${payload.file}`;
      default:
        return null;
    }
  }

  // 追加一个事件；重复的幂等事件被忽略并返回 null。
  async append(type, payload) {
    const idempotencyKey = this.#idempotencyKey(type, payload);
    if (idempotencyKey && this.#seen.has(idempotencyKey)) return null;
    const event = {seq: ++this.#sequence, at: new Date().toISOString(), run: this.#runId, type, payload};
    if (idempotencyKey) {
      event.idempotencyKey = idempotencyKey;
      this.#seen.add(idempotencyKey);
    }
    await new Promise((resolve, reject) => {
      this.#writeStream.write(JSON.stringify(event) + '\n', error => (error ? reject(error) : resolve()));
    });
    if (this.#syncWrites) {
      await new Promise((resolve, reject) => this.#fileHandle.sync(error => (error ? reject(error) : resolve())));
    }
    return event;
  }

  // 写检查点（原子替换）。检查点是缓存：损坏或缺失时退回全量重放。
  async checkpoint(state) {
    const tmp = this.checkpointPath + '.tmp';
    const handle = await open(tmp, 'w');
    await handle.writeFile(JSON.stringify(state, null, 2));
    await handle.sync();
    await handle.close();
    await rename(tmp, this.checkpointPath);
  }

  // 全量读取事件（重建状态用）。
  async *readEvents() {
    let text;
    try {
      text = await readFile(this.logPath, 'utf8');
    } catch {
      return;
    }
    for (const line of text.split('\n')) {
      if (line) yield JSON.parse(line);
    }
  }

  async close() {
    if (!this.#writeStream) return;
    await new Promise(resolve => this.#writeStream.end(resolve));
    await this.#fileHandle.close();
    this.#writeStream = null;
    this.#fileHandle = null;
  }

  pathFor(subDirectory, name) {
    return path.join(this.#directory, subDirectory, name);
  }
}

// 重放账本，产出完整状态对象。投影逻辑集中在此处，内存状态只是它的缓存。
export function replay(events) {
  const state = createEmptyState();
  for (const event of events) applyEvent(state, event);
  return state;
}

export function createEmptyState() {
  return {
    files: new Map(),       // file -> {claimed: [{start,end,spool}], streamDone, badTail}
    records: new Map(),     // `${file}#${ordinal}` -> 记录事件
    badRecords: [],         // 坏记录（含字节范围）
    arrivals: new Map(),    // 未配对身份：pairKey -> {1: entry, 2: entry}
    arrivalCount: new Map(),// 每文件首次到达计数（重传不增加，配对也不清零）
    conflicts: [],          // 隔离项
    pairs: new Map(),       // pairKey -> 配对发布内容
    pairOrder: [],
    retransmits: [],
  };
}

export function applyEvent(state, event) {
  const {type, payload} = event;
  switch (type) {
    case EVENT_TYPES.CLAIM: {
      const entry = state.files.get(payload.file) ?? {claims: [], streamDone: false, badTail: null};
      entry.claims.push({start: payload.start, end: payload.end, spool: payload.spool, seq: event.seq});
      state.files.set(payload.file, entry);
      break;
    }
    case EVENT_TYPES.STREAM_DONE: {
      const entry = state.files.get(payload.file) ?? {claims: [], streamDone: false, badTail: null};
      entry.streamDone = true;
      entry.totalBytes = payload.totalBytes;
      state.files.set(payload.file, entry);
      break;
    }
    case EVENT_TYPES.BAD_TAIL: {
      const entry = state.files.get(payload.file) ?? {claims: [], streamDone: false, badTail: null};
      entry.badTail = payload;
      state.files.set(payload.file, entry);
      break;
    }
    case EVENT_TYPES.BAD_RECORD:
      state.badRecords.push(payload);
      break;
    case EVENT_TYPES.RECORD:
    case EVENT_TYPES.RETRANSMIT:
    case EVENT_TYPES.CONFLICT: {
      state.records.set(`${payload.file}#${payload.ordinal}`, payload);
      if (type === EVENT_TYPES.RECORD || type === EVENT_TYPES.RETRANSMIT) {
        const bucket = state.arrivals.get(payload.pairKey) ?? {};
        bucket[payload.end] = {
          file: payload.file,
          lane: payload.lane,
          ordinal: payload.ordinal,
          fingerprint: payload.fingerprint,
          header: payload.header,
          sequence: payload.sequence,
          quality: payload.quality,
          memberStart: payload.memberStart,
          byteStart: payload.byteStart,
          byteEnd: payload.byteEnd,
        };
        state.arrivals.set(payload.pairKey, bucket);
        if (type === EVENT_TYPES.RECORD) {
          state.arrivalCount.set(payload.file, (state.arrivalCount.get(payload.file) ?? 0) + 1);
        } else {
          state.retransmits.push(payload);
        }
      } else {
        state.conflicts.push(payload);
      }
      break;
    }
    case EVENT_TYPES.PAIR: {
      if (!state.pairs.has(payload.pairKey)) {
        state.pairs.set(payload.pairKey, payload);
        state.pairOrder.push(payload.pairKey);
        // 配对发布后该身份离开待配索引，arrivals 仅保留未配对的到达。
        state.arrivals.delete(payload.pairKey);
      }
      break;
    }
    default:
      break;
  }
  return state;
}
