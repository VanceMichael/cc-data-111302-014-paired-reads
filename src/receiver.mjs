// 双端接收编排器：字节领取 -> 持久 spool -> gzip member 流式切分 ->
// FASTQ 解析 -> 有界暂存 -> 双端配对 -> 查询与完整性判断。
//
// 三层判断严格区分：
//   1. 流结束（HTTP 传输层）：streamDone，只说明字节传完；
//   2. 记录都合法：badRecords 为空且没有 badTail；
//   3. 整个运行完整：每个应到 lane 的每端都流结束、合法，且实际配对数
//      与交付清单（manifest）声明的每端计数一致，没有未配对缺口。
//
// 中断恢复：已领取字节以 claim 事件入账并落在 spool 文件，重连带 Range
// 续传；已处理记录以 (file, ordinal) 幂等跳过；已发布配对以 pairKey 幂等。
// 恢复后查询接口仍能准确说明字节处理进度与配对发布情况。
import {open, mkdir} from 'node:fs/promises';
import path from 'node:path';
import {createWriteStream} from 'node:fs';
import {GzipMemberReader} from './gzip-members.mjs';
import {FastqParser} from './fastq.mjs';
import {
  EventStore,
  EVENT_TYPES,
  fingerprint,
  pairKey,
  parsePairKey,
  createEmptyState,
  applyEvent,
} from './event-store.mjs';
import {SpillableStaging} from './staging.mjs';

const DEFAULT_BUDGET = 256 * 1024;
const DEFAULT_MAX_MEMBER_BYTES = 512 * 1024 * 1024;

export class Receiver {
  #store;
  #state;
  #staging;
  #manifest;
  #options;
  #seenOrdinals = new Map(); // file -> Set<ordinal>，恢复后跳过已入账记录
  #processedFiles = new Set(); // 已完整处理过的文件，恢复后不再重放解析

  constructor(store, manifest, options = {}) {
    this.#store = store;
    this.#manifest = manifest;
    this.#options = {
      budgetBytes: options.budgetBytes ?? DEFAULT_BUDGET,
      maxMemberBytes: options.maxMemberBytes ?? DEFAULT_MAX_MEMBER_BYTES,
      ...options,
      // 统一注入点：调用方用 fetchImpl，兼容旧名 fetch。
      fetchImpl: options.fetchImpl ?? options.fetch ?? fetch,
    };
    this.#state = createEmptyState();
  }

  // 从账本重建全部内存状态、暂存表与已处理 ordinal 集合。
  static async recover(store, manifest, options = {}) {
    const receiver = new Receiver(store, manifest, options);
    for await (const event of store.readEvents()) {
      applyEvent(receiver.#state, event);
      const p = event.payload;
      if (p && typeof p.ordinal === 'number' && p.file) {
        receiver.#ordinalSet(p.file).add(p.ordinal);
      }
      // 只要某文件已产生过解析类事件，就视为该文件已完整处理过：
      // 恢复后不再重放解析（账本已含其全部事实），避免破坏暂存配对。
      if (p?.file && [EVENT_TYPES.RECORD, EVENT_TYPES.RETRANSMIT, EVENT_TYPES.CONFLICT, EVENT_TYPES.BAD_RECORD].includes(event.type)) {
        receiver.#processedFiles.add(p.file);
      }
    }
    receiver.#staging = new SpillableStaging({
      directory: store.pathFor('spill', ''),
      budgetBytes: receiver.#options.budgetBytes,
      namePrefix: `staging-${manifest.run}`,
    });
    // 用账本中的到达事件重建暂存，已配对的身份不再放入。
    for (const [key, bucket] of receiver.#state.arrivals) {
      if (!receiver.#state.pairs.has(key)) await receiver.#staging.put(key, bucket);
    }
    return receiver;
  }

  #ordinalSet(file) {
    let set = this.#seenOrdinals.get(file);
    if (!set) {
      set = new Set();
      this.#seenOrdinals.set(file, set);
    }
    return set;
  }

  get state() {
    return this.#state;
  }

  get staging() {
    return this.#staging;
  }

  // 关闭底层事件账本句柄；状态仍可查询直到对象被回收。
  async close() {
    await this.#store.close();
  }

  // 交付清单索引：文件名 -> {lane, end, expectedCount}。
  #fileIndex() {
    const index = new Map();
    for (const lane of this.#manifest.lanes) {
      index.set(lane.r1, {lane: lane.id, end: 1, expectedCount: lane.count});
      index.set(lane.r2, {lane: lane.id, end: 2, expectedCount: lane.count});
    }
    return index;
  }

  #spoolPath(file) {
    return this.#store.pathFor('spool', file.replace(/[^A-Za-z0-9._-]/g, '_'));
  }

  // spool 文件大小即已落盘的连续领取进度。
  async #claimedBytes(file) {
    try {
      const handle = await open(this.#spoolPath(file), 'r');
      const stat = await handle.stat();
      await handle.close();
      return stat.size;
    } catch {
      return 0;
    }
  }

  // 追加事件并同步投影内存状态；返回 null 表示命中幂等被忽略。
  async #record(type, payload) {
    const event = await this.#store.append(type, payload);
    if (event) applyEvent(this.#state, event);
    return event;
  }

  // 带 Range 的分块领取：远端文件续传到本地 spool，逐块记 claim 事件。
  async claimFile(baseUrl, file) {
    const fetchImpl = this.#options.fetchImpl;
    const spoolPath = this.#spoolPath(file);
    await mkdir(path.dirname(spoolPath), {recursive: true});
    const already = await this.#claimedBytes(file);
    const headers = already > 0 ? {Range: `bytes=${already}-`} : {};
    const response = await fetchImpl(`${baseUrl}/files/${file}`, {
      headers,
      signal: AbortSignal.timeout(30000),
    });
    let offset = already;
    let resumed = already > 0;
    if (already > 0 && response.status === 200) {
      // 来源不支持 Range：整文件重下，claim 幂等保证不重复入账。
      offset = 0;
      resumed = false;
    } else if (already > 0 && response.status !== 206) {
      throw new Error(`续传失败：已领取 ${already} 字节，来源响应 ${response.status}`);
    }
    if (!response.ok && response.status !== 206) throw new Error(`领取失败 ${file}: ${response.status}`);
    const rangeHeader = response.headers.get('content-range');
    const totalBytes = rangeHeader ? Number(rangeHeader.split('/')[1]) : null;

    const writer = createWriteStream(spoolPath, {flags: offset === 0 ? 'w' : 'a'});
    for await (const chunk of response.body) {
      const start = offset;
      await new Promise((resolve, reject) => {
        writer.write(chunk, error => (error ? reject(error) : resolve()));
      });
      offset += chunk.length;
      await this.#record(EVENT_TYPES.CLAIM, {file, start, end: offset, spool: path.basename(spoolPath)});
    }
    await new Promise((resolve, reject) => {
      writer.end(error => (error ? reject(error) : resolve()));
    });
    await this.#record(EVENT_TYPES.STREAM_DONE, {file, totalBytes: totalBytes ?? offset, spoolBytes: offset});
    return {totalBytes: totalBytes ?? offset, resumed, spoolPath};
  }

  // 解析一个已领取的 spool 文件。整个管线流式：FileHandle -> member 读取器
  // -> （校验通过后）解压块 -> FASTQ 解析器 -> 分发。任何一层都不持有整个文件。
  async processFile(file) {
    const info = this.#fileIndex().get(file);
    if (!info) throw new Error(`文件 ${file} 不在交付清单中，拒绝处理`);
    const handle = await open(this.#spoolPath(file), 'r');
    const memberReader = new GzipMemberReader({maxMemberBytes: this.#options.maxMemberBytes});
    const parser = new FastqParser();
    let currentMemberStart = 0;
    let decompressedOffset = 0; // 解压文本中的绝对字节位置

    // 分发必须严格串行：同一批可能含多条记录，后续记录要看到前一条写入的
    // 暂存内容（重传/冲突判定依赖它），因此用 Promise 链顺序执行。
    let dispatchChain = Promise.resolve();
    const feedParts = (parts, memberStart) => {
      for (const part of parts) {
        for (const event of parser.push(part, decompressedOffset)) {
          dispatchChain = dispatchChain.then(() => this.#dispatchFastqEvent(event, file, info, memberStart));
        }
        decompressedOffset += part.length;
      }
    };

    const handleStructural = async event => {
      if (event.type === 'member') {
        currentMemberStart = event.start;
        if (!event.crcOk || !event.sizeOk) {
          // 校验失败的 member：上报坏尾，且不把其解压内容送入解析器。
          await this.#record(EVENT_TYPES.BAD_TAIL, {
            file, start: event.start, end: event.end,
            reason: `member 校验失败 crcOk=${event.crcOk} sizeOk=${event.sizeOk}`,
          });
          return;
        }
        feedParts(event.parts, event.start);
      } else if (event.type === 'truncated') {
        currentMemberStart = event.start;
        await this.#record(EVENT_TYPES.BAD_TAIL, {
          file, start: event.start, end: event.end, reason: event.reason, salvagedBytes: event.salvagedBytes,
        });
        // 抢救出的完整输出块仍尝试解析，其中坏记录会被正常隔离。
        feedParts(event.parts, event.start);
      } else if (event.type === 'garbage') {
        await this.#record(EVENT_TYPES.BAD_TAIL, {
          file, start: event.start, end: event.end, reason: event.reason,
        });
      }
    };

    let compressedOffset = 0;
    for await (const chunk of handle.readableWebStream()) {
      const structural = await memberReader.push(Buffer.from(chunk), compressedOffset);
      compressedOffset += chunk.length;
      for (const event of structural) await handleStructural(event);
      await dispatchChain;
    }
    const tail = await memberReader.finish();
    if (tail) await handleStructural(tail);
    await dispatchChain;
    for (const event of parser.finish()) {
      await this.#dispatchFastqEvent(event, file, info, currentMemberStart);
    }
    await dispatchChain;
    await handle.close();
  }

  async #dispatchFastqEvent(event, file, info, memberStart) {
    // 恢复后重放已处理 ordinal：跳过，保证不重复发布。
    if (typeof event.ordinal === 'number' && this.#ordinalSet(file).has(event.ordinal)) return;

    if (event.type === 'bad') {
      await this.#record(EVENT_TYPES.BAD_RECORD, {
        file, lane: info.lane, code: event.code, message: event.message,
        byteStart: event.byteStart, byteEnd: event.byteEnd, lines: event.lines, ordinal: event.ordinal, memberStart,
      });
      if (typeof event.ordinal === 'number') this.#ordinalSet(file).add(event.ordinal);
      return;
    }

    // 端标识核对：无法识别或与文件所属端不一致都按坏记录隔离。
    if (event.end === null) {
      await this.#record(EVENT_TYPES.BAD_RECORD, {
        file, lane: info.lane, code: 'UNKNOWN_END',
        message: `标题无法识别端标识（需 @id/${info.end} 或 @id ${info.end}:N:0:${info.end} 形式）`,
        byteStart: event.byteStart, byteEnd: event.byteEnd, lines: [event.header], ordinal: event.ordinal, memberStart,
      });
      this.#ordinalSet(file).add(event.ordinal);
      return;
    }
    if (event.end !== info.end) {
      await this.#record(EVENT_TYPES.BAD_RECORD, {
        file, lane: info.lane, code: 'END_MISMATCH',
        message: `标题端标识 ${event.end} 与文件所属端 ${info.end} 不一致`,
        byteStart: event.byteStart, byteEnd: event.byteEnd, lines: [event.header], ordinal: event.ordinal, memberStart,
      });
      this.#ordinalSet(file).add(event.ordinal);
      return;
    }

    const key = pairKey(this.#manifest.run, info.lane, event.id);
    const fp = fingerprint(event);
    const existing = await this.#staging.get(key);
    const basePayload = {
      file, lane: info.lane, end: info.end, ordinal: event.ordinal, pairKey: key,
      readId: event.id, fingerprint: fp, header: event.header,
      sequence: event.sequence, quality: event.quality,
      byteStart: event.byteStart, byteEnd: event.byteEnd, memberStart,
    };
    let type = EVENT_TYPES.RECORD;
    if (existing && existing[info.end]) {
      if (existing[info.end].fingerprint === fp) {
        type = EVENT_TYPES.RETRANSMIT; // 同端同内容：重传
      } else {
        await this.#record(EVENT_TYPES.CONFLICT, {
          ...basePayload,
          existingFingerprint: existing[info.end].fingerprint,
          existingFile: existing[info.end].file,
          message: `同身份 ${info.lane}/${event.id} 的 ${info.end} 端出现不同内容，已隔离，不任选第一条`,
        });
        this.#ordinalSet(file).add(event.ordinal);
        return; // 冲突内容不覆盖暂存，不参与配对
      }
    }
    await this.#record(type, basePayload);
    this.#ordinalSet(file).add(event.ordinal);

    const bucket = existing ?? {};
    bucket[info.end] = basePayload;
    await this.#staging.put(key, bucket);
    await this.#tryPair(key);
  }

  // 两端到齐则发布配对；pairKey 幂等，投影由 #record 统一完成。
  async #tryPair(key) {
    const bucket = await this.#staging.get(key);
    if (!bucket || !bucket[1] || !bucket[2]) return;
    const {runId, laneId, readId} = parsePairKey(key);
    const payload = {
      pairKey: key, run: runId, lane: laneId, read: readId,
      r1: {file: bucket[1].file, header: bucket[1].header, sequence: bucket[1].sequence, quality: bucket[1].quality, fingerprint: bucket[1].fingerprint},
      r2: {file: bucket[2].file, header: bucket[2].header, sequence: bucket[2].sequence, quality: bucket[2].quality, fingerprint: bucket[2].fingerprint},
    };
    const appended = await this.#record(EVENT_TYPES.PAIR, payload);
    if (appended) await this.#staging.delete(key);
  }

  // 一键接收整个运行。中断后再次调用从账本与 spool 恢复进度。
  async receiveRun(baseUrl, {onProgress = () => {}} = {}) {
    const done = [];
    for (const file of this.#fileIndex().keys()) {
      if (!this.#state.files.get(file)?.streamDone) {
        const claim = await this.claimFile(baseUrl, file);
        onProgress({phase: 'claimed', file, ...claim});
      } else {
        onProgress({phase: 'cached', file});
      }
      if (this.#processedFiles.has(file)) {
        onProgress({phase: 'replayed', file});
      } else {
        await this.processFile(file);
        this.#processedFiles.add(file);
        onProgress({phase: 'processed', file});
      }
      done.push(file);
    }
    await this.#store.checkpoint(await this.report());
    return done;
  }

  // ---- 查询接口 ----

  pairs(laneId) {
    return this.#state.pairOrder
      .map(key => this.#state.pairs.get(key))
      .filter(pair => !laneId || pair.lane === laneId);
  }

  // 单端缺口：暂存中仍缺另一端的身份。
  async gaps() {
    const result = [];
    for await (const key of this.#staging.keys()) {
      const bucket = await this.#staging.get(key);
      const {laneId, readId} = parsePairKey(key);
      const missingEnds = [];
      if (!bucket[1]) missingEnds.push(1);
      if (!bucket[2]) missingEnds.push(2);
      if (missingEnds.length) {
        result.push({lane: laneId, read: readId, missingEnds, arrivedEnds: Object.keys(bucket).map(Number)});
      }
    }
    return result;
  }

  badRecords() {
    return this.#state.badRecords.map(item => ({...item}));
  }

  badTails() {
    const tails = [];
    for (const [file, entry] of this.#state.files) {
      if (entry.badTail) tails.push({file, ...entry.badTail});
    }
    return tails;
  }

  conflicts() {
    return this.#state.conflicts.map(item => ({...item}));
  }

  retransmits() {
    return this.#state.retransmits.map(item => ({...item}));
  }

  // 字节处理进度：每文件已领取字节、总字节、流结束与压缩尾状态。
  // bytesComplete 只表示传输字节到齐；complete 还要求压缩尾完好。
  byteProgress() {
    const result = {};
    for (const [file, entry] of this.#state.files) {
      const lastClaim = entry.claims.at(-1);
      const bytesComplete = entry.streamDone && (entry.totalBytes == null || lastClaim?.end === entry.totalBytes);
      result[file] = {
        claimedBytes: lastClaim?.end ?? 0,
        totalBytes: entry.totalBytes ?? null,
        streamDone: entry.streamDone,
        bytesComplete,
        compressedIntact: entry.badTail == null,
        complete: bytesComplete && entry.badTail == null,
        badTail: entry.badTail ? {start: entry.badTail.start, end: entry.badTail.end, reason: entry.badTail.reason} : null,
      };
    }
    return result;
  }

  // 运行完整度：三层判断分别给出，不混为一谈。
  async completeness() {
    const index = this.#fileIndex();
    const lanes = [];
    let streamsDone = true;
    let recordsValid = true;
    let complete = true;
    const allGaps = await this.gaps();

    for (const lane of this.#manifest.lanes) {
      const report = {
        lane: lane.id,
        expected: {r1: lane.count, r2: lane.count},
        actual: {r1: 0, r2: 0},
        pairs: 0,
        gaps: [],
        problems: [],
      };
      for (const [file, meta] of index) {
        if (meta.lane !== lane.id) continue;
        const entry = this.#state.files.get(file);
        if (!entry?.streamDone) {
          streamsDone = false;
          complete = false;
          report.problems.push(`${file}: 流未结束`);
        }
        if (entry?.badTail) {
          recordsValid = false;
          complete = false;
          report.problems.push(`${file}: 压缩尾损坏（${entry.badTail.reason}）`);
        }
      }
      for (const bad of this.#state.badRecords.filter(b => b.lane === lane.id)) {
        recordsValid = false;
        complete = false;
        report.problems.push(`坏记录 ordinal=${bad.ordinal} ${bad.code} 解压字节[${bad.byteStart},${bad.byteEnd})`);
      }
      for (const conflict of this.#state.conflicts.filter(c => c.lane === lane.id)) {
        complete = false;
        report.problems.push(`冲突隔离 ${conflict.readId} 端${conflict.end}`);
      }
      report.actual.r1 = this.#state.arrivalCount.get(lane.r1) ?? 0;
      report.actual.r2 = this.#state.arrivalCount.get(lane.r2) ?? 0;
      report.pairs = this.pairs(lane.id).length;
      report.gaps = allGaps.filter(g => g.lane === lane.id);
      if (report.gaps.length) complete = false;
      if (report.actual.r1 !== lane.count || report.actual.r2 !== lane.count) {
        complete = false;
        report.problems.push(`到达计数与清单不符：期望 ${lane.count}/${lane.count}，实际 ${report.actual.r1}/${report.actual.r2}`);
      }
      if (report.pairs !== lane.count) complete = false;
      lanes.push(report);
    }
    return {
      run: this.#manifest.run,
      streamsDone,
      recordsValid: recordsValid && this.#state.badRecords.length === 0,
      complete,
      lanes,
    };
  }

  async report() {
    return {
      run: this.#manifest.run,
      bytes: this.byteProgress(),
      pairs: this.pairs(),
      gaps: await this.gaps(),
      badRecords: this.badRecords(),
      badTails: this.badTails(),
      conflicts: this.conflicts(),
      retransmits: this.retransmits(),
      completeness: await this.completeness(),
    };
  }
}

export async function openReceiver(directory, runId, manifest, options = {}) {
  await mkdir(directory, {recursive: true});
  const store = await new EventStore(directory, runId, {syncWrites: options.syncWrites ?? true}).open();
  return Receiver.recover(store, manifest, options);
}

export async function loadManifest(source, fetchImpl = fetch) {
  const response = await fetchImpl(`${source}/manifest`, {signal: AbortSignal.timeout(5000)});
  if (!response.ok) throw new Error(`交付清单读取失败: ${response.status}`);
  return response.json();
}
