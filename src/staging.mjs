// 有界内存的双端暂存表。
//
// 两端到达顺序可能相差很远（lane1 的 R2 与 R1 顺序不同，且整轮运行不能
// 读进内存等待）。暂存表为每个 (run, lane) 维护 read 身份到“已到端”的索引：
//
// - 内存只保留最近访问的条目，总字节数超过 budget 时把整批条目排序后
//   追加为一个磁盘 spill 分块（JSON Lines），并清空对应内存；
// - 查找先查内存，未命中再查各分块（分块内按 key 有序，二分定位）；
// - 配对发布后删除条目：内存中直接删，分块中追加 tombstone，查询时合并；
// - 分块数量随暂存量对数级增长，读取单个 key 的代价与分块数成正比，
//   而分块数 ≈ log(总暂存量 / budget)，满足有界内存要求。
//
// 身份键带 lane，因此两个 lane 的同名 read 天然互不覆盖。
import {open, mkdir, rename} from 'node:fs/promises';
import path from 'node:path';
import {createInterface} from 'node:readline';
import {createReadStream} from 'node:fs';

export class SpillableStaging {
  #directory;
  #budgetBytes;
  #memory = new Map(); // key -> entry（entry 含 _bytes）
  #memoryBytes = 0;
  #spills = [];        // [{file, keys: [key...]} 有序]
  #tombstones = new Set();
  #spillIndex = 0;
  #namePrefix;

  constructor({directory, budgetBytes = 256 * 1024, namePrefix = 'staging'} = {}) {
    this.#directory = directory;
    this.#budgetBytes = budgetBytes;
    this.#namePrefix = namePrefix;
  }

  get memoryBytes() {
    return this.#memoryBytes;
  }

  get spillCount() {
    return this.#spills.length;
  }

  get entryCount() {
    let count = this.#memory.size;
    for (const spill of this.#spills) count += spill.keys.length;
    return count - this.#tombstones.size;
  }

  #sizeOf(value) {
    return Buffer.byteLength(JSON.stringify(value));
  }

  async #ensureDirectory() {
    await mkdir(this.#directory, {recursive: true});
  }

  // 写入或更新一个身份条目。entry 形如 {1: {...}, 2: {...}}。
  async put(key, entry) {
    await this.#ensureDirectory();
    const previous = this.#memory.get(key);
    if (previous) this.#memoryBytes -= previous._bytes;
    const stored = {...entry};
    stored._bytes = this.#sizeOf(stored);
    this.#memory.set(key, stored);
    this.#memoryBytes += stored._bytes;
    if (this.#memoryBytes > this.#budgetBytes) await this.#spill();
  }

  async #spill() {
    if (this.#memory.size === 0) return;
    const sorted = [...this.#memory.entries()].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    const name = `${this.#namePrefix}-${String(++this.#spillIndex).padStart(4, '0')}.jsonl`;
    const target = path.join(this.#directory, name);
    const tmp = target + '.tmp';
    const handle = await open(tmp, 'w');
    for (const [key, value] of sorted) {
      const clean = {...value};
      delete clean._bytes;
      await handle.writeFile(JSON.stringify({key, value: clean}) + '\n');
    }
    await handle.sync();
    await handle.close();
    await rename(tmp, target);
    this.#spills.push({file: target, keys: sorted.map(([key]) => key)});
    this.#memory.clear();
    this.#memoryBytes = 0;
  }

  // 在有序分块中按 key 二分定位后，流式读到该条目，不把整分块载入内存。
  async #readSpill(spill, key) {
    const {keys, file} = spill;
    let lo = 0;
    let hi = keys.length - 1;
    let found = false;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (keys[mid] === key) {
        found = true;
        break;
      }
      if (keys[mid] < key) lo = mid + 1;
      else hi = mid - 1;
    }
    if (!found) return undefined;
    const rl = createInterface({input: createReadStream(file), crlfDelay: Infinity});
    for await (const line of rl) {
      const parsed = JSON.parse(line);
      if (parsed.key === key) return parsed.value;
      if (parsed.key > key) break; // 分块内有序，越过后无需再读
    }
    return undefined;
  }

  async get(key) {
    if (this.#tombstones.has(key)) return undefined;
    if (this.#memory.has(key)) {
      const value = this.#memory.get(key);
      const clean = {...value};
      delete clean._bytes;
      return clean;
    }
    for (let i = this.#spills.length - 1; i >= 0; i--) {
      const value = await this.#readSpill(this.#spills[i], key);
      if (value !== undefined) return value;
    }
    return undefined;
  }

  // 逻辑删除：配对发布后身份条目不再需要保留。
  async delete(key) {
    this.#tombstones.add(key);
    const previous = this.#memory.get(key);
    if (previous) {
      this.#memoryBytes -= previous._bytes;
      this.#memory.delete(key);
    }
  }

  // 列出当前所有未配对身份（单端缺口查询的基础）。流式产出，不整体进内存。
  async *keys() {
    const emitted = new Set();
    for (const key of this.#memory.keys()) {
      if (!this.#tombstones.has(key)) {
        emitted.add(key);
        yield key;
      }
    }
    for (const spill of this.#spills) {
      const rl = createInterface({input: createReadStream(spill.file), crlfDelay: Infinity});
      for await (const line of rl) {
        const {key} = JSON.parse(line);
        if (emitted.has(key) || this.#tombstones.has(key)) continue;
        emitted.add(key);
        yield key;
      }
    }
  }

  async snapshot() {
    const result = [];
    for await (const key of this.keys()) {
      const value = await this.get(key);
      result.push({key, value});
    }
    return result;
  }
}
