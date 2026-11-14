// 测试与演示共用的本地分块投递来源。
//
// 不经过 HTTP，直接用 Buffer 模拟来源行为，便于精确控制：
// - chunkBytes：把压缩文件切成小块投递，复现乱序分块到达；
// - badTail：截掉最后 8 字节，制造坏压缩尾；
// - Range：记录已领取字节后用 bytes=start- 续传，模拟中断恢复；
// - 可注入额外文件，构造重传、同身份冲突、同名 read 跨 lane 等场景。
import {gzipSync} from 'node:zlib';

// 构造一个内存来源。files: {name: {gzip: Buffer, text?: string, members?: string[]}}
export function createMockSource(manifest, files, {chunkBytes = 7, supportsRange = true} = {}) {
  const served = [];
  const fetchImpl = async (url, options = {}) => {
    const path = url.replace(/^https?:\/\/[^/]+/, '');
    if (path === '/manifest') {
      return mockResponse(JSON.stringify(manifest), {'content-type': 'application/json'}, 200);
    }
    const match = path.match(/^\/files\/(.+)$/);
    if (!match) return mockResponse('{"error":"not found"}', {}, 404);
    const name = decodeURIComponent(match[1]);
    const entry = files[name];
    if (!entry) return mockResponse('{"error":"未知端文件"}', {}, 404);
    let data = entry.gzip;
    if (entry.badTail) data = data.subarray(0, data.length - 8);
    const rangeHeader = options.headers?.Range;
    let start = 0;
    let status = 200;
    if (rangeHeader) {
      const parsed = /bytes=(\d+)-/.exec(rangeHeader);
      start = parsed ? Number(parsed[1]) : 0;
      if (!supportsRange) {
        start = 0;
        status = 200;
      } else {
        status = 206;
      }
    }
    const slice = data.subarray(start);
    served.push({name, start, end: data.length, status});
    const headers = {
      'content-type': 'application/gzip',
      ...(status === 206 ? {'content-range': `bytes ${start}-${data.length - 1}/${data.length}`} : {}),
    };
    // 用异步可迭代 body 模拟分块网络投递。
    const body = async function* () {
      for (let offset = 0; offset < slice.length; offset += chunkBytes) {
        yield slice.subarray(offset, Math.min(offset + chunkBytes, slice.length));
      }
    };
    return {ok: status < 400, status, headers: new Headers(headers), body: body()};
  };
  return {fetchImpl, served: () => served};
}

function mockResponse(text, headers, status) {
  return {
    ok: status < 400,
    status,
    headers: new Headers(headers),
    body: (async function* () {
      yield Buffer.from(text);
    })(),
  };
}

// 由多段 FASTQ 文本生成多 member gzip。
export function multiMemberGzip(texts) {
  return Buffer.concat(texts.map(text => gzipSync(Buffer.from(text))));
}

// 标准来源（与 src/source.mjs 的 fixtures 保持一致的数据语义）。
export function standardManifest() {
  return {
    run: 'run1',
    lanes: [
      {id: 'lane1', r1: 'lane1-R1.gz', r2: 'lane1-R2.gz', count: 2},
      {id: 'lane2', r1: 'lane2-R1.gz', r2: 'lane2-R2.gz', count: 1},
    ],
  };
}

export function standardFiles() {
  const texts = {
    'lane1-R1.gz': ['@read-a/1\nAC\nGT\n+\n!!\n!!\n', '@read-b/1\nAG\n+\n##\n'],
    'lane1-R2.gz': ['@read-b 2:N:0:1\nCT\n+\n##\n', '@read-a/2\nACGT\n+\n!!!!\n'],
    'lane2-R1.gz': ['@read-a/1\nAA\n+\n!!\n'],
    'lane2-R2.gz': ['@read-a/2\nTT\n+\n!!\n'],
  };
  const files = {};
  for (const [name, parts] of Object.entries(texts)) {
    files[name] = {gzip: multiMemberGzip(parts)};
  }
  return files;
}
