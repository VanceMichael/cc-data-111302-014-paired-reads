// 本地分块投递演示：不依赖真实仪器或网络，用内存来源按小块投递 gzip，
// 展示乱序两端、坏压缩尾、同名 read 跨 lane、重传与冲突隔离，并打印
// 可查询的已配对资料、单端缺口、坏记录范围与运行完整度。
//
// 运行：node src/demo-paired.mjs
// 状态写入 .local/demo/，可重复运行观察恢复（默认每次清空重建）。
import {mkdtemp, rm, mkdir, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {openReceiver} from './receiver.mjs';
import {createMockSource, multiMemberGzip} from '../test/mock-source.mjs';

// 演示用交付清单：两个 lane，lane1 每端 2 条，lane2 每端 2 条（含一条缺口）。
const manifest = {
  run: 'demo-run',
  lanes: [
    {id: 'lane1', r1: 'lane1-R1.gz', r2: 'lane1-R2.gz', count: 2},
    {id: 'lane2', r1: 'lane2-R1.gz', r2: 'lane2-R2.gz', count: 2},
  ],
};

function buildFiles() {
  return {
    // R1 顺序 read-a, read-b；R2 故意反过来 read-b, read-a，体现两端乱序。
    // 同时混用两种端标识写法（/1 与 1:N:0:1），且序列/质量允许跨行。
    'lane1-R1.gz': {
      gzip: multiMemberGzip([
        '@read-a/1\nAC\nGT\n+\n!!\n!!\n',
        '@read-b/1\nAGT\n+\n###\n',
      ]),
    },
    'lane1-R2.gz': {
      gzip: multiMemberGzip([
        '@read-b 2:N:0:1\nGGA\n+\n###\n',
        '@read-a/2\nTGCA\n+\n!!!!\n',
      ]),
    },
    // lane2 与 lane1 含同名 read-a，但 lane 不同，身份键互不覆盖。
    // R1 含重传（read-a 同内容两次）与冲突（read-c 同身份不同内容）。
    'lane2-R1.gz': {
      gzip: multiMemberGzip([
        '@read-a/1\nAA\n+\n!!\n',
        '@read-a/1\nAA\n+\n!!\n', // 重传：同端同内容
        '@read-c/1\nCC\n+\n##\n',
        '@read-c/1\nGG\n+\n##\n', // 冲突：同身份不同内容，隔离
      ]),
    },
    // R2 只到 read-a，read-c 的 R2 缺失，形成可查询的单端缺口。
    'lane2-R2.gz': {
      gzip: multiMemberGzip(['@read-a 2:N:0:1\nTT\n+\n!!\n']),
      badTail: true, // 截掉最后 8 字节，制造坏压缩尾
    },
  };
}

function section(title) {
  console.log(`\n=== ${title} ===`);
}

async function main() {
  const directory = await mkdtemp(path.join(tmpdir(), 'paired-demo-'));
  let receiver;
  try {
    const files = buildFiles();
    const source = createMockSource(manifest, files, {chunkBytes: 7});
    receiver = await openReceiver(directory, manifest.run, manifest, {
      fetchImpl: source.fetchImpl,
      syncWrites: false,
      budgetBytes: 512, // 刻意压小预算，触发 spill 分块以展示有界内存
    });

    const phases = [];
    await receiver.receiveRun('http://demo-source', {onProgress: p => phases.push(p)});

    section('投递与处理阶段（体现分块、缓存与恢复语义）');
    for (const phase of phases) console.log(`- ${phase.phase}: ${phase.file}`);
    console.log(`暂存落盘分块数：${receiver.staging.spillCount}，峰值内存暂存字节：${receiver.staging.memoryBytes}`);

    section('字节处理进度（哪些输入字节已处理、流是否结束）');
    console.log(JSON.stringify(receiver.byteProgress(), null, 2));

    section('已配对资料');
    for (const pair of receiver.pairs()) {
      console.log(`- [${pair.lane}] ${pair.read}`);
      console.log(`    R1 ${pair.r1.header} seq=${pair.r1.sequence}`);
      console.log(`    R2 ${pair.r2.header} seq=${pair.r2.sequence}`);
    }

    section('单端缺口（缺哪一端）');
    const gaps = await receiver.gaps();
    if (gaps.length === 0) console.log('（无）');
    for (const gap of gaps) console.log(`- [${gap.lane}] ${gap.read} 缺端 ${gap.missingEnds.join('/')}，已到端 ${gap.arrivedEnds.join('/')}`);

    section('坏记录范围（解压文本字节区间）');
    for (const bad of receiver.badRecords()) {
      console.log(`- [${bad.lane}] ${bad.file} ordinal=${bad.ordinal} ${bad.code} 字节[${bad.byteStart},${bad.byteEnd})：${bad.message}`);
    }
    if (receiver.badRecords().length === 0) console.log('（无结构坏记录）');

    section('坏压缩尾（压缩文件字节区间）');
    for (const tail of receiver.badTails()) console.log(`- ${tail.file} 压缩字节[${tail.start},${tail.end})：${tail.reason}`);

    section('冲突隔离（不任选第一条）');
    for (const conflict of receiver.conflicts()) {
      console.log(`- [${conflict.lane}] ${conflict.readId} 端${conflict.end}：新指纹 ${conflict.fingerprint.slice(0, 12)}… ≠ 已存 ${conflict.existingFingerprint.slice(0, 12)}…`);
    }

    section('重传（同端同内容）');
    for (const r of receiver.retransmits()) console.log(`- [${r.lane}] ${r.readId} 端${r.end} ordinal=${r.ordinal}`);

    section('运行完整度（三层判断分开）');
    const report = await receiver.completeness();
    console.log(`流全部结束：${report.streamsDone}`);
    console.log(`记录都合法：${report.recordsValid}`);
    console.log(`整个运行完整：${report.complete}`);
    for (const lane of report.lanes) {
      console.log(`- ${lane.lane}：期望 ${lane.expected.r1}/${lane.expected.r2}，实际到达 ${lane.actual.r1}/${lane.actual.r2}，配对 ${lane.pairs}`);
      for (const problem of lane.problems) console.log(`    问题：${problem}`);
    }

    // 落地一份 JSON 报告，方便下游系统读取。
    await mkdir(path.join(directory, 'out'), {recursive: true});
    await writeFile(path.join(directory, 'out', 'report.json'), JSON.stringify(await receiver.report(), null, 2));
    section('报告位置');
    console.log(path.join(directory, 'out', 'report.json'));
  } finally {
    // 演示目录位于系统临时区，保留报告路径已打印，进程结束后由系统回收。
    await receiver.close().catch(() => {});
    if (!process.env.KEEP_DEMO) await rm(directory, {recursive: true, force: true}).catch(() => {});
  }
}

main().catch(error => {
  console.error('演示失败：', error);
  process.exitCode = 1;
});
