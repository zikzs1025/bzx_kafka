import json
import glob
import statistics
import os

FILES = sorted(glob.glob('target/perf/run-*.json'))
runs = [json.load(open(f, encoding='utf-8')) for f in FILES]


def group_of(label):
    # A1-p1t1-500k -> A ; E-p16t3-1m -> E
    return label.split('-')[0].rstrip('0123456789')


groups = {}
for r in runs:
    groups.setdefault(group_of(r['label']), []).append(r)

NAMES = {
    'A': '单分区基线',
    'B': '关闭 fsync',
    'C': '16 分区单线程',
    'D': '推荐配置',
    'E': '100 万条',
    'F': '小文件 5000 行',
    'G': 'batch 2000',
    'H': 'batch 100',
    'I': '300 万条长跑',
    'J': '背压专项',
    'Q1': '快速对照 fsync 开',
    'Q2': '快速对照 fsync 关',
}

summary = {}
for key, items in groups.items():
    summary[key] = {
        'name': NAMES.get(key, key),
        'n': len(items),
        'records': items[0]['records'],
        'partitions': items[0]['partitions'],
        'threads': items[0]['threads'],
        'batch': items[0]['batch'],
        'per_file': items[0]['perFile'],
        'fsync': items[0]['fsync'],
        'files': items[0]['files'],
        'bytes': items[0]['totalBytes'],
        'tps': statistics.median(r['writeTps'] for r in items),
        'mbps': statistics.median(r['writeMBps'] for r in items),
        'avg': statistics.median(r['avgBatchMs'] for r in items),
        'p50': statistics.median(r['p50Ms'] for r in items),
        'p95': statistics.median(r['p95Ms'] for r in items),
        'p99': statistics.median(r['p99Ms'] for r in items),
        'max': statistics.median(r['maxBatchMs'] for r in items),
        'batches': statistics.median(r['batches'] for r in items),
        'upload': statistics.median(r['uploadElapsedMs'] for r in items),
        'batch_ms': statistics.median(r['avgBatchMs'] for r in items),
        'all_ok': all(r['verifyOk'] and r['fieldCountOk'] for r in items),
        'lines': sum(r['records'] for r in items),
        'cpu': statistics.median(r['writeCpuPercent'] for r in items),
        'heap': max(r['heapUsedMb'] for r in items),
    }

total_runs = len(runs)
total_records = sum(r['records'] for r in runs)
total_bytes = sum(r['totalBytes'] for r in runs)

print('== per-scenario ==')
print(f"{'key':4}{'场景':16}{'次数':>4}{'TPS':>10}{'MB/s':>8}{'avg':>7}{'p50':>7}{'p95':>7}{'p99':>8}{'max':>8}{'files':>6}{'batches':>8}{'ok':>6}")
for key in sorted(summary):
    s = summary[key]
    print(f"{key:4}{s['name']:16}{s['n']:>4}{s['tps']:>10.0f}{s['mbps']:>8.1f}{s['avg']:>7.2f}"
          f"{s['p50']:>7.2f}{s['p95']:>7.2f}{s['p99']:>8.2f}{s['max']:>8.2f}{s['files']:>6}"
          f"{s['batches']:>8.0f}{str(s['all_ok']):>6}")

print()
print(f'runs={total_runs} records={total_records} bytes={total_bytes} ({total_bytes/1024**3:.2f} GB)')
print(f"A tps={summary['A']['tps']:.0f} B tps={summary['B']['tps']:.0f} "
      f"fsync_cost={(summary['B']['tps']-summary['A']['tps'])/summary['B']['tps']*100:.1f}%")
print(f"D/A={summary['D']['tps']/summary['A']['tps']:.2f}x  D/C={summary['D']['tps']/summary['C']['tps']:.2f}x  "
      f"G/D={summary['G']['tps']/summary['D']['tps']:.2f}x  H/D={summary['H']['tps']/summary['D']['tps']:.3f}x")
print(f"G/H={summary['G']['tps']/summary['H']['tps']:.1f}x  I cpu={summary['I']['cpu']:.0f}% heap={summary['I']['heap']}MB")

json.dump(summary, open('target/perf/summary.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
