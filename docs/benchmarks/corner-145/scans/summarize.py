import collections, json, math, statistics, sys
from pathlib import Path

src=Path(sys.argv[1])
rows=[json.loads(x) for x in src.read_text().splitlines()]
def q(xs,p):
    return sorted(xs)[math.ceil(len(xs)*p/100)-1]
def times(xs,key='FirstPutNS'):
    vals=[r[key]/1e6 for r in xs]
    return {x:round(q(vals,n),6) for x,n in [('p50_ms',50),('p95_ms',95),('p99_ms',99),('max_ms',100)]}
def min_med_max(samples):
    return [min(samples),statistics.median(samples),max(samples)]
arms=sorted({x['Arm'] for x in rows})
rounds=sorted({x['Round'] for x in rows})
pairs=collections.defaultdict(dict)
for row in rows:
    k=row['Round'],row['Pair']
    assert row['Arm'] not in pairs[k]
    pairs[k][row['Arm']]=row
    assert not row.get('IDWitness')
    assert row['QueueAfter']==0 and row['RootAfter']!=row['RootBefore'] and row['RootAfter']<=row['HighWaterBefore']==row['HighWaterAfter']
    assert bool(row['QueueBefore']>0)==(row['Arm']=='first-after-release')
    assert all(i>0 for i in row['PinnedDATA'])
for k,options in pairs.items():
    assert set(options)==set(arms)
    assert {r['Position'] for r in options.values()}=={0,1}
    a,b=(options[n] for n in arms)
    assert (a['RetiredAtTwoViews'],a['CommitSequence'],a['NewerReleasedFirst'])==(b['RetiredAtTwoViews'],b['CommitSequence'],b['NewerReleasedFirst'])
ans={'samples':len(rows),'matched_pairs':len(pairs),'per_arm':{},'per_round':{},'order':{},'details':{}}
for arm in arms:
    group=[r for r in rows if r['Arm']==arm]
    ans['per_arm'][arm]=times(group)
    ans['per_round'][arm]=[times([r for r in group if r['Round']==i]) for i in rounds]
    ans['order'][arm]=[times([r for r in group if r['Position']==i]) for i in (0,1)]
    ans['details'][arm]={
        'conditioner':times(group,'ConditionerPutNS'),
        'held_seconds_min_median_max':min_med_max([r['OldestHeldNS']/1e9 for r in group]),
        'queue_before_min_median_max':min_med_max([r['QueueBefore'] for r in group]),
        'retired_after_min_median_max':min_med_max([r['RetiredAfterPut'] for r in group]),
        'pinned_DATA_bytes_pre_release':min_med_max([v for r in group for v in r['PinnedDATA']]),
        'compact_calls':sorted({len(r['PinnedDATA']) for r in group}),
        'sequence':sorted({r['CommitSequence'] for r in group}),
        'round_p99_ms_min_median_max':min_med_max([x['p99_ms'] for x in ans['per_round'][arm]])
    }
deltas=[(g['first-after-release']['FirstPutNS']-g['normal']['FirstPutNS'])/1e6 for g in pairs.values()]
ans['paired']={'release_slower':sum(x>0 for x in deltas),'total':len(deltas),'median_difference_ms':statistics.median(deltas),'p99_difference_ms':q(deltas,99),'pooled_p99_ratio':ans['per_arm']['first-after-release']['p99_ms']/ans['per_arm']['normal']['p99_ms']}
print(json.dumps(ans,indent=2))
