"""用 examples/context.json 的交错投递演练登记量“当时能知道多少”分析。

复用 context.py 的入库口径（按 received_at 驱动时钟、每条登记重复投递核对幂等），
随后在同一内存台账上跑到达分析与滚动回看。整个过程只读合成数据、参数与种子固定，
输出可复现，不写入持久台账。
"""
import copy
import json
from pathlib import Path

from registry_bridge.analysis import registry
from examples.context import load


def _build_store():
    from registry_bridge.ledger.storage import Store
    from registry_bridge.ledger import issuers
    from registry_bridge.feeds import receiver

    data = load()
    now = ['2026-01-01T00:00:00Z']
    store = Store(':memory:', clock=lambda: now[0])
    for issuer in ['north', 'south']:
        issuers.add(store, issuer, '合成登记机构')
    seq = {'north': 0, 'south': 0}
    template = json.loads(
        (Path(__file__).parents[1] / 'fixtures/demo.json').read_text()
    )['messages'][1]
    for row in data['records']:
        now[0] = row['received_at']
        seq[row['issuer']] += 1
        message = copy.deepcopy(template)
        message.update(
            event_id=f"{row['issuer']}:{seq[row['issuer']]}",
            issuer=row['issuer'], sequence=seq[row['issuer']],
            asset_id=row['asset_id'],
        )
        message['effective_at'] = row['effective_at']
        receiver.receive(store, message)
        receiver.receive(store, message)
    return store, data['cutoff']


def params(cutoff, issuer):
    return {
        'issuer': issuer,
        'start_week': '2026-06-01',
        'end_week': '2026-10-12',
        'knowledge_cutoff': cutoff,
        'timezone': 'UTC',
        'max_maturity_lag_weeks': 4,
        'min_history_weeks': 4,
        'bootstrap_samples': 500,
        'bootstrap_lower_quantile': 0.1,
        'bootstrap_upper_quantile': 0.9,
        'seed': 20261112,
    }


def run():
    store, cutoff = _build_store()
    with store:
        analyses = {}
        for issuer in ('north', 'south'):
            document = registry.run_analysis(store, params(cutoff, issuer))
            analyses[issuer] = {
                'analysis_id': document['analysis_id'],
                'profile_status': document['profile']['status'],
                'borrowed': document['profile']['borrowed'],
                'donor_issuers': document['profile']['donor_issuers'],
                'residual_underreporting_possible':
                    document['anomalies']['residual_underreporting_possible'],
                'negative_lag_count': document['anomalies']['negative_lag']['count'],
                'received_not_projected': document['received_not_projected']['count'],
                'immature_cohorts': [
                    {
                        'business_week': row['business_week'],
                        'known_count': row['known_count'],
                        'estimated_final_count': row['estimated_final_count'],
                        'expected_outstanding_count': row['expected_outstanding_count'],
                        'status': row['status'],
                    }
                    for row in document['cohorts'] if not row['mature']
                ],
            }
        hindcast = registry.hindcast(
            store, params(cutoff, 'north'), horizon_weeks=4,
        )
        return {
            'ok': True,
            'knowledge_cutoff': cutoff,
            'analyses': analyses,
            'hindcast': {
                'review_count': len(hindcast['reviews']),
                'mechanism_flagged': hindcast['mechanism_change']['flagged_issuers'],
            },
        }


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True, indent=1))
