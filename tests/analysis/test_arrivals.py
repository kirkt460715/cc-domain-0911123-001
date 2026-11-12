import copy
import datetime as dt
import json

from registry_bridge.analysis import arrivals, exporting, registry
from registry_bridge.errors import InvalidMessage, NotFound
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import issuers
from registry_bridge.ledger.storage import Store
from registry_bridge.service import RegistryService
from tests.support import Clock, LedgerCase, registration, transition


def timed_register(sequence, *, issuer, asset, effective, applied=None, received=None, holder='holder1'):
    message = registration(sequence, issuer=issuer, asset=asset, holder=holder)
    message['event_id'] = f'{issuer}:{asset}:{sequence}'
    message['effective_at'] = effective
    return message


class TimedFeed:
    """按 received_at 交错投递，并让存储时钟决定 applied_at。"""

    def __init__(self, store):
        self.store = store
        self.clock = Clock()
        self.store.clock = self.clock
        self.sequences = {}

    def add(self, message, *, received_at, drain=True):
        issuer = message['issuer']
        self.sequences[issuer] = self.sequences.get(issuer, 0) + 1
        message = copy.deepcopy(message)
        message['sequence'] = self.sequences[issuer]
        target = dt.datetime.fromisoformat(received_at.replace('Z', '+00:00'))
        self.clock.now = target
        return receive(self.store, message, drain=drain)


def base_params(**overrides):
    params = {
        'issuer': 'north',
        'start_week': '2026-06-01',
        'end_week': '2026-10-12',
        'knowledge_cutoff': '2026-10-15T00:00:00Z',
        'timezone': 'UTC',
        'max_maturity_lag_weeks': 4,
        'min_history_weeks': 4,
        'bootstrap_samples': 500,
        'bootstrap_lower_quantile': 0.1,
        'bootstrap_upper_quantile': 0.9,
        'seed': 20261112,
    }
    params.update(overrides)
    return params


class ArrivalAnalysisTests(LedgerCase):
    @staticmethod
    def feed_steady_history(feed, *, issuer='north', weeks=12, per_week=5, lag_days=2):
        start = dt.date(2026, 6, 1)
        seq = 0
        for week_index in range(weeks):
            monday = start + dt.timedelta(weeks=week_index)
            for item in range(per_week):
                seq += 1
                # 事件分散在周一到周五；滞后从 2 天到约三周，制造跨周的部分到达
                effective = dt.datetime.combine(
                    monday + dt.timedelta(days=item % 5), dt.time(0, 0), tzinfo=dt.timezone.utc,
                )
                lag = lag_days + ((seq * 7 + item * 3) % 21)
                received = effective + dt.timedelta(days=lag)
                feed.add(
                    timed_register(seq, issuer=issuer, asset=f'{issuer}-w{week_index}-a{item}',
                                   effective=effective.isoformat().replace('+00:00', 'Z')),
                    received_at=received.isoformat().replace('+00:00', 'Z'),
                )

    def test_only_first_successful_register_counts_as_new(self):
        feed = TimedFeed(self.store)
        feed.add(timed_register(1, issuer='north', asset='a1',
                                effective='2026-06-01T00:00:00Z'),
                 received_at='2026-06-01T00:00:00Z')
        # amend 不算新增
        amend = transition(2, 2, 'amend', issuer='north', asset='a1')
        amend['event_id'] = 'north:a1:2'
        feed.add(amend, received_at='2026-06-02T00:00:00Z')
        # 重复投递：同 event_id 再投一次，台账只保留一条
        duplicate = timed_register(1, issuer='north', asset='a1',
                                   effective='2026-06-01T00:00:00Z')
        duplicate['sequence'] = 1
        feed.add(duplicate, received_at='2026-06-03T00:00:00Z')
        result = registry.run_analysis(self.store, base_params())
        first_week = result['cohorts'][0]
        self.assertEqual(first_week['known_count'], 1)
        self.assertEqual(first_week['observed_final_count'], 1)

    def test_mature_week_uses_observed_final_and_immature_week_estimates(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=19, per_week=6)
        # 最近两周只到了部分登记
        start = dt.date(2026, 6, 1)
        seq = 19 * 6
        for week_index in (18, 19):
            monday = start + dt.timedelta(weeks=week_index)
            for item in range(3):
                seq += 1
                effective = dt.datetime.combine(monday, dt.time(0, 0), tzinfo=dt.timezone.utc)
                feed.add(
                    timed_register(seq, issuer='north', asset=f'north-late-w{week_index}-a{item}',
                                   effective=effective.isoformat().replace('+00:00', 'Z')),
                    received_at=(effective + dt.timedelta(days=1)).isoformat().replace('+00:00', 'Z'),
                )
        result = registry.run_analysis(self.store, base_params())
        by_week = {row['business_week']: row for row in result['cohorts']}
        mature = by_week['2026-09-07']
        self.assertTrue(mature['mature'])
        self.assertEqual(mature['status'], 'mature')
        self.assertEqual(mature['estimated_final_count'], 6)
        immature = by_week['2026-10-05']
        self.assertFalse(immature['mature'])
        self.assertEqual(immature['status'], 'estimated')
        self.assertIsNotNone(immature['interval'])
        self.assertGreaterEqual(immature['estimated_final_count'], immature['known_count'])
        self.assertGreaterEqual(immature['expected_outstanding_count'], 0)
        self.assertLessEqual(immature['interval']['lower'], immature['interval']['point'])
        self.assertGreaterEqual(immature['interval']['upper'], immature['interval']['point'])

    def test_received_but_not_projected_listed_separately(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=18)
        # 堵住 north 流：先投 sequence 2 且不 drain，再投 sequence 1，二者都保持 pending
        blocked = timed_register(999, issuer='north', asset='blocked-2',
                                 effective='2026-10-06T00:00:00Z')
        blocked['sequence'] = 999
        feed.add(blocked, received_at='2026-10-07T00:00:00Z', drain=False)
        waiting = timed_register(998, issuer='north', asset='waiting-1',
                                 effective='2026-10-05T00:00:00Z')
        waiting['sequence'] = 998
        feed.add(waiting, received_at='2026-10-08T00:00:00Z', drain=False)
        result = registry.run_analysis(self.store, base_params())
        pending = result['received_not_projected']
        self.assertEqual(pending['count'], 2)
        self.assertEqual(pending['knowledge_cutoff_basis'], 'applied_at')
        pending_ids = {item['event_id'] for item in pending['items']}
        self.assertIn('north:blocked-2:999', pending_ids)
        # pending 登记不计入已知量
        by_week = {row['business_week']: row for row in result['cohorts']}
        self.assertEqual(by_week['2026-10-05']['known_count'], 0)

    def test_negative_lag_is_anomaly_and_excluded_from_profile_training(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=19)
        # 构造一个负延迟周：applied 早于 effective
        odd = timed_register(1, issuer='north', asset='negative-lag',
                             effective='2026-08-15T00:00:00Z')
        odd['sequence'] = 500
        feed.add(odd, received_at='2026-08-10T00:00:00Z')
        result = registry.run_analysis(self.store, base_params())
        self.assertGreaterEqual(result['anomalies']['negative_lag']['count'], 1)
        excluded = result['profile']['excluded_negative_lag_weeks']
        self.assertIn('2026-08-10', excluded)

    def test_refuses_deterministic_estimate_when_history_insufficient(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=6, per_week=4)
        result = registry.run_analysis(self.store, base_params(min_history_weeks=10))
        self.assertEqual(result['profile']['status'], 'insufficient_history')
        for row in result['cohorts']:
            if not row['mature']:
                self.assertIsNone(row['estimated_final_count'])
                self.assertEqual(row['status'], 'insufficient_history')
                # 拒绝估计时仍保留观测量
                self.assertIsInstance(row['known_count'], int)

    def test_borrowing_requires_explicit_opt_in_and_reports_donors(self):
        feed = TimedFeed(self.store)
        # north 历史很短，south 有充足且稳定的历史，二者同组
        self.feed_steady_history(feed, issuer='north', weeks=5, per_week=4)
        self.feed_steady_history(feed, issuer='south', weeks=19, per_week=6)
        groups = {'north': 'group-a', 'south': 'group-a'}
        without = registry.run_analysis(self.store, base_params(
            min_history_weeks=8, borrow_groups=groups, allow_borrow=False))
        self.assertEqual(without['profile']['status'], 'insufficient_history')
        self.assertEqual(without['profile']['donor_issuers'], [])
        with_borrow = registry.run_analysis(self.store, base_params(
            min_history_weeks=8, borrow_groups=groups, allow_borrow=True))
        self.assertEqual(with_borrow['profile']['status'], 'ok')
        self.assertTrue(with_borrow['profile']['borrowed'])
        self.assertEqual(with_borrow['profile']['donor_issuers'], ['south'])

    def test_other_group_issuers_are_not_borrowed(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, issuer='north', weeks=5, per_week=4)
        self.feed_steady_history(feed, issuer='south', weeks=19, per_week=6)
        groups = {'north': 'group-a', 'south': 'group-b'}
        result = registry.run_analysis(self.store, base_params(
            min_history_weeks=8, borrow_groups=groups, allow_borrow=True))
        self.assertEqual(result['profile']['status'], 'insufficient_history')
        self.assertEqual(result['profile']['donor_issuers'], [])

    def test_seed_makes_bootstrap_intervals_deterministic(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=19, per_week=6)
        first = registry.run_analysis(self.store, base_params(seed=42))
        second = registry.run_analysis(self.store, base_params(seed=42))
        self.assertEqual(first['cohorts'], second['cohorts'])
        other = registry.run_analysis(self.store, base_params(seed=43))
        estimated = [row for row in first['cohorts'] if row['status'] == 'estimated']
        other_estimated = [row for row in other['cohorts'] if row['status'] == 'estimated']
        self.assertTrue(estimated and other_estimated)

    def test_timezone_changes_week_assignment(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=19)
        utc = registry.run_analysis(self.store, base_params(timezone='UTC'))
        shanghai = registry.run_analysis(self.store, base_params(timezone='Asia/Shanghai'))
        self.assertEqual(shanghai['parameters']['timezone'], 'Asia/Shanghai')
        # 同样的数据、不同时区，文档摘要应当不同
        self.assertNotEqual(utc['analysis_id'], shanghai['analysis_id'])

    def test_invalid_week_and_quantile_rejected(self):
        with self.assertRaises(InvalidMessage):
            registry.run_analysis(self.store, base_params(start_week='2026-06-02'))
        with self.assertRaises(InvalidMessage):
            registry.run_analysis(self.store, base_params(bootstrap_upper_quantile=0.0))
        with self.assertRaises(InvalidMessage):
            registry.run_analysis(self.store, base_params(timezone='Not/AZone'))

    def test_zero_known_cohort_refuses_point_estimate(self):
        feed = TimedFeed(self.store)
        self.feed_steady_history(feed, weeks=19, per_week=6)
        result = registry.run_analysis(self.store, base_params())
        zero_known = [c for c in result['cohorts'] if not c['mature'] and c['known_count'] == 0]
        self.assertTrue(zero_known)
        for row in zero_known:
            self.assertIsNone(row['estimated_final_count'])
            self.assertIsNone(row['expected_outstanding_count'])
            self.assertEqual(row['status'], 'ratio_zero')

    def test_rejected_and_non_register_events_never_enter_cohorts(self):
        feed = TimedFeed(self.store)
        # 一个正常登记
        feed.add(timed_register(1, issuer='north', asset='good',
                                effective='2026-06-01T00:00:00Z'),
                 received_at='2026-06-01T00:00:00Z')
        # 对未登记资产直接发 amend，投影必然拒绝
        bad = transition(2, 2, 'amend', issuer='north', asset='never-registered')
        bad['event_id'] = 'north:bad-amend'
        feed.add(bad, received_at='2026-06-02T00:00:00Z')
        registers = registry.load_register_events(self.store)
        self.assertEqual([row['asset_id'] for row in registers], ['good'])
        # 拒绝事件不是 pending，不会混进“收到未投影”
        self.assertEqual(registry.load_pending_events(self.store), [])
        result = registry.run_analysis(self.store, base_params())
        self.assertEqual(result['received_not_projected']['count'], 0)
        self.assertEqual(sum(c['known_count'] for c in result['cohorts']), 1)


class PersistenceTests(LedgerCase):
    def test_saved_analysis_is_content_addressed_and_immutable(self):
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19)
        created = registry.create_analysis(self.store, base_params())
        analysis_id = created['analysis_id']
        fetched = registry.get_analysis(self.store, analysis_id)
        self.assertEqual(fetched, created['document'])
        # 同样输入与参数重复保存：复用同一 id
        again = registry.create_analysis(self.store, base_params())
        self.assertEqual(again['analysis_id'], analysis_id)
        self.assertTrue(again['reused'])
        with self.assertRaises(NotFound):
            registry.get_analysis(self.store, '0' * 64)

    def test_late_resupply_creates_new_version_but_old_remains_retrievable(self):
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19, per_week=6)
        first = registry.create_analysis(self.store, base_params())
        first_id = first['analysis_id']
        # 周三补报：为最近一周新增此前缺失的登记
        start = dt.date(2026, 6, 1)
        monday = start + dt.timedelta(weeks=19)
        seq = 19 * 6
        for item in range(3, 6):
            seq += 1
            effective = dt.datetime.combine(monday, dt.time(0, 0), tzinfo=dt.timezone.utc)
            feed.add(
                timed_register(seq, issuer='north', asset=f'north-resupply-a{item}',
                               effective=effective.isoformat().replace('+00:00', 'Z')),
                received_at='2026-10-14T00:00:00Z',
            )
        revised = registry.explain_revision(
            self.store, {**base_params(), 'reference_analysis_id': first_id},
        )
        self.assertNotEqual(revised['analysis_id'], first_id)
        self.assertEqual(revised['revision']['changes'][0]['business_week'], '2026-10-12')
        self.assertEqual(revised['revision']['changes'][0]['newly_known_count'], 3)
        # 旧版本原样可取
        old = registry.get_analysis(self.store, first_id)
        self.assertEqual(old['analysis_id'], first_id)
        self.assertNotEqual(old['input_fingerprint'], revised['document']['input_fingerprint'])

    def test_revision_attributes_knowledge_when_only_cutoff_advances(self):
        # 同一台账（输入指纹不变），仅知识截止前移，也要能解释新增可知登记来自哪周
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19, per_week=6)
        early = registry.create_analysis(
            self.store, base_params(knowledge_cutoff='2026-09-01T00:00:00Z'),
        )
        late = registry.explain_revision(self.store, {
            **base_params(knowledge_cutoff='2026-10-15T00:00:00Z'),
            'reference_analysis_id': early['analysis_id'],
        })
        # 指纹相同，但冻结的可知事件集合不同
        self.assertEqual(
            early['document']['input_fingerprint'],
            late['document']['input_fingerprint'],
        )
        self.assertTrue(late['revision']['changes'])
        for change in late['revision']['changes']:
            self.assertEqual(change['newly_known_count'], len(change['event_ids']))
        # 早期版本依旧原样可取
        self.assertEqual(
            registry.get_analysis(self.store, early['analysis_id'])['analysis_id'],
            early['analysis_id'],
        )

    def test_export_roundtrip_preserves_document_byte_for_byte(self):
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19)
        created = registry.create_analysis(self.store, base_params())
        path = self.path.parent / 'analysis.json'
        written = exporting.write_analysis(self.store, created['analysis_id'], path)
        self.assertTrue(path.is_file())
        loaded = exporting.read_analysis_file(path)
        self.assertEqual(loaded, created['document'])
        # 导回一个全新的台账后内容一致
        with Store(':memory:') as other:
            issuers.add(other, 'north', 'North')
            result = exporting.import_analysis(other, path)
            self.assertEqual(result['analysis_id'], created['analysis_id'])
            self.assertEqual(registry.get_analysis(other, result['analysis_id']), loaded)
        self.assertEqual(written['sha256'], __import__('hashlib').sha256(
            path.read_bytes()).hexdigest())

    def test_tampered_export_is_rejected(self):
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19)
        created = registry.create_analysis(self.store, base_params())
        path = self.path.parent / 'analysis.json'
        exporting.write_analysis(self.store, created['analysis_id'], path)
        envelope = json.loads(path.read_text())
        envelope['document']['parameters']['seed'] = 999
        path.write_text(json.dumps(envelope))
        with self.assertRaises(InvalidMessage):
            exporting.read_analysis_file(path)


class HindcastTests(LedgerCase):
    def test_hindcast_only_uses_then_known_records_and_compares_naive_baseline(self):
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19, per_week=6, lag_days=2)
        params = base_params(horizon_weeks=4)
        result = registry.hindcast(self.store, params, horizon_weeks=params['horizon_weeks'])
        self.assertEqual(result['kind'], 'arrival_hindcast')
        self.assertEqual(len(result['reviews']), 4)
        # 回看点必须是分析范围内最后四个业务周的周一
        self.assertEqual(
            [review['review_week'] for review in result['reviews']],
            ['2026-09-21', '2026-09-28', '2026-10-05', '2026-10-12'],
        )
        for review in result['reviews']:
            self.assertIsNotNone(review['model_mae'])
            self.assertIsNotNone(review['naive_mae'])
            self.assertTrue(review['weeks'])
            for item in review['weeks']:
                self.assertLessEqual(item['known_count'], item['final_count'])
        # 整个回看期内，模型在部分周严格优于“已知即最终”
        better = sum(review['model_better_weeks'] for review in result['reviews'])
        self.assertGreater(better, 0)
        # 持久化内容寻址
        saved = registry.save_analysis(self.store, result)
        self.assertEqual(saved['analysis_id'], result['analysis_id'])

    def test_mechanism_change_flags_issuer_with_shifted_lag_pattern(self):
        feed = TimedFeed(self.store)
        # north 前半段几乎实时、后半段大幅迟报
        ArrivalAnalysisTests.feed_steady_history(feed, issuer='north', weeks=10, per_week=6, lag_days=1)
        start = dt.date(2026, 6, 1)
        seq = 10 * 6
        for week_index in range(10, 19):
            monday = start + dt.timedelta(weeks=week_index)
            for item in range(6):
                seq += 1
                effective = dt.datetime.combine(monday, dt.time(0, 0), tzinfo=dt.timezone.utc)
                feed.add(
                    timed_register(seq, issuer='north', asset=f'north-shift-w{week_index}-a{item}',
                                   effective=effective.isoformat().replace('+00:00', 'Z')),
                    received_at=(effective + dt.timedelta(days=14)).isoformat().replace('+00:00', 'Z'),
                )
        result = registry.hindcast(self.store, base_params())
        self.assertIn('north', result['mechanism_change']['flagged_issuers'])


class ServiceWiringTests(LedgerCase):
    def test_service_operations_and_interleaved_drill_end_to_end(self):
        service = RegistryService(self.store)
        feed = TimedFeed(self.store)
        ArrivalAnalysisTests.feed_steady_history(feed, weeks=19)
        params = base_params()
        created = service.call('arrival.analyze', params)
        self.assertIn('analysis_id', created)
        listing = service.call('arrival.list', {})
        self.assertEqual(listing['count'], 1)
        fetched = service.call('arrival.get', {'analysis_id': created['analysis_id']})
        self.assertEqual(fetched['analysis_id'], created['analysis_id'])
        path = str(self.path.parent / 'svc-analysis.json')
        export_result = service.call('arrival.export', {
            'analysis_id': created['analysis_id'], 'path': path,
        })
        self.assertEqual(export_result['analysis_id'], created['analysis_id'])
        imported = service.call('arrival.import', {'path': path})
        self.assertTrue(imported['reused'])
        hindcast = service.call('arrival.hindcast', {**params, 'horizon_weeks': 2})
        self.assertEqual(len(hindcast['reviews']), 2)
        with self.assertRaises(InvalidMessage):
            service.call('arrival.analyze', {'issuer': 'north'})

    def test_unknown_operation_path_unchanged(self):
        service = RegistryService(self.store)
        with self.assertRaises(InvalidMessage):
            service.call('arrival.nonsense', {})
