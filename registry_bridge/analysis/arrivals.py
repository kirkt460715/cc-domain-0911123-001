"""登记量“当时能知道多少”分析。

队列按登记生效周（业务周，周一为起点）划分，统计单位是机构内首次成功 register
的资产；amend、重复投递与后续状态变化都不构成新增。可知时点一律取 applied_at，
收到但尚未投影的登记单独列出。本模块只做纯计算，不接触数据库。
"""
import datetime as dt
import random
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import canonical, digest, identifier, integer, timestamp


def parse_timezone(value):
    if not isinstance(value, str) or not value:
        raise InvalidMessage('Timezone name is required')
    try:
        return ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise InvalidMessage('Unknown timezone', context={'timezone': value}) from exc


def parse_week(value, field):
    if not isinstance(value, str):
        raise InvalidMessage('Expected business week date', context={'field': field})
    try:
        day = dt.date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidMessage('Business week must be an ISO calendar date', context={'field': field}) from exc
    if day.weekday() != 0:
        raise InvalidMessage('Business week range must start on Monday', context={'field': field})
    return day


def parse_bound(value, field, zone):
    instant = dt.datetime.fromisoformat(timestamp(value, field).replace('Z', '+00:00'))
    return instant.astimezone(zone)


def validate_params(p):
    identifier(p['issuer'], 'issuer')
    zone = parse_timezone(p['timezone'])
    start_week = parse_week(p['start_week'], 'start_week')
    end_week = parse_week(p['end_week'], 'end_week')
    if end_week < start_week:
        raise InvalidMessage('Business week range is reversed')
    cutoff = parse_bound(p['knowledge_cutoff'], 'knowledge_cutoff', zone)
    integer(p['max_maturity_lag_weeks'], 'max_maturity_lag_weeks', 0, 520)
    integer(p['min_history_weeks'], 'min_history_weeks', 1, 520)
    integer(p['bootstrap_samples'], 'bootstrap_samples', 100, 100000)
    for bound in (p['bootstrap_lower_quantile'], p['bootstrap_upper_quantile']):
        if isinstance(bound, bool) or not isinstance(bound, (int, float)) or not 0 < bound < 1:
            raise InvalidMessage('Bootstrap quantile must lie strictly between zero and one')
    if p['bootstrap_lower_quantile'] >= p['bootstrap_upper_quantile']:
        raise InvalidMessage('Bootstrap quantile range is reversed')
    instability = p.get('instability_threshold', p.get('instability'))
    if instability is not None and (
        isinstance(instability, bool) or not isinstance(instability, (int, float)) or not 0 < instability < 1
    ):
        raise InvalidMessage('Instability threshold must lie strictly between zero and one')
    groups = {}
    for name, group in (p.get('borrow_groups') or {}).items():
        groups[identifier(name, 'borrow_groups')] = identifier(group, 'borrow_group')
    seed = p.get('seed')
    if seed is not None:
        integer(seed, 'seed', 0, 2**53 - 1)
    return {
        'issuer': p['issuer'],
        'zone': zone,
        'start_week': start_week,
        'end_week': end_week,
        'cutoff': cutoff,
        'max_lag': p['max_maturity_lag_weeks'],
        'min_history': p['min_history_weeks'],
        'samples': p['bootstrap_samples'],
        'lower_q': p['bootstrap_lower_quantile'],
        'upper_q': p['bootstrap_upper_quantile'],
        'instability': instability if instability is not None else 0.25,
        'groups': groups,
        'allow_borrow': bool(p.get('allow_borrow', False)),
        'seed': seed,
    }


def week_key(local_datetime):
    day = local_datetime.date()
    return day - dt.timedelta(days=day.weekday())


def week_label(day):
    return day.isoformat()


def enumerate_weeks(start_week, end_week):
    result = []
    cursor = start_week
    while cursor <= end_week:
        result.append(cursor)
        cursor += dt.timedelta(days=7)
    return result


def parse_event_time(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def build_cohorts(events, params):
    """把首次成功 register 事件按机构/生效周聚合成队列。

    events 为可迭代行，每行含 issuer/asset_id/effective_at/applied_at/event_id。
    调用方已在 SQL 层用每个 (issuer,asset_id) 的最小 position 去重，amend 与重复
    投递不会进入；可选 as_of 时只保留 applied_at<=as_of 的事件。
    """
    zone = params['zone']
    weeks = enumerate_weeks(params['start_week'], params['end_week'])
    week_set = set(weeks)
    cohorts = {}
    issuers = sorted({event['issuer'] for event in events} | {params['issuer']})
    events = list(events)
    for issuer in issuers:
        per_week = {week: [] for week in weeks}
        for event in events:
            if event['issuer'] != issuer:
                continue
            effective = parse_event_time(event['effective_at']).astimezone(zone)
            key = week_key(effective)
            if key not in week_set:
                continue
            per_week[key].append(event)
        cohorts[issuer] = {
            week: sorted(items, key=lambda item: (item['applied_at'], item['event_id']))
            for week, items in per_week.items()
        }
    return weeks, cohorts


def cohort_age(week, cutoff):
    """截止时点时队列已经走过的完整业务周数（0 表示当周）。"""
    return max(0, (cutoff.date() - week).days // 7)


def training_detail(cohorts, issuer, training_weeks, max_lag, zone):
    """把训练队列拆成按周的累计到达计数；负延迟周整周剔除。"""
    detail = []
    excluded = []
    for week in training_weeks:
        items = cohorts[issuer][week]
        if not items:
            continue
        if any(parse_event_time(item['applied_at']) < parse_event_time(item['effective_at']) for item in items):
            excluded.append(week_label(week))
            continue
        total = len(items)
        cumulative = [0] * (max_lag + 1)
        late = 0
        monday = dt.datetime.combine(week, dt.time(0, 0), tzinfo=zone)
        monday_utc = monday.astimezone(dt.timezone.utc)
        for item in items:
            # 到达年龄与 cohort_age 口径一致：以队列周周一为起点，而非事件自身生效时刻。
            lag_seconds = (parse_event_time(item['applied_at']) - monday_utc).total_seconds()
            arrival_age = int(lag_seconds // (7 * 86400))
            age = min(max_lag, max(0, arrival_age))
            for index in range(age, max_lag + 1):
                cumulative[index] += 1
            if arrival_age > max_lag:
                late += 1
        detail.append({
            'week': week_label(week),
            'total': total,
            'cumulative': cumulative,
            'late_rate': late / total,
        })
    return detail, excluded


def pooled_profile(detail, max_lag):
    denominator = sum(item['total'] for item in detail)
    if not denominator:
        return [0.0] * (max_lag + 1)
    return [
        sum(item['cumulative'][age] for item in detail) / denominator
        for age in range(max_lag + 1)
    ]


def profile_status(detail, profile, min_history, instability, max_lag):
    if not detail:
        return 'no_data'
    if len(detail) < min_history:
        return 'insufficient_history'
    if profile[max_lag] <= 0.0:
        return 'ratio_zero'
    # 成熟队列在最大滞后处应当基本到齐；到不齐说明成熟期后仍有漏报机制。
    if profile[max_lag] < 1.0 - instability:
        return 'unstable'
    # 各历史周在同一年龄的到达比例差异过大，说明到达机制不稳定。
    for age in range(max_lag + 1):
        ratios = [item['cumulative'][age] / item['total'] for item in detail]
        mean = sum(ratios) / len(ratios)
        if mean:
            variance = sum((value - mean) ** 2 for value in ratios) / len(ratios)
            if (variance ** 0.5) > instability:
                return 'unstable'
    return 'ok'


def quantile(sorted_values, q):
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = q * (len(sorted_values) - 1)
    lower = int(position)
    fraction = position - lower
    upper = min(lower + 1, len(sorted_values) - 1)
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def bootstrap_final(known, detail, age, params):
    """按历史周整体重采样：每次抽同等数量的训练周（有放回），重算该年龄到达比例。"""
    max_lag = params['max_lag']
    age = min(age, max_lag)
    rng = params['rng']
    draws = []
    count = len(detail)
    for _ in range(params['samples']):
        chosen = [detail[rng.randrange(count)] for _ in range(count)]
        denominator = sum(item['total'] for item in chosen)
        arrived = sum(item['cumulative'][age] for item in chosen)
        ratio = arrived / denominator if denominator else 0.0
        if ratio <= 0.0:
            continue
        draws.append(int(round(known / ratio)))
    if not draws:
        return None
    draws.sort()
    full_ratio = sum(item['cumulative'][age] for item in detail) / sum(item['total'] for item in detail)
    point = int(round(known / full_ratio)) if full_ratio > 0 else known
    return {
        'point': max(known, point),
        'lower': max(known, int(round(quantile(draws, params['lower_q'])))),
        'upper': max(known, int(round(quantile(draws, params['upper_q'])))),
        'resampled_week_count': count,
    }


def estimate_profile(cohorts, issuer, weeks, cutoff, params):
    """为目标机构构造到达比例曲线，包含同组借用与拒绝原因。"""
    max_lag = params['max_lag']
    training_weeks = [week for week in weeks if cohort_age(week, cutoff) >= max_lag]
    detail, excluded = training_detail(cohorts, issuer, training_weeks, max_lag, params['zone'])
    profile = pooled_profile(detail, max_lag)
    status = profile_status(detail, profile, params['min_history'], params['instability'], max_lag)
    donors = []
    borrowed = False
    if status != 'ok' and params['allow_borrow']:
        group = params['groups'].get(issuer)
        if group is not None:
            pool = []
            for other in sorted(cohorts):
                if other == issuer or params['groups'].get(other) != group:
                    continue
                other_detail, other_excluded = training_detail(
                    cohorts, other, training_weeks, max_lag, params['zone'],
                )
                if other_detail:
                    donors.append(other)
                    pool.extend(other_detail)
                    excluded.extend(other_excluded)
            other_profile = pooled_profile(pool, max_lag)
            other_status = profile_status(
                pool, other_profile, params['min_history'], params['instability'], max_lag,
            )
            if pool and other_status == 'ok':
                detail, profile, status, borrowed = pool, other_profile, 'ok', True
    return {
        'status': status,
        'profile': [round(value, 6) for value in profile],
        'training_weeks': sorted(item['week'] for item in detail),
        'training_week_count': len(detail),
        'training_event_count': sum(item['total'] for item in detail),
        'donor_issuers': donors,
        'borrowed': borrowed,
        'excluded_negative_lag_weeks': sorted(set(excluded)),
        'late_rates_after_maturity': {
            item['week']: round(item['late_rate'], 6) for item in detail if item['late_rate'] > 0
        },
    }, detail


def analyze(events, pending, params):
    """主入口：events 为全部首次 register 行，pending 为收到未投影的 register 行。"""
    weeks, cohorts = build_cohorts(events, params)
    issuer = params['issuer']
    cutoff = params['cutoff']
    max_lag = params['max_lag']
    params = {**params, 'rng': random.Random(params['seed'])}
    profile_info, detail = estimate_profile(cohorts, issuer, weeks, cutoff, params)
    profile = profile_info['profile']
    rows = []
    for week in weeks:
        items = cohorts[issuer][week]
        age = cohort_age(week, cutoff)
        known = sum(1 for item in items if parse_event_time(item['applied_at']) <= cutoff)
        mature = age >= max_lag
        row = {
            'business_week': week_label(week),
            'age_weeks': age,
            'mature': mature,
            'known_count': known,
            'observed_final_count': len(items) if mature else None,
        }
        if mature:
            row['status'] = 'mature'
            row['estimated_final_count'] = len(items)
            row['expected_outstanding_count'] = 0
            row['interval'] = {'lower': len(items), 'upper': len(items), 'point': len(items)}
        elif profile_info['status'] != 'ok' or not profile[min(age, max_lag)] or known == 0:
            # 已知量为零时没有可锚定的观测，即使历史比例非零也拒绝给确定估计。
            if profile_info['status'] != 'ok':
                row['status'] = profile_info['status']
            else:
                row['status'] = 'ratio_zero'
            row['estimated_final_count'] = None
            row['expected_outstanding_count'] = None
            row['interval'] = None
        else:
            interval = bootstrap_final(known, detail, age, params)
            if interval is None:
                row['status'] = 'ratio_zero'
                row['estimated_final_count'] = None
                row['expected_outstanding_count'] = None
                row['interval'] = None
            else:
                row['status'] = 'estimated'
                row['estimated_final_count'] = interval['point']
                row['expected_outstanding_count'] = max(0, interval['point'] - known)
                row['interval'] = interval
        rows.append(row)
    week_set = set(weeks)
    pending_rows = []
    for item in pending:
        if item['issuer'] != issuer:
            continue
        effective = parse_event_time(item['effective_at']).astimezone(params['zone'])
        key = week_key(effective)
        if key in week_set:
            pending_rows.append({
                'event_id': item['event_id'],
                'asset_id': item['asset_id'],
                'effective_at': item['effective_at'],
                'received_at': item['received_at'],
                'business_week': week_label(key),
            })
    pending_rows.sort(key=lambda item: (item['received_at'], item['event_id']))
    negatives = []
    for week in weeks:
        for item in cohorts[issuer][week]:
            if parse_event_time(item['applied_at']) < parse_event_time(item['effective_at']):
                negatives.append({
                    'event_id': item['event_id'],
                    'business_week': week_label(week),
                    'effective_at': item['effective_at'],
                    'applied_at': item['applied_at'],
                })
    residual_leak = any(
        cohort_age(week, cutoff) >= max_lag
        and profile_info['late_rates_after_maturity'].get(week_label(week), 0.0) > 0
        for week in weeks
    )
    # 把本版本在知识截止时已可知的登记事件冻结进文档，使旧版本可自证修订来源。
    known_pair_set = {
        (week_label(week_key(parse_event_time(item['effective_at']).astimezone(params['zone']))),
         item['event_id'])
        for week in weeks for item in cohorts[issuer][week]
        if parse_event_time(item['applied_at']) <= cutoff
    }
    known_pairs = [list(pair) for pair in sorted(known_pair_set)]
    return {
        'parameters': {
            'issuer': issuer,
            'start_week': week_label(params['start_week']),
            'end_week': week_label(params['end_week']),
            'knowledge_cutoff': params['cutoff'].astimezone(dt.timezone.utc)
            .isoformat(timespec='microseconds').replace('+00:00', 'Z'),
            'timezone': str(params['zone']),
            'max_maturity_lag_weeks': max_lag,
            'min_history_weeks': params['min_history'],
            'bootstrap_samples': params['samples'],
            'bootstrap_lower_quantile': params['lower_q'],
            'bootstrap_upper_quantile': params['upper_q'],
            'instability_threshold': params['instability'],
            'allow_borrow': params['allow_borrow'],
            'borrow_groups': params['groups'],
            'seed': params['seed'],
        },
        'profile': profile_info,
        'cohorts': rows,
        'received_not_projected': {
            'knowledge_cutoff_basis': 'applied_at',
            'count': len(pending_rows),
            'items': pending_rows,
        },
        'anomalies': {
            'negative_lag': {'count': len(negatives), 'items': negatives},
            'residual_underreporting_possible': residual_leak,
        },
        'known_event_pairs': known_pairs,
    }


def input_fingerprint(register_events, pending_events):
    """固定输入事件：补报后新事件会改变指纹，从而产生新版本分析。"""
    fixed = [
        [
            row['position'], row['event_id'], row['issuer'], row['asset_id'],
            row['effective_at'], row['received_at'], row['applied_at'], row['disposition'],
        ]
        for row in register_events
    ]
    fixed_pending = [
        [row['position'], row['event_id'], row['issuer'], row['asset_id'],
         row['effective_at'], row['received_at'], row['disposition']]
        for row in pending_events
    ]
    return digest({
        'register_events': fixed,
        'pending_register_events': fixed_pending,
        'max_position': max((row['position'] for row in register_events), default=0),
    })


def document_digest(document):
    payload = {key: value for key, value in document.items() if key not in {'analysis_id', 'created_at'}}
    return digest(payload)


def canonical_document(document):
    return canonical(document)
