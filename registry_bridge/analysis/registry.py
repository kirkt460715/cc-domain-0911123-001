"""登记量分析的台账读取、内容寻址持久化与滚动回看。"""
import datetime as dt
import json
import random

from registry_bridge.analysis import arrivals
from registry_bridge.errors import InvalidMessage, NotFound
from registry_bridge.feeds.message import canonical, digest, integer
from registry_bridge.ledger import audit

REGISTER_SQL = (
    'SELECT e.position,e.event_id,e.issuer,e.asset_id,e.effective_at,e.received_at,'
    "e.applied_at,e.disposition FROM events e JOIN ("
    'SELECT issuer,asset_id,MIN(position) AS position FROM events '
    "WHERE operation='register' AND disposition='applied' GROUP BY issuer,asset_id"
    ') f ON f.position=e.position ORDER BY e.position'
)
PENDING_SQL = (
    'SELECT position,event_id,issuer,asset_id,effective_at,received_at,disposition '
    "FROM events WHERE operation='register' AND disposition='pending' ORDER BY position"
)


def load_register_events(store, *, as_of=None):
    rows = store.all(REGISTER_SQL)
    if as_of is not None:
        rows = [row for row in rows if row['applied_at'] is not None and row['applied_at'] <= as_of]
    return rows


def load_pending_events(store):
    return store.all(PENDING_SQL)


def _normalize(p):
    return arrivals.validate_params({
        'issuer': p['issuer'],
        'timezone': p.get('timezone', 'UTC'),
        'start_week': p['start_week'],
        'end_week': p['end_week'],
        'knowledge_cutoff': p['knowledge_cutoff'],
        'max_maturity_lag_weeks': p.get('max_maturity_lag_weeks', 4),
        'min_history_weeks': p.get('min_history_weeks', 4),
        'bootstrap_samples': p.get('bootstrap_samples', 1000),
        'bootstrap_lower_quantile': p.get('bootstrap_lower_quantile', 0.1),
        'bootstrap_upper_quantile': p.get('bootstrap_upper_quantile', 0.9),
        'instability_threshold': p.get('instability_threshold'),
        'allow_borrow': p.get('allow_borrow', False),
        'borrow_groups': p.get('borrow_groups', {}),
        'seed': p.get('seed'),
    })


def run_analysis(store, p):
    params = _normalize(p)
    register_events = load_register_events(store)
    pending_events = load_pending_events(store)
    fingerprint = arrivals.input_fingerprint(register_events, pending_events)
    document = arrivals.analyze(register_events, pending_events, params)
    document['kind'] = 'arrival_analysis'
    document['input_fingerprint'] = fingerprint
    analysis_id = arrivals.document_digest(document)
    document['analysis_id'] = analysis_id
    document['created_at'] = store.clock()
    return document


def save_analysis(store, document):
    """内容寻址保存：同一输入与参数永远得到同一 id，旧版本不会被覆盖。"""
    if document['analysis_id'] != arrivals.document_digest(document):
        raise InvalidMessage('Analysis document digest does not match content')
    with store.transaction():
        existing = store.one(
            'SELECT analysis_id FROM arrival_analyses WHERE analysis_id=?', (document['analysis_id'],),
        )
        if existing is None:
            store.execute(
                'INSERT INTO arrival_analyses(analysis_id,kind,document,input_fingerprint,created_at) '
                'VALUES(?,?,?,?,?)',
                (document['analysis_id'], document['kind'], canonical(document),
                 document['input_fingerprint'], document['created_at']),
            )
            audit.append(store, 'arrival.analysis.saved', 'analyst', {
                'analysis_id': document['analysis_id'],
                'input_fingerprint': document['input_fingerprint'],
                'kind': document['kind'],
            })
            reused = False
        else:
            reused = True
    return {'analysis_id': document['analysis_id'], 'reused': reused, 'created_at': document['created_at']}


def create_analysis(store, p):
    document = run_analysis(store, p)
    saved = save_analysis(store, document)
    return {**saved, 'document': document}


def get_analysis(store, analysis_id):
    row = store.one('SELECT document FROM arrival_analyses WHERE analysis_id=?', (analysis_id,))
    if row is None:
        raise NotFound('Analysis does not exist')
    return json.loads(row['document'])


def list_analyses(store, *, kind=None, limit=100):
    integer(limit, 'limit', 1, 1000)
    if kind is not None:
        rows = store.all(
            'SELECT analysis_id,kind,input_fingerprint,created_at FROM arrival_analyses '
            'WHERE kind=? ORDER BY created_at DESC,analysis_id LIMIT ?', (kind, limit),
        )
    else:
        rows = store.all(
            'SELECT analysis_id,kind,input_fingerprint,created_at FROM arrival_analyses '
            'ORDER BY created_at DESC,analysis_id LIMIT ?', (limit,),
        )
    return {'items': rows, 'count': len(rows)}


def explain_revision(store, p, *, reference=None):
    """补报后创建新分析，并列出相对旧版本新增可知的登记来自哪些生效周。"""
    reference_id = p.get('reference_analysis_id', reference)
    document = run_analysis(store, p)
    if reference_id is None:
        saved = save_analysis(store, document)
        return {**saved, 'document': document, 'revision': {'reference_analysis_id': None, 'changes': []}}
    previous = get_analysis(store, reference_id)
    if previous['parameters']['issuer'] != document['parameters']['issuer']:
        raise InvalidMessage('Reference analysis belongs to another issuer')
    prior_events = {tuple(pair) for pair in previous['known_event_pairs']}
    current_events = {tuple(pair) for pair in document['known_event_pairs']}
    added = sorted(current_events - prior_events)
    by_week = {}
    for business_week, event_id in added:
        by_week.setdefault(business_week, []).append(event_id)
    changes = [
        {'business_week': week, 'newly_known_count': len(events), 'event_ids': sorted(events)}
        for week, events in sorted(by_week.items())
    ]
    note = None
    if not changes and previous['input_fingerprint'] == document['input_fingerprint']:
        note = 'input_unchanged'
    saved = save_analysis(store, document)
    revision = {
        'reference_analysis_id': reference_id,
        'reference_input_fingerprint': previous['input_fingerprint'],
        'current_input_fingerprint': document['input_fingerprint'],
        'changes': changes,
    }
    if note is not None:
        revision['note'] = note
    return {**saved, 'document': document, 'revision': revision}


# --------------------------------------------------------------------------- #
# 滚动回看
# ---------------------------------------------------------------------------

def _review_points(params, horizon_weeks):
    """回看点取每个业务周周一的同一钟点（UTC），从第一个成熟周之后开始。"""
    start = params['start_week']
    zone = params['zone']
    first_review = start + dt.timedelta(weeks=params['max_lag'] + 1)
    last_week = params['end_week']
    points = []
    cursor = first_review
    while cursor <= last_week:
        local = dt.datetime.combine(cursor, dt.time(0, 0), tzinfo=zone)
        points.append((cursor, local.astimezone(dt.timezone.utc)))
        cursor += dt.timedelta(weeks=1)
    if horizon_weeks is not None:
        integer(horizon_weeks, 'horizon_weeks', 1, 520)
        points = points[-horizon_weeks:]
    return points


def hindcast(store, p, *, horizon_weeks=None):
    """每个回看点只用那时可知的记录和已成熟周训练，评估历史预测误差。"""
    params = _normalize(p)
    integer(p.get('bootstrap_samples', 1000), 'bootstrap_samples', 100, 100000)
    full_events = load_register_events(store)
    full_pending = load_pending_events(store)
    full_fingerprint = arrivals.input_fingerprint(full_events, full_pending)
    # 当前真值：每个队列在今天的最终观测登记量（文档已按目标机构过滤）。
    truth_document = arrivals.analyze(full_events, full_pending, params)
    truth = {
        row['business_week']: _cohort_final_count(full_events, params, row['business_week'])
        for row in truth_document['cohorts']
    }
    reviews = []
    rng_seed = params['seed']
    for review_week, review_instant in _review_points(params, horizon_weeks):
        as_of = review_instant.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        known_events = load_register_events(store, as_of=as_of)
        review_params = {**params, 'cutoff': review_instant, 'seed': rng_seed}
        document = arrivals.analyze(known_events, [], review_params)
        model_errors = []
        naive_errors = []
        per_week = []
        for row in document['cohorts']:
            if row['mature']:
                continue
            final = truth.get(row['business_week'])
            if final is None:
                continue
            known = row['known_count']
            naive_errors.append(abs(known - final))
            estimate = row['estimated_final_count']
            if estimate is None:
                per_week.append({
                    'business_week': row['business_week'], 'known_count': known,
                    'final_count': final, 'model_estimate': None,
                    'model_abs_error': None, 'naive_abs_error': abs(known - final),
                    'status': row['status'],
                })
                continue
            model_errors.append(abs(estimate - final))
            per_week.append({
                'business_week': row['business_week'], 'known_count': known,
                'final_count': final, 'model_estimate': estimate,
                'model_abs_error': abs(estimate - final),
                'naive_abs_error': abs(known - final),
                'interval': row['interval'], 'status': row['status'],
            })
        reviews.append({
            'review_week': review_week.isoformat(),
            'knowledge_cutoff': as_of,
            'model_mae': round(sum(model_errors) / len(model_errors), 6) if model_errors else None,
            'naive_mae': round(sum(naive_errors) / len(naive_errors), 6) if naive_errors else None,
            'model_better_weeks': sum(
                1 for item in per_week
                if item['model_abs_error'] is not None
                and item['model_abs_error'] < item['naive_abs_error']
            ),
            'naive_better_weeks': sum(
                1 for item in per_week
                if item['model_abs_error'] is not None
                and item['model_abs_error'] > item['naive_abs_error']
            ),
            'refused_weeks': sum(1 for item in per_week if item['model_estimate'] is None),
            'weeks': per_week,
        })
    mechanism = detect_mechanism_change(store, params)
    document = {
        'kind': 'arrival_hindcast',
        'parameters': truth_document['parameters'],
        'input_fingerprint': full_fingerprint,
        'reviews': reviews,
        'mechanism_change': mechanism,
    }
    document['analysis_id'] = arrivals.document_digest(document)
    document['created_at'] = store.clock()
    return document


def _cohort_final_count(events, params, business_week):
    """目标机构在指定生效周的最终首次登记量（含当前尚未到截止时点的登记）。"""
    target = dt.date.fromisoformat(business_week)
    count = 0
    for event in events:
        if event['issuer'] != params['issuer']:
            continue
        local = arrivals.parse_event_time(event['effective_at']).astimezone(params['zone'])
        if arrivals.week_key(local) == target:
            count += 1
    return count


def detect_mechanism_change(store, params):
    """比较各机构前半段与后半段历史的登记滞后分布，标记迟报机制改变的机构。"""
    events = load_register_events(store)
    zone = params['zone']
    weeks = arrivals.enumerate_weeks(params['start_week'], params['end_week'])
    midpoint = weeks[len(weeks) // 2]
    issuers = sorted({event['issuer'] for event in events})
    rng = random.Random(params['seed'])
    flagged = []
    per_issuer = []
    for issuer in issuers:
        early, late = [], []
        for event in events:
            if event['issuer'] != issuer or event['applied_at'] is None:
                continue
            local = arrivals.parse_event_time(event['effective_at']).astimezone(zone)
            lag_seconds = (arrivals.parse_event_time(event['applied_at'])
                           - arrivals.parse_event_time(event['effective_at'])).total_seconds()
            lag_days = lag_seconds / 86400
            if arrivals.week_key(local) < midpoint:
                early.append(max(0, lag_days))
            else:
                late.append(max(0, lag_days))
        if len(early) < 5 or len(late) < 5:
            per_issuer.append({'issuer': issuer, 'status': 'insufficient_data'})
            continue
        observed = abs(_mean(late) - _mean(early))
        pooled = early + late
        trials = 200
        exceeded = 0
        for _ in range(trials):
            shuffled = pooled[:]
            rng.shuffle(shuffled)
            left = shuffled[:len(early)]
            right = shuffled[len(early):]
            if abs(_mean(right) - _mean(left)) >= observed:
                exceeded += 1
        p_value = (exceeded + 1) / (trials + 1)
        changed = p_value < 0.1
        entry = {
            'issuer': issuer,
            'early_mean_lag_days': round(_mean(early), 6),
            'late_mean_lag_days': round(_mean(late), 6),
            'permutation_p_value': round(p_value, 6),
            'mechanism_changed': changed,
        }
        per_issuer.append(entry)
        if changed:
            flagged.append(issuer)
    return {
        'split_week': midpoint.isoformat(),
        'permutation_trials': 200,
        'significance_level': 0.1,
        'flagged_issuers': flagged,
        'issuers': per_issuer,
    }


def _mean(values):
    return sum(values) / len(values) if values else 0.0
