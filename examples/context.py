import json
from pathlib import Path

def load():
    return json.loads((Path(__file__).with_name("context.json")).read_text())

def run():
    from registry_bridge.ledger.storage import Store
    from registry_bridge.ledger import issuers
    from registry_bridge.feeds import receiver
    from registry_bridge.reconcile import rebuild
    from registry_bridge.query import catalog
    import copy
    data = load()
    now = ['2026-01-01T00:00:00Z']
    with Store(':memory:', clock=lambda: now[0]) as store:
        for issuer in ['north', 'south']:
            issuers.add(store, issuer, '合成登记机构')
        seq = {'north': 0, 'south': 0}
        for row in data['records']:
            now[0] = row['received_at']
            seq[row['issuer']] += 1
            template = json.loads((Path(__file__).parents[1] / 'fixtures/demo.json').read_text())['messages'][1]
            message = copy.deepcopy(template)
            message.update(event_id=f"{row['issuer']}:{seq[row['issuer']]}", issuer=row['issuer'], sequence=seq[row['issuer']], asset_id=row['asset_id'])
            message['effective_at'] = row['effective_at']
            receiver.receive(store, message)
            receiver.receive(store, message)
        assert store.scalar('SELECT COUNT(*) FROM events') == len(data['records'])
        check = rebuild.check(store)
        return {'ok': check['ok'], 'records': len(data['records']), 'metrics': catalog.metrics(store)}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True))
