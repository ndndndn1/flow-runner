"""Offline, evidence-first practice agent. No network or model credentials."""
from __future__ import annotations
import argparse
import copy
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))
from flow_runner.workflow import validate_workflow, topological_order, bind_input, evaluate
from flow_runner.metadata import describe
from jsonschema import validate, ValidationError

BASE = Path(__file__).parent


def obj(properties, required=None):
    return {'type': 'object', 'properties': properties, 'required': required or list(properties),
            'additionalProperties': False}

STR = {'type': 'string', 'minLength': 1}
NUM = {'type': 'number', 'minimum': 0}
ROW = obj({'id': STR, 'village': {'enum': ['교동마을', '갯마을']}, 'product': STR,
           'quantity': NUM, 'unit': {'enum': ['kg', 'box']},
           'kg_per_box': {'anyOf': [{'type': 'number', 'exclusiveMinimum': 0}, {'type': 'null'}]},
           'source': STR})
ORDER = obj({'id': STR, 'product': STR, 'kg': {'type': 'integer', 'minimum': 1}, 'source': STR})
INPUT = obj({'inventory': {'type': 'array', 'items': ROW},
             'orders': {'type': 'array', 'items': ORDER},
             'dialogue': {'type': 'array', 'items': obj({'id': STR, 'speaker': STR, 'text': STR})},
             'answers': {'type': 'object', 'additionalProperties': {'anyOf': [
                 {'type': 'number', 'exclusiveMinimum': 0}, {'type': 'null'}]}},
             'capacity_kg': {'type': 'integer', 'minimum': 0}})


def extract(data):
    validate(data, INPUT)
    # Deliberately narrow grammar: unknown text becomes a question, never a fabricated fact.
    claims, unknown = [], []
    for turn in data['dialogue']:
        match = re.fullmatch(r'(.+?) 재고는 (\d+(?:\.\d+)?) kg입니다\.', turn['text'])
        if match:
            claims.append({'product': match[1], 'kg': float(match[2]), 'source': turn['id'],
                           'speaker': turn['speaker'], 'quote': turn['text']})
        else:
            unknown.append({'source': turn['id'], 'question': '발언의 품목·수량·단위를 확인해 주세요.',
                            'quote': turn['text']})
    return {'data': data, 'claims': claims, 'questions': unknown}


def reconcile(bundle):
    data = bundle['data']
    issues, questions, stock, evidence = [], list(bundle['questions']), {}, {}
    groups = {}
    for row in data['inventory']:
        groups.setdefault(row['product'], []).append(row)
    ids = [r['id'] for r in data['inventory']]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate inventory row id; cannot establish provenance')
    for product, rows in groups.items():
        total, sources, blocked = 0, [], False
        for row in rows:
            factor = row['kg_per_box']
            if row['unit'] == 'box' and factor is None:
                q = f"{row['id']}:kg_per_box"
                answer = data['answers'].get(q)
                questions.append({'source': row['source'], 'question': q, 'answer': answer})
                factor = answer
                if factor is None:
                    issues.append({'product': product, 'code': 'MISSING_UNIT', 'sources': [row['source']]})
                    blocked = True
                    continue
                sources.append('answer:' + q)
            total += row['quantity'] * (factor if row['unit'] == 'box' else 1)
            sources.append(row['source'])
        claims = [c for c in bundle['claims'] if c['product'] == product]
        if any(c['kg'] != total for c in claims):
            issues.append({'product': product, 'code': 'CONFLICT',
                           'sources': sources + [c['source'] for c in claims],
                           'ledger_kg': total, 'claims': claims})
            blocked = True
        stock[product] = 0 if blocked else total
        evidence[product] = sources + [c['source'] for c in claims]
    for claim in bundle['claims']:
        if claim['product'] not in groups:
            issues.append({'product': claim['product'], 'code': 'UNSUPPORTED_CLAIM',
                           'sources': [claim['source']]})
    orders, seen = [], {}
    for order in data['orders']:
        if order['id'] in seen:
            previous = seen[order['id']]
            if (previous['product'], previous['kg']) != (order['product'], order['kg']):
                raise ValueError('conflicting duplicate order: ' + order['id'])
            issues.append({'product': order['product'], 'code': 'DEDUPLICATED',
                           'sources': [previous['source'], order['source']]})
            continue
        seen[order['id']] = order
        orders.append(order)
    # Unknown language may hide constraints; do not authorize shipping until clarified.
    if bundle['questions']:
        stock = {p: 0 for p in stock}
        issues.append({'code': 'UNPARSED_DIALOGUE', 'sources': [q['source'] for q in bundle['questions']]})
    return {'stock': stock, 'evidence': evidence, 'orders': orders, 'issues': issues,
            'questions': questions, 'capacity_kg': data['capacity_kg']}


def decide(clean):
    # Complete-order fulfillment, in explicit FIFO fixture order. No profit/freshness claims.
    remaining = dict(clean['stock'])
    capacity = clean['capacity_kg']
    shipments, held = [], []
    for order in clean['orders']:
        product, kg = order['product'], order['kg']
        if remaining.get(product, 0) < kg or capacity < kg:
            held.append({'order': order['id'], 'reason': 'insufficient_verified_stock_or_capacity'})
            continue
        remaining[product] -= kg
        capacity -= kg
        shipments.append({'order': order['id'], 'product': product, 'kg': kg,
                          'sources': clean['evidence'][product] + [order['source']]})
    return {'status': 'needs_review' if held or any(i['code'] != 'DEDUPLICATED' for i in clean['issues'])
            else 'ready', 'shipments': shipments, 'held': held, 'issues': clean['issues'],
            'questions': clean['questions'], 'policy': 'verified-stock, whole-order FIFO'}


def verify(clean, result):
    known = {o['id']: o for o in clean['orders']}
    shipped, totals = set(), {}
    for item in result['shipments']:
        order = known.get(item['order'])
        if order is None or item['order'] in shipped:
            raise ValueError('unknown or duplicated shipment')
        if item['product'] != order['product'] or item['kg'] != order['kg']:
            raise ValueError('shipment differs from order')
        expected = clean['evidence'][item['product']] + [order['source']]
        if item['sources'] != expected:
            raise ValueError('missing or fabricated evidence')
        shipped.add(item['order'])
        totals[item['product']] = totals.get(item['product'], 0) + item['kg']
    if any(kg > clean['stock'].get(p, 0) for p, kg in totals.items()):
        raise ValueError('stock exceeded')
    if sum(totals.values()) > clean['capacity_kg']:
        raise ValueError('capacity exceeded')
    held = [h['order'] for h in result['held']]
    if len(held) != len(set(held)) or shipped & set(held) or shipped | set(held) != set(known):
        raise ValueError('order accounting failed')
    expected_status = 'needs_review' if held or any(i['code'] != 'DEDUPLICATED' for i in clean['issues']) else 'ready'
    if result['status'] != expected_status or result['issues'] != clean['issues'] or result['questions'] != clean['questions']:
        raise ValueError('review status or issue evidence altered')
    return dict(result, verified=True, shipped_kg=sum(totals.values()))


MODULES = {'extract': lambda x: extract(x['data']),
           'reconcile': lambda x: reconcile(x['bundle']),
           'decide': lambda x: decide(x['clean']),
           'verify': lambda x: verify(x['clean'], x['result'])}
# Local schemas enforce stage envelopes; field and invariant checks live above.
OUTPUT_KEYS = {'extract': {'data': 'object', 'claims': 'array', 'questions': 'array'},
               'reconcile': {'stock': 'object', 'evidence': 'object', 'orders': 'array', 'issues': 'array', 'questions': 'array', 'capacity_kg': 'integer'},
               'decide': {'status': 'string', 'shipments': 'array', 'held': 'array', 'issues': 'array', 'questions': 'array', 'policy': 'string'},
               'verify': {'status': 'string', 'shipments': 'array', 'held': 'array', 'issues': 'array', 'questions': 'array', 'policy': 'string', 'verified': 'boolean', 'shipped_kg': 'number'}}


def run(data):
    context = {'workflow': {'input': copy.deepcopy(data)}, 'steps': {}}
    trace = []
    try:
        spec = validate_workflow(json.loads((BASE / 'workflow.json').read_text()))
        steps = {s.id: s for s in spec.steps}
        for sid in topological_order({s.id: set(s.needs) for s in spec.steps}):
            step = steps[sid]
            payload = bind_input(step.bindings, context)
            name = step.module.split('/')[1].split('@')[0]
            keys = {'extract': ['data'], 'reconcile': ['bundle'], 'decide': ['clean'], 'verify': ['clean', 'result']}[name]
            validate(payload, obj({k: {'type': 'object'} for k in keys}))
            describe(payload, step.module + ':input', max_bytes=10*1024*1024)
            output = MODULES[name](payload)
            validate(output, obj({k: {'type': t} for k, t in OUTPUT_KEYS[name].items()}))
            trace.append({'step': sid, 'state': 'COMPLETE',
                          'input': describe(payload, step.module + ':input', max_bytes=10*1024*1024),
                          'output': describe(output, step.module + ':output', max_bytes=10*1024*1024)})
            context['steps'][sid] = {'output': output}
        return {'result': evaluate(spec.outputs['result'], context), 'trace': trace}
    except (ValueError, KeyError, TypeError, ValidationError) as exc:
        trace.append({'step': locals().get('sid', 'workflow'), 'state': 'FAIL', 'error': str(exc)})
        return {'result': {'status': 'failed', 'verified': False, 'shipments': [], 'error': str(exc)}, 'trace': trace}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        report = run(json.loads(args.input.read_text()))
    except (OSError, json.JSONDecodeError) as exc:
        report = {'result': {'status': 'failed', 'verified': False, 'shipments': [], 'error': str(exc)}, 'trace': []}
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(text + '\n')
    else:
        print(text)
    return 2 if report['result']['status'] == 'failed' else 0

if __name__ == '__main__':
    raise SystemExit(main())
