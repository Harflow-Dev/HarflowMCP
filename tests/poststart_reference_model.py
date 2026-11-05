"""Independent test-only oracle derived from the approved poststart policy.

No implementation imports: evidence, authority and effects remain separate.
"""
def schema_accepts(value, schema, root=None):
    """Small independent JSON Schema evaluator, rejecting unknown assertions.

    Only a closed supported keyword set is permitted so omissions cannot silently
    convert a schema agreement test into an acceptance test.
    """
    if isinstance(schema, bool):
        return schema
    root = schema if root is None else root
    annotations = {'$schema', '$id', 'title', 'description', '$defs', 'examples', 'default', '$comment'}
    assertions = {'$ref', 'type', 'const', 'enum', 'required', 'properties',
                  'additionalProperties', 'maxProperties', 'minProperties', 'items', 'minItems', 'maxItems', 'uniqueItems',
                  'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum',
                  'minLength', 'maxLength', 'pattern', 'allOf', 'anyOf', 'oneOf', 'not'}
    unknown = set(schema) - annotations - assertions
    if unknown:
        raise AssertionError(f'unhandled schema keywords: {unknown}')
    if '$ref' in schema:
        ref = schema['$ref']
        assert ref.startswith('#/'), ref
        target = root
        for part in ref[2:].split('/'):
            target = target[part.replace('~1', '/').replace('~0', '~')]
        if not schema_accepts(value, target, root):
            return False
    types = {'object': lambda x: isinstance(x, dict), 'array': lambda x: isinstance(x, list),
             'string': lambda x: isinstance(x, str), 'boolean': lambda x: type(x) is bool,
             'integer': lambda x: type(x) is int or (type(x) is float and x.is_integer()), 'number': lambda x: type(x) in (int, float),
             'null': lambda x: x is None}
    if 'type' in schema:
        choices = schema['type'] if isinstance(schema['type'], list) else [schema['type']]
        if not any(types[t](value) for t in choices):
            return False
    def equal(a,b):
        return ((type(a) in (int,float) and type(b) in (int,float)) or type(a) is type(b)) and a == b
    if 'const' in schema and not equal(value,schema['const']):
        return False
    if 'enum' in schema and not any(equal(value,v) for v in schema['enum']):
        return False
    for keyword, check in [('allOf', all), ('anyOf', any)]:
        if keyword in schema and not check(schema_accepts(value, s, root) for s in schema[keyword]):
            return False
    if 'oneOf' in schema and sum(schema_accepts(value, s, root) for s in schema['oneOf']) != 1:
        return False
    if 'not' in schema and schema_accepts(value, schema['not'], root):
        return False
    if isinstance(value, dict):
        if len(value) < schema.get('minProperties', 0) or len(value) > schema.get('maxProperties', float('inf')):
            return False
        if not set(schema.get('required', ())) <= set(value):
            return False
        properties = schema.get('properties', {})
        for key, item in value.items():
            rule = properties.get(key, schema.get('additionalProperties', True))
            if not schema_accepts(item, rule, root):
                return False
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0) or len(value) > schema.get('maxItems', float('inf')):
            return False
        if schema.get('uniqueItems') and any(value[i] == value[j] for i in range(len(value)) for j in range(i)):
            return False
        if 'items' in schema and not all(schema_accepts(v, schema['items'], root) for v in value):
            return False
    if isinstance(value, str):
        import re
        if len(value) < schema.get('minLength', 0) or len(value) > schema.get('maxLength', float('inf')):
            return False
        if 'pattern' in schema and re.search(schema['pattern'], value) is None:
            return False
    if type(value) in (int, float):
        for key, predicate in [('minimum', lambda a,b:a>=b), ('maximum', lambda a,b:a<=b),
                               ('exclusiveMinimum', lambda a,b:a>b), ('exclusiveMaximum', lambda a,b:a<b)]:
            if key in schema and not predicate(value, schema[key]):
                return False
    return True


class ProtocolOracle:
    """Test-only state and external effect ledger, independently policy derived.

    Durable issued commands are distinct from the volatile capability to deliver
    them. A crash destroys that capability even when no effect was delivered.
    """
    def __init__(self, proof_names):
        self.proof_names = tuple(proof_names)
        self.intent = None
        self.replacement = None
        self.fenced = False
        self.certificate = False
        self.released = False
        self.stop_status = 'none'
        self.dispatch_status = 'none'
        self.stop_handoff = False
        self.dispatch_handoff = False
        self.effects = {'stop': 0, 'dispatch': 0}
        self.accepted_result = False
        self.siblings = []
        self.remaining_attempts = None
        self.complete = False
        self.evidence_revision = 0
        self.certificate_revision = None
        self.lineage = None
        self.context = dict.fromkeys(('authority','effect_safe','budget','configuration','claims'),True)

    @staticmethod
    def binding(e):
        return tuple(e[k] for k in ('request_id','epoch','payload','authority','identity'))

    def apply(self, e):
        k = e['kind']
        if k == 'admit':
            if self.intent is None and not self.accepted_result and not e['result_accepted']:
                if self.context['authority'] and all(e[x] for x in ('authority','capacity','enrolled','single_provider','scope_exact','qualified_profile','qualified_budgets')):
                    self.intent = self.binding(e)
                    self.fenced = True
        elif k == 'accept_result' and not self.fenced:
            self.accepted_result = True
        elif k == 'prepare_stop':
            if self.fenced and self.stop_status == 'none':
                self.stop_status = 'unknown'
                self.stop_handoff = True
        elif k == 'deliver_stop':
            if self.stop_handoff:
                self.effects['stop'] += 1
                self.stop_handoff = False
        elif k == 'stop_ack':
            if self.effects['stop'] and self.stop_status == 'unknown':
                self.stop_status = 'acknowledged'
        elif k == 'observe' and self.fenced and self.stop_status != 'none':
            self.evidence_revision += 1
            self.certificate_revision = None
            self.complete = (e['inventory_complete'] and e['obligations_settled']
                             and all(e['proofs'].get(p) is True for p in self.proof_names))
            self.certificate = False
            self.released = False
            self.stop_handoff = self.dispatch_handoff = False
        elif k == 'certify' and self.complete:
            self.certificate = True
            self.certificate_revision = self.evidence_revision
        elif k == 'release' and self.certificate:
            self.released = True
        elif k == 'replace' and self.replacement is None:
            if (self.certificate and self.released and self.intent is not None
                and e['epoch'] == self.intent[1] and e['identity'] == self.intent[-1]
                and e['request_id'] != self.intent[0]
                and e['authority'] and e['effect_safe'] and e['remaining_attempts'] > 0
                and all(self.context.values())):
                self.replacement = self.binding(e)
                self.remaining_attempts = e['remaining_attempts']-1
                import hashlib, json
                bound={key:e[key] for key in ('request_id','epoch','payload','authority','identity','effect_safe','remaining_attempts')}
                token=hashlib.sha256(json.dumps(bound,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                self.lineage={'supersedes':self.intent[0], 'identity':e['identity'],
                              **{field:prefix+token for field,prefix in (('decision_id','control-'),('attempt_id','attempt-'),('launch_id','launch-'))}}
        elif k == 'prepare_dispatch':
            if (self.replacement is not None and self.dispatch_status == 'none'
                and self.certificate and self.released
                and all(e[x] for x in ('authority','effect_safe','budget','configuration','claims'))
                and all(self.context.values())):
                self.dispatch_status = 'unknown'
                self.dispatch_handoff = True
        elif k == 'deliver_dispatch':
            if self.dispatch_handoff:
                self.effects['dispatch'] += 1
                self.dispatch_handoff = False
        elif k == 'dispatch_ack':
            if self.effects['dispatch'] and self.dispatch_status == 'unknown':
                self.dispatch_status = 'acknowledged'
        elif k == 'context_change':
            self.context = {key:e[key] for key in self.context}
            self.stop_handoff = self.dispatch_handoff = False
        elif k in ('crash','recover'):
            self.stop_handoff = self.dispatch_handoff = False
        elif k == 'sibling_result' and e['id'] not in self.siblings:
            self.siblings.append(e['id'])
        return self

    def projection(self):
        return {'effects': dict(self.effects), 'intent': self.intent is not None,
                'replacement': self.replacement is not None, 'fenced': self.fenced,
                'certificate': self.certificate, 'released': self.released,
                'stop_status': self.stop_status, 'dispatch_status': self.dispatch_status,
                'accepted_result': self.accepted_result, 'siblings': list(self.siblings),
                'remaining_attempts': self.remaining_attempts, 'lineage':self.lineage,
                'evidence_revision':self.evidence_revision, 'certificate_revision':self.certificate_revision,
                'original_attempt_charged':True, 'model_decisions':0, 'old_callback_mutations':0}
