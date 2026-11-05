"""Pure validation of synthetic candidate specimens; no runtime authority."""
import json
import math
import re

MAX_BYTES = 24576
U64 = (1 << 64) - 1
KINDS = {'policy', 'permission', 'observation', 'storage', 'steering_guard'}

class Rejected(ValueError):
    def __init__(self, parse, reason):
        self.result = {'parse': parse, 'reason': reason}

def require(ok, reason):
    if not ok:
        raise Rejected('invalid', reason)

def supported(ok, reason):
    if not ok:
        raise Rejected('unsupported', reason)

def integer(value, low=0, high=U64):
    return type(value) is int and low <= value <= high

def token(value, length):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{%d}' % length, value) is not None

def process(value):
    return (isinstance(value, dict) and integer(value.get('pid'), 2, 2147483647)
            and integer(value.get('start_seconds'), 1)
            and integer(value.get('start_microseconds'), 0, 999999)
            and isinstance(value.get('boot_uuid'), str)
            and re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value['boot_uuid']) is not None)

def budgets(value, depth=0):
    require(depth <= 16, 'budget')
    if isinstance(value, str):
        try:
            size = len(value.encode('utf-8'))
        except UnicodeEncodeError:
            raise Rejected('invalid', 'encoding') from None
        require(size <= 4096, 'budget')
    elif isinstance(value, (list, dict)):
        if isinstance(value, list):
            require(len(value) <= 64, 'budget')
            items = value
        else:
            items = [part for pair in value.items() for part in pair]
        for item in items:
            budgets(item, depth + 1)

def scalar_check(value):
    if isinstance(value, str):
        value.encode('utf-8')
    elif isinstance(value, list):
        for item in value:
            scalar_check(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            scalar_check(key)
            scalar_check(item)

def lexical_depth(text):
    """Bound nesting before JSON parsing, respecting quoted/escaped delimiters."""
    depth = 0
    quoted = escaped = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char in ' \t\r\n,:':
            continue
        if char in ']}':
            depth = max(0, depth - 1)
            continue
        require(depth <= 16, 'budget')
        if char == '"':
            quoted = True
        elif char in '[{':
            depth += 1


def decode(raw):
    require(isinstance(raw, bytes) and len(raw) <= MAX_BYTES, 'budget')
    try:
        text = raw.decode('utf-8')
    except UnicodeError:
        raise Rejected('invalid', 'encoding') from None
    lexical_depth(text)
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate key')
            result[key] = value
        return result
    def constant(_):
        raise ValueError('nonfinite number')
    try:
        value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
        scalar_check(value)
        # JSON exponent overflow also yields nonfinite floats.
        def finite(item):
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError('nonfinite number')
            if isinstance(item, (list, dict)):
                for child in (item.values() if isinstance(item, dict) else item):
                    finite(child)
        finite(value)
    except (ValueError, UnicodeError, RecursionError):
        raise Rejected('invalid', 'encoding') from None
    budgets(value)
    return value

def binding(value):
    require(isinstance(value, dict), 'binding')
    require(all(token(value.get(key), 32) for key in ('operation', 'session', 'reservation', 'attempt', 'generation')), 'binding')
    require(process(value.get('owner')), 'binding')
    root = value.get('root')
    require(isinstance(root, dict) and integer(root.get('device')) and integer(root.get('inode'), 1) and integer(root.get('protocol')), 'binding')
    require(all(token(value.get(key), 64) for key in ('request_digest', 'configuration_digest')), 'binding')
    require('steering' in value, 'binding')
    steering = value['steering']
    if steering is not None:
        require(isinstance(steering, dict) and isinstance(steering.get('role'), str)
                and token(steering.get('checkpoint'), 32) and integer(steering.get('revision'))
                and token(steering.get('issued_attempt'), 32)
                and steering['issued_attempt'] == value['attempt'], 'binding')
    supported(root['protocol'] == 5, 'binding')
    if steering is not None:
        supported(steering['role'] in {'worker', 'builder', 'orchestrator'}, 'binding')

def enum_fields(data, enums, booleans=()):
    require(all(isinstance(data.get(key), str) for key in enums), 'fields')
    require(all(type(data.get(key)) is bool for key in booleans), 'fields')

def check_enums(data, enums):
    supported(all(data[key] in choices.split() for key, choices in enums.items()), 'enum')
