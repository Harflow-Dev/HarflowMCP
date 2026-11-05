"""Standalone synthetic specification assets, not a Python runtime implementation."""
from .validation import Rejected, decode, require, supported, binding, integer, KINDS
from . import models

def classify(raw: bytes) -> dict:
    """Classify bounded specimen bytes without effects or runtime evidence."""
    try:
        value = decode(raw)
        require(isinstance(value, dict), 'envelope')
        require(value.get('format') == 'harflow.owner-runtime.profile' and integer(value.get('v'))
                and isinstance(value.get('kind'), str) and isinstance(value.get('binding'), dict)
                and isinstance(value.get('data'), dict), 'envelope')
        supported(value['v'] == 1
                  and value['kind'] in KINDS, 'envelope')
        binding(value['binding'])
        if value['kind'] == 'storage':
            return models.storage(value['data'], value['binding'])
        return getattr(models, value['kind'])(value['data'])
    except Rejected as error:
        return error.result

def fixtures():
    from .cases import fixtures as build
    return build()
