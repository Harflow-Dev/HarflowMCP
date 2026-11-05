"""Self-contained Draft 2020-12 schemas matching the closed candidate syntax."""
from .validation import HEADER, IDENTITY, PROOFS, REVISIONS, DIGESTS, UUID_PATTERN, SHA_PATTERN
from .model import EVENTS, BOOLS

def obj(properties):
    return {'type':'object','properties':properties,'required':list(properties),'additionalProperties':False,'maxProperties':100}
STR={'type':'string','minLength':1,'maxLength':4096}
BOOL={'type':'boolean'}
NUM={'type':'number','exclusiveMinimum':0,'maximum':9007199254740991}
INT={'type':'integer','minimum':0,'maximum':9007199254740991}
SHA={**STR,'pattern':'^'+SHA_PATTERN+'$','maxLength':64}
UUID={**STR,'pattern':'^'+UUID_PATTERN+'$','maxLength':36}
IDENT=obj({key:INT if key in REVISIONS else SHA if key in DIGESTS else UUID if key in ('request_id','launch_id') else STR for key in IDENTITY})
VERIFIER=obj({'name':STR,'version':STR,'digest':SHA})
PROOF=obj(dict.fromkeys(PROOFS,BOOL))
def schemas():
    common={key:{'const':value,'type':'boolean' if type(value) is bool else 'integer' if type(value) is int else 'string'} for key,value in HEADER.items()}
    common['hypothetical']={'const':True,'type':'boolean'}
    def document(kind,fields): return obj({**common,'kind':{'const':kind,'type':'string'},**fields})
    contain=document('contain_request',{'request_id':UUID,'ledger_epoch':INT,'identity':IDENT,'scope':{'const':'original_launch_complete_owned_closure','type':'string'},'reason':{**STR,'pattern':r'\S'},
        'profile':obj({'name':STR,'version':STR,'digest':SHA,'expected_time_budgets':obj(dict.fromkeys(('stop','grace','execution','background'),NUM))})})
    replacement=document('replacement_request',{'request_id':UUID,'contain_request_id':UUID,'ledger_epoch':INT,'identity':IDENT,'certificate_digest':SHA,'effect_verifier':VERIFIER,'expected_remaining_attempts':INT})
    certificate=document('certificate',{'contain_request_id':UUID,'ledger_epoch':INT,'identity':IDENT,'proofs':PROOF,'effect_verifier':VERIFIER,'inventory':{'type':'array','minItems':1,'maxItems':100,
        'items':obj({'actor':STR,'generation':STR,'obligation':{'type':'string','enum':['required','incidental','unknown']},'settlement':{'type':'string','enum':['satisfied','failed','pending','unknown']}})}})
    variants=[]
    for kind,fields in EVENTS.items():
        props={'kind':{'const':kind,'type':'string'}}
        for field in fields: props[field]=BOOL if field in BOOLS else PROOF if field=='proofs' else INT if field=='remaining_attempts' else STR
        variants.append(obj(props))
    scenario=document('scenario',{'events':{'type':'array','maxItems':100,'items':{'oneOf':variants}}})
    result={'request.schema.json':{'oneOf':[contain,replacement]},'certificate.schema.json':certificate,'scenario.schema.json':scenario}
    return {name:{'$schema':'https://json-schema.org/draft/2020-12/schema', '$id':'urn:harflow:st04-poststart-v1:'+name,**schema} for name,schema in result.items()}
