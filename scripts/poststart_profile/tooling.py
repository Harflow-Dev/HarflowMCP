"""Exact bounded candidate asset ownership and atomic generation."""
import hashlib
import os
from pathlib import Path
import tempfile
from .cases import canonical, fixtures
from .schemas import schemas
from .validation import validate_link
SPEC_NAMES=('ST04-poststart.md','ST04-poststart-schema.md')
def digest(raw): return hashlib.sha256(raw).hexdigest()
def read(path,limit=8388608):
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink!=1: raise ValueError('nonregular input')
    with path.open('rb') as stream: raw=stream.read(limit+1)
    if len(raw)>limit: raise ValueError('read budget')
    return raw

def expected_assets(specification_paths):
    paths=tuple(Path(p) for p in specification_paths)
    if tuple(p.name for p in paths)!=SPEC_NAMES: raise ValueError('exact specification pair required')
    assets={name:raw for name,(raw,_) in fixtures().items()}
    validate_link(assets['contain-request.json'],assets['certificate.json'],assets['replacement-request.json'])
    assets.update({name:canonical(doc) for name,doc in schemas().items()})
    entries=[{'filename':name,'bytes':len(raw),'sha256':digest(raw)} for name,raw in sorted(assets.items())]
    manifest={'format':'harflow.steering.poststart.candidate','v':1,
        'status':'specified_unimplemented','provenance':'synthetic_specification',
        'python_runtime_support':False,'rust_adoption':False,'runtime_qualified':False,
        'reserved_capability':'workflow-steering-poststart.v1','capability_advertised':False,
        'specifications':[{'path':'docs/decisions/'+p.name,'sha256':digest(read(p,131072))} for p in paths],
        'files':entries,'expectations':{name:expected for name,(_,expected) in fixtures().items()}}
    assets['manifest.json']=canonical(manifest)
    if len(assets['manifest.json'])>131072: raise ValueError('manifest budget')
    assets['manifest.sha256']=(digest(assets['manifest.json'])+'\n').encode()
    if sum(map(len,assets.values()))>8388608: raise ValueError('shared proof budget')
    return assets

def directory(path,names,missing):
    path=Path(path)
    if any(p.is_symlink() for p in (path,*path.parents)): raise ValueError('symlink directory')
    if not path.exists():
        if missing:return path
        raise ValueError('missing directory')
    if not path.is_dir(): raise ValueError('non-directory')
    found=set()
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.name not in names or len(found)>=len(names): raise ValueError('inventory mismatch')
            if entry.is_symlink() or not entry.is_file(follow_symlinks=False) or entry.stat(follow_symlinks=False).st_nlink!=1:
                raise ValueError('nonregular asset')
            found.add(entry.name)
    if not missing and found!=names: raise ValueError('inventory mismatch')
    return path

def generate(output_dir,specification_paths):
    assets=expected_assets(specification_paths); path=directory(output_dir,set(assets),True)
    path.mkdir(parents=True,exist_ok=True)
    for name,raw in assets.items():
        fd,temp=tempfile.mkstemp(prefix='.poststart-',dir=path)
        try:
            with os.fdopen(fd,'wb') as stream: stream.write(raw)
            os.replace(temp,path/name)
        finally:
            if os.path.exists(temp): os.unlink(temp)

def check(output_dir,specification_paths):
    assets=expected_assets(specification_paths); path=directory(output_dir,set(assets),False)
    for name,raw in assets.items():
        if read(path/name,len(raw))!=raw: raise ValueError('candidate bytes differ: '+name)
