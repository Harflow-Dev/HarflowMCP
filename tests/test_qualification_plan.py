from types import SimpleNamespace
import pytest

from harflow_mcp import qualification_plan as plan


def test_features_select_only_requested_permissions():
    assert plan.required_features() == ()
    assert plan.required_features(allowed_commands=[]) == ()
    assert plan.required_features(caller_approvals=True) == ('caller_approval_tools',)
    assert plan.required_features(caller_approvals=True, caller_write_approvals=True, allowed_commands=['x']) == ('caller_approval_tools', 'caller_write_tools', 'allowed_commands')


def test_plain_task_does_not_inspect_unneeded_binary(monkeypatch):
    from harflow_mcp.backends import qualification
    monkeypatch.setattr(qualification, 'identify', lambda *_: pytest.fail('unneeded identity inspection'))
    result = plan.discover([('task', {'backend': 'not-installed'})])
    assert result == {'qualification_version': 1, 'discovery_only': True, 'requirements': [{'id': 'task', 'backend': 'not-installed', 'required_features': [], 'features': {}}]}


def test_discovery_deduplicates_identity_preserves_origins_and_never_requires(monkeypatch):
    from harflow_mcp import backends
    from harflow_mcp.backends import qualification
    identity = qualification.Identity('example', '1', '/example', 'a'*64, 'darwin')
    calls=[]
    monkeypatch.setattr(backends, 'get', lambda _: SimpleNamespace(name='example'))
    monkeypatch.setattr(qualification, 'identify', lambda backend: calls.append(backend.name) or identity)
    monkeypatch.setattr(qualification, 'inspect', lambda backend, **kwargs: {'caller_approval_tools': {'ok': True, 'source': 'seeded', 'reason': 'seed', 'identity': kwargs['identity'].__dict__}})
    monkeypatch.setattr(qualification, 'require', lambda *a, **kw: pytest.fail('provider self-test'))
    result=plan.discover([(name, {'backend': 'example', 'caller_approvals': True}) for name in ['a', 'b']])
    assert calls == ['example']
    assert [r['id'] for r in result['requirements']] == ['a','b']
    for row in result['requirements']:
        assert row['identity'] == row['features']['caller_approval_tools']['identity']
        assert 'unavailable_reason' not in row


def test_missing_required_feature_is_unavailable(monkeypatch):
    from harflow_mcp import backends
    from harflow_mcp.backends import qualification
    identity=qualification.Identity('example','1','/example','a'*64,'darwin')
    monkeypatch.setattr(backends, 'get', lambda _: None)
    monkeypatch.setattr(qualification, 'identify', lambda _: identity)
    monkeypatch.setattr(qualification, 'inspect', lambda *a, **kw: {})
    row=plan.discover([('task', {'backend':'example','allowed_commands':['x']})])['requirements'][0]
    assert row['features']['allowed_commands']['ok'] is False
    assert row['features']['allowed_commands']['identity'] == row['identity']
    assert row['unavailable_reason']


def test_unavailable_binary_does_not_invent_identity(monkeypatch):
    from harflow_mcp import backends
    from harflow_mcp.backends import qualification
    monkeypatch.setattr(backends, 'get', lambda _: None)
    def fail(_): raise OSError('missing')
    monkeypatch.setattr(qualification, 'identify', fail)
    row=plan.discover([('task',{'backend':'example','caller_approvals':True})])['requirements'][0]
    assert 'identity' not in row
    assert row['unavailable_reason']=='missing'


def test_workflow_origins_include_children_fallbacks_and_on_demand():
    agent={'backend':'example','fallbacks':[{'backend':'fallback'}]}
    tree={'root_workflow_id':'root','edges':[{'to':'child','orchestrator_mode':'child'}],'workflows':{ident:{'definition':{'orchestrator':agent,'nodes':[{'id':'n','type':'agent','agent':agent}], 'on_demand_nodes':[{'id':'d','agent':agent}]}} for ident in ['root','child']}}
    rows=plan.workflow_candidates(tree)
    assert len(rows)==12
    assert {r[0] for r in rows} == {f'{ident}/{kind}/{position}' for ident in ['root','child'] for kind in ['orchestrator','node/n','on-demand/d'] for position in [0,1]}


def test_requirements_budget_rejects_without_inspecting(monkeypatch):
    with pytest.raises(ValueError, match='4096'):
        plan.discover([('task', {'backend':'x'})]*4097)


async def test_task_discovery_returns_before_qualifier(tmp_path, monkeypatch):
    from harflow_mcp import tasks, backends, qualification_plan
    backend = backends.get('claude')
    def fail(*a, **kw): pytest.fail('qualification provider self-test')
    monkeypatch.setattr(backend, 'qualify_caller_approvals', fail)
    monkeypatch.setattr(backend, 'qualify_allowed_commands', fail)
    monkeypatch.setattr(qualification_plan, 'discover', lambda items: {'items': items})
    registry=tasks.TaskRegistry(log_dir=tmp_path/'state'/'tasks')
    result=await registry.preview_permissions(tmp_path,backend=backend,caller_approvals=True,allowed_commands=['make test'],qualification_only=True)
    assert result['items'][0][1]['caller_approvals'] is True
    assert result['items'][0][1]['allowed_commands'] == ('make test',)


def test_workflow_discovery_returns_before_native_owner_qualifiers(tmp_path, monkeypatch):
    from harflow_mcp import workflows, workflow_native_policy, qualification_plan
    raw={'name':'test','orchestrator':{'backend':'codex'}, 'nodes':[{'id':'start','type':'start','prompt':'Go'},{'id':'work','type':'agent','instructions':'Work','agent':{'backend':'codex'}},{'id':'end','type':'end'}], 'connections':[{'id':'a','source':'start','target':'work'},{'id':'b','source':'work','target':'end'}]}
    store=workflows.WorkflowStore(tmp_path/'state')
    store.save('test',raw)
    def fail(*a,**kw): pytest.fail('owner qualification')
    monkeypatch.setattr(workflow_native_policy,'plan_owner_contracts',fail)
    monkeypatch.setattr(qualification_plan,'discover',lambda items:{'items':items})
    result=workflows.preview_workflow_run('test',tmp_path,root=store.root,qualification_only=True)
    assert len(result['items'])==2


def test_repeated_feature_refreshes_shared_revision_once(monkeypatch,capsys):
    from harflow_mcp import ctl,backends
    from harflow_mcp.backends import qualification
    backend=SimpleNamespace(name='example',qualification_features=lambda:{'a':SimpleNamespace(revision='one'),'b':SimpleNamespace(revision='one')})
    monkeypatch.setattr(backends,'get',lambda _:backend)
    calls=[]
    def require(_,feature,refresh=False):
        calls.append((feature,refresh))
        return SimpleNamespace(as_dict=lambda:{'ok':True})
    monkeypatch.setattr(qualification,'require',require)
    assert ctl._cmd_qualify(SimpleNamespace(backend='example',feature=['b','a','b'],json=True))==0
    assert calls==[('b',True),('a',False)]


def test_ordinary_workflow_preview_preserves_owner_validation_order(tmp_path, monkeypatch):
    from harflow_mcp import workflows, workflow_native_policy, workflow_resources
    raw={'name':'test','orchestrator':{'backend':'codex'}, 'nodes':[{'id':'start','type':'start','prompt':'Go'},{'id':'work','type':'agent','instructions':'Work','agent':{'backend':'codex'}},{'id':'end','type':'end'}], 'connections':[{'id':'a','source':'start','target':'work'},{'id':'b','source':'work','target':'end'}]}
    store=workflows.WorkflowStore(tmp_path/'state')
    store.save('test',raw)
    calls=[]
    def owner(*a,**kw):
        calls.append('owner')
        raise ValueError('owner refusal sentinel')
    def scopes(*a,**kw):
        calls.append('scope')
        pytest.fail('ordinary preview changed validation order')
    monkeypatch.setattr(workflow_native_policy,'plan_owner_contracts',owner)
    monkeypatch.setattr(workflow_resources,'validate_tree_scopes',scopes)
    with pytest.raises(ValueError,match='owner refusal sentinel'):
        workflows.preview_workflow_run('test',tmp_path,root=store.root)
    assert calls==['owner']


def test_public_cli_parses_cached_only_and_repeated_features():
    from harflow_mcp import ctl
    parser = ctl._build_parser()[0]
    task = parser.parse_args(['permission-preview', '--repo', '/repo', '--backend', 'claude', '--qualification-only', '--json'])
    assert task.qualification_only is True
    workflow = parser.parse_args(['workflow-preview', 'example', '--repo', '/repo', '--qualification-only', '--json'])
    assert workflow.name == 'example' and workflow.qualification_only is True
    refresh = parser.parse_args(['qualify', 'claude', '--feature', 'caller_approval_tools', '--feature', 'caller_write_tools', '--json'])
    assert refresh.feature == ['caller_approval_tools', 'caller_write_tools']


def test_response_budget_rejects_complete_result_without_truncation():
    with pytest.raises(ValueError, match='1 MiB'):
        plan.discover([('a' * (1024 * 1024), {'backend': 'example'})])


@pytest.mark.parametrize('mode', ['current_only', 'mixed', 'nested_child'])
def test_real_preview_uses_actual_orchestrator_owners(tmp_path, monkeypatch, mode):
    from harflow_mcp import workflows
    from harflow_mcp.backends import qualification
    def definition(name, approvals=False):
        return {'name':name,'routing_mode':'explicit','orchestrator':{'backend':'claude' if approvals else 'codex', **({'caller_approvals':True} if approvals else {})},'nodes':[{'id':'start','type':'start','prompt':'Go'},{'id':'work','type':'agent','instructions':'Work','agent':{'backend':'codex'}},{'id':'end','type':'end'}],'connections':[{'id':'a','source':'start','target':'work'},{'id':'b','source':'work','target':'end'}]}
    store=workflows.WorkflowStore(tmp_path/'state')
    nested=store.save('nested',definition('nested',True))
    current=definition('current',True)
    if mode=='nested_child':
        current['nodes'].append({'id':'nested-call','type':'workflow','workflow_ref':{'workflow_id':nested['workflow_id']},'orchestrator_mode':'child'})
    if mode=='nested_child':
        current['connections']=[{'id':'a','source':'start','target':'work'},{'id':'b','source':'work','target':'nested-call'},{'id':'c','source':'nested-call','target':'end'}]
    current=store.save('current',current)
    root=definition('root')
    root['nodes'].append({'id':'current-call','type':'workflow','workflow_ref':{'workflow_id':current['workflow_id']},'orchestrator_mode':'current'})
    if mode=='mixed':
        root['nodes'].append({'id':'child-call','type':'workflow','workflow_ref':{'workflow_id':current['workflow_id']},'orchestrator_mode':'child'})
    chain=['start','work','current-call',*(['child-call'] if mode=='mixed' else []),'end']
    root['connections']=[{'id':str(i),'source':a,'target':b} for i,(a,b) in enumerate(zip(chain,chain[1:]))]
    root=store.save('root',root)
    inspected=[]
    def identify(backend):
        inspected.append(backend.name)
        raise OSError('binary unavailable')
    monkeypatch.setattr(qualification,'identify',identify)
    result=workflows.preview_workflow_run('root',tmp_path,root=store.root,qualification_only=True)
    ids=[row['id'] for row in result['requirements']]
    assert ids.count(f"{root['workflow_id']}/orchestrator/0")==1
    assert (f"{current['workflow_id']}/orchestrator/0" in ids)==(mode=='mixed')
    assert (f"{nested['workflow_id']}/orchestrator/0" in ids)==(mode=='nested_child')
    assert f"{current['workflow_id']}/node/work/0" in ids
    assert inspected==([] if mode=='current_only' else ['claude'])


def test_workflow_enumeration_stops_at_origin_budget():
    class CountedNodes:
        visits=0
        def __iter__(self):
            for index in range(10000):
                self.visits+=1
                yield {'id':str(index),'type':'agent','agent':{'backend':'example'}}
    nodes=CountedNodes()
    tree={'root_workflow_id':'root','workflows':{'root':{'definition':{'orchestrator':{'backend':'example'},'nodes':nodes}}}}
    with pytest.raises(ValueError,match='4096'):
        plan.workflow_candidates(tree)
    assert nodes.visits==4096


def test_repeated_large_facts_budget_stops_serialization_early(monkeypatch):
    from harflow_mcp import backends
    from harflow_mcp.backends import qualification
    identity=qualification.Identity('example','1','/example','a'*64,'darwin')
    monkeypatch.setattr(backends,'get',lambda _:None)
    monkeypatch.setattr(qualification,'identify',lambda _:identity)
    monkeypatch.setattr(qualification,'inspect',lambda *a,**kw:{'caller_approval_tools':{'ok':True,'source':'seeded','reason':'seed','identity':identity.__dict__,'facts':{'payload':'x'*60000}}})
    encoder=plan.json.JSONEncoder
    encoded=[]
    class CountingEncoder(encoder):
        def iterencode(self,value,*a,**kw):
            encoded.append(value['id'])
            return super().iterencode(value,*a,**kw)
    monkeypatch.setattr(plan.json,'JSONEncoder',CountingEncoder)
    with pytest.raises(ValueError,match='1 MiB'):
        plan.discover([(str(i),{'backend':'example','caller_approvals':True}) for i in range(4096)])
    assert len(encoded)<20


def test_unicode_wire_budget_matches_actual_ctl_envelope(monkeypatch):
    from harflow_mcp.wire_contracts import envelope,CTL_FORMAT
    import json
    candidate={'backend':'example'}
    result=plan.discover([('界'*100000,candidate)])
    assert len((json.dumps(envelope(CTL_FORMAT,{'result':result}))+'\n').encode()) <= 1024*1024
    with pytest.raises(ValueError,match='1 MiB'):
        plan.discover([('界'*180000,candidate)])


def test_qualify_unavailable_fallback_has_bounded_source(monkeypatch,capsys):
    import json
    from harflow_mcp import ctl,backends
    from harflow_mcp.backends import qualification
    from harflow_mcp.backends.base import UnsupportedCapability
    backend=SimpleNamespace(name='example',qualification_features=lambda:{'a':SimpleNamespace(revision='one')})
    monkeypatch.setattr(backends,'get',lambda _:backend)
    def fail(*a,**kw):raise UnsupportedCapability('x'*2000)
    monkeypatch.setattr(qualification,'require',fail)
    monkeypatch.setattr(qualification,'inspect',fail)
    assert ctl._cmd_qualify(SimpleNamespace(backend='example',feature=['a'],json=True))==1
    row=json.loads(capsys.readouterr().out)['result']['features']['a']
    assert row=={'ok':False,'source':'unavailable','reason':'x'*1000}
