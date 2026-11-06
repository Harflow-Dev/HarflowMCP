"""Maintainer-accepted, exact native conversation profile qualification.

No provider probes run here. Native no-tool protocol feasibility is not a coding
permission qualification. Accepted rows are maintained source evidence, not a
caller-writable receipt store; the list stays empty until reviewed runtime/profile
qualification. Every execution owner independently identifies its current runtime.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any

from . import qualification
from .base import Invocation
from .conversation import ConversationLaunch, ConversationRefused
from .conversation_native import SUPPORTED_PROTOCOL_VERSIONS, conversation_profile, restore_conversation_port

FEATURE = "conversation_delivery_v1"
REVISION = "native-conversation-profile-v1"
# Each accepted row binds runtime_fingerprint + profile_fingerprint + evidence_sha256.
# This is deliberately not populated from the earlier literal-text native smokes.
ACCEPTED_PROFILES: tuple[dict[str, str], ...] = (
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': '908237ec4942171580f8b95591452a1b7df279fb6e904f80468a4fb2e0e593b3', 'evidence_sha256': 'b8f7a3b12d76406b04336a590748fd0cefb5bf09184cd3224bf12d0b7bba46c6'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': 'e36da891ba0f63152bcdb7e3a8943e779303ae51589021648c7d92d737fc8805', 'evidence_sha256': '2d90e319660f259f374b72fd384040fa73a0d39bd5414b7192f9cb26ab5f0f56'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': '7bbfdf98fc01206434ef70c3b263f967e3f8e45b6e47765d59440bc909059529', 'evidence_sha256': 'eeb091aa85f9335de1d174f3f3ad1070a33181b84c91063b1f07436156708770'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': '73cf9ba6c15a89aba215b03b47375c7120be5b6d76577de071cb5dc84b075b11', 'evidence_sha256': '6c948f11ad50d92bee5cb24edef044a68cd094bb7543b5f0c48b497c5b02f6fb'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': '6e613d2f479105cc241139be91fc471a603ba216e7dfb6c7d8ae6178a646b283', 'evidence_sha256': '69f2c0f6f9aa36d5748901706ca663eb8f19ae332b9bc932d8359ba7478350c4'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': '743864381d010e274de9414f4b601e8971e53943c1e4edef87fe4436800e7ffd', 'evidence_sha256': 'c59289a7f1f1c1995657ee867dc9d05689ab8028c1506c659f3cb1c5ce570a26'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': 'a9e560efcf85e4d76707df95f7387e04d0fc596c538d1f8918a4b84e79efd0d1', 'evidence_sha256': 'd265736f6fba6597dccb68a6aaacc9c7ecde377387520e81836b7ffc841e4bf1'},
    {'runtime_fingerprint': '811cc5ca2187839a81ee08680501ed2c4d09943148af531270994a5da26466da', 'profile_fingerprint': 'f163b687ac7d10446742510b4e1754c8bf983b46484215444419cacc0ed12781', 'evidence_sha256': '6a1a0c30deeb41e9d4270b894a84b1e61ba819dea04ed92e5c7902a01e12c3e4'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '5d03bb246e471ac8339c965137e56b4421a929db38dfa5d98cc74440409c136d', 'evidence_sha256': '5a1e363b0ab125516cb919175ef655f4503775abe1a10bb75f534672de0628cf'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '81895e8c05bdc376da2acdac004d4603fdd044aa8855c5fe4fda68834de9d9c0', 'evidence_sha256': 'ab99f84be01b23a13fc9b6e7c2fd5ca91a0f5bcd7e5aa1177d5facd1626cfafe'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '9316c870469b810c5ffd4f4b19f1f008715a8a884c494c2d2ee8a1c1a79f72f8', 'evidence_sha256': '87b9576196d29069e9ae6455e883d86b26690c119c4a40a97f4b39d433272ae4'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': 'c260752385e625b19f98c64221f166a07ffe783934ac330f640aef3d661f5b31', 'evidence_sha256': 'ce70f1e5e3f9b6d7fcadb1730214db15a93956682a5a710fdfbb6bc93d775da0'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '6dc7228ca7b7963e298772dff71fdd6944baba1d1f76d5391c7f1e05f025b5c3', 'evidence_sha256': 'f69b6f21681566fcedc4ecc8a30902d50415e21f058896fc8fefbe3a3ffc980c'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '093b057f5421284d1ae7007d073fecef1837a40f98382d9a02b67222d50a0768', 'evidence_sha256': '48f1e7975aed5e1ce74d6bef68cf1dcc6798fb70eca56ee76fdf35ebf684bead'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '91214e2f347a64fcd3a917c72a09454edf9e045b42d35fe871e890be292249da', 'evidence_sha256': '2e7b1d69c91edd09232de1078a2e8b856776bf259f0638dab0832320843d693b'},
    {'runtime_fingerprint': '0ef55770c3822a89c8d6e1f01c1869bbb6148974ecfdd5e21b4aa57863fa3781', 'profile_fingerprint': '168abf9f16bab4688c5b0e22ac02698446106ccf40853e3c2b846f8c3e851033', 'evidence_sha256': '5b50e056ac93d1154776242ad314a6ecf2dc3d1e199cf62adc328b8477b223b6'},
    {'runtime_fingerprint': '238a26456b4c71c833458d74d650b2ff3d76252cdffac67efb6a2bbce3a7bc49', 'profile_fingerprint': 'bc938182725f02e227a5179fe8c82f17b40b854de5d524ea5ce97b2e857b4cf4', 'evidence_sha256': '27d5c0f43f0513fe353e05bb5a8fcf4c0a59f4d917d7ccf5054eb92480ef88bd'},
    {'runtime_fingerprint': '238a26456b4c71c833458d74d650b2ff3d76252cdffac67efb6a2bbce3a7bc49', 'profile_fingerprint': '8b9cd564b1ed4820672bcb7b2e747a127ac1f91879db7b5ebb22655a33896689', 'evidence_sha256': '6afeeb86bc8ec80d033a7e17aaa203e82aaa6acf8212afe2d46fe5c30e60da98'},
    {'runtime_fingerprint': '238a26456b4c71c833458d74d650b2ff3d76252cdffac67efb6a2bbce3a7bc49', 'profile_fingerprint': 'b8d60e72b305f6c360028e19cf29eadd1075b8ac09d701e202dc6ff0f7a43140', 'evidence_sha256': '3d20cf4cb2a62efcb6e2069db47778b53da24e91ceff202f96c43edcf4267c1e'},
    {'runtime_fingerprint': '238a26456b4c71c833458d74d650b2ff3d76252cdffac67efb6a2bbce3a7bc49', 'profile_fingerprint': 'e61ed0e6c205bba51da0fa6da2871a505e7ebb767d92ca66d7859909a5dc8772', 'evidence_sha256': '7543a5086c954da603f41204f970f23f10f0bbfe44edb61580c2f7c1be5b4a18'},
    {'runtime_fingerprint': '0bb1f02844499b53f94358fc00e87745823428ebf77808e43d803dd5673f13ec', 'profile_fingerprint': 'baf1d168d64119ae74a568deeadd27555495b36cb9ea73b8cbd887a13587dfd4', 'evidence_sha256': '7ea6ea1123a530c9f4b567774064ce271637f197184f47a13f00668cb6600f9c'},
    {'runtime_fingerprint': '0bb1f02844499b53f94358fc00e87745823428ebf77808e43d803dd5673f13ec', 'profile_fingerprint': '91190493340380383987cbdc922f00959dfccfb58d6be8c40870b0604f077335', 'evidence_sha256': 'cc5c3d7b2fe700ceccdde1f5a6d0d490b973d9a9db0d610b8450c008a091ca63'},
    {'runtime_fingerprint': '0bb1f02844499b53f94358fc00e87745823428ebf77808e43d803dd5673f13ec', 'profile_fingerprint': '9c728ad279b1f8651f667196cdf2924b044c291ddc6daa2f26754dec9a6b65b0', 'evidence_sha256': '06955baa961fa01d4ca1370e510516b706927e55cc3ab7181eb97201ecdc4fb2'},
    {'runtime_fingerprint': '0bb1f02844499b53f94358fc00e87745823428ebf77808e43d803dd5673f13ec', 'profile_fingerprint': 'e6298ec8aaa753302f24474f50fe3bb5ba61df330b239e9556e420c090ccb147', 'evidence_sha256': '6300be1295dda137ff902fb1ba63bcfe412eb6d8248250c45258c2ee88330b88'},
    {'runtime_fingerprint': '616ac1c5c3e2e608f4981221f36ab78dfd3e0eb6f56216976a0121f8e947e8b3', 'profile_fingerprint': '48c95e515b270e499d5fb37d54e680863c052b58b4fae03955391e8afb84318f', 'evidence_sha256': 'c4d480d94591c6b7a9dcd29e8a9924d2cb2083ead01cffbc6497838379021819'},
    {'runtime_fingerprint': '616ac1c5c3e2e608f4981221f36ab78dfd3e0eb6f56216976a0121f8e947e8b3', 'profile_fingerprint': '98d24ae83a9b80fb8105c47f2c5005b6340d9b32f63ab3b7e6ab257f8417915d', 'evidence_sha256': '7e0f3fc3256ddc27f2680625f07a3c219f25760eaa30b43eb5a3484232b69b06'},
    {'runtime_fingerprint': '616ac1c5c3e2e608f4981221f36ab78dfd3e0eb6f56216976a0121f8e947e8b3', 'profile_fingerprint': '5f18e817a94bd4d584ecdb3d0396f4d93bda1c2c5cd820a90c16593e238290ef', 'evidence_sha256': '11a833d29e3aebcd9d48688249d5c373ce128077a465047d95473b319c462883'},
    {'runtime_fingerprint': '616ac1c5c3e2e608f4981221f36ab78dfd3e0eb6f56216976a0121f8e947e8b3', 'profile_fingerprint': '3ba5ae67586c4306a216e332aa2a17bbdef73dbd3813e1691e66462b5c5958ab', 'evidence_sha256': '9deff291adb0bc3627760091e93b9487f6cc86bc2fb1985c5421f9fcd82ffe57'},
)


def fingerprint(value: Any) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode()
    except (TypeError, ValueError) as exc:
        raise ConversationRefused("conversation qualification authority is not JSON-safe") from exc
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ConversationQualification:
    runtime_fingerprint: str
    profile_fingerprint: str
    launch_fingerprint: str
    evidence_sha256: str
    version: str


def identify_runtime(backend: Any) -> tuple[qualification.Identity, str]:
    """Fresh executable/version/hash observation; never an unrelated cached feature."""
    identity = qualification.identify(backend)
    if identity.version not in SUPPORTED_PROTOCOL_VERSIONS.get(backend.name, ()):
        raise ConversationRefused("conversation runtime version is not supported")
    runtime: dict[str, Any] = {"identity": asdict(identity)}
    if backend.name == "vibe":
        # The ACP Python launcher is distinct from the ordinary Vibe executable.
        # Bind interpreter, launcher and shipped protocol/harness code as well.
        from .vibe_acp import runtime_identity
        runtime["native_runtime"] = runtime_identity()
    return identity, fingerprint(runtime)


def canonical_profile(backend: Any, invocation: Invocation, profile: dict[str, Any],
                      *, repo_path: Path, managed_paths=None) -> dict[str, Any]:
    """Bind exact permission authority; normalize only a validated repository role.

    No arbitrary path is removed. Exact repo strings in values may become a
    typed role marker only after cwd equality and backend assertion have passed.
    Other granted roots, config paths and namespace keys stay exactly bound.
    """
    backend.assert_safe(invocation, profile["freedom"], profile.get("requested_network"))
    repo = str(repo_path)
    if not repo_path.is_absolute() or profile.get("cwd") != repo:
        raise ConversationRefused("conversation qualification repository authority mismatch")
    excluded = {"initial_input", "argv", "conversation_context"}
    authority = copy.deepcopy({item.name: getattr(invocation, item.name) for item in fields(invocation) if item.name not in excluded})
    description = copy.deepcopy({key: value for key, value in profile.items() if key not in {"initial_prompt", "session_id"}})
    if backend.name == "claude":
        from .conversation_native import _option
        resumed = _option(invocation.argv, "--resume") is not None
    else:
        resumed = profile.get("session_id") is not None
    description["session_operation"] = "resume" if resumed else "start"
    # Claude carries its resume/session id inside argv; prompts never occur in
    # the converted native argv. Erase only the specifically declared identity.
    argv = list(description["argv"])
    for index, token in enumerate(argv[:-1]):
        if token in {"--session-id", "--resume", "--conversation"}:
            argv[index + 1] = {"harflow_identity_role": "native_session"}
    description["argv"] = argv
    repository_role = {"harflow_path_role": "repository"}
    description["cwd"] = repository_role
    if description.get('configuration_witness') is not None:
        witness = description['configuration_witness']
        if witness.get('cwd') != repo:
            raise ConversationRefused('configuration witness repository scope mismatch')
        witness['cwd'] = repository_role
    if isinstance(description.get("writable_roots"), list):
        roots = description["writable_roots"]
        # Only the declared first repository slot is the repository role;
        # trailing caller extra grants retain their literal authority.
        if roots and roots[0] == repo:
            description["writable_roots"] = [repository_role, *roots[1:]]
    if description.get('runtime_workspace_roots') is not None:
        roots = description['runtime_workspace_roots']
        if type(roots) is not list or not roots or roots[0] != repo:
            raise ConversationRefused('runtime workspace root repository slot mismatch')
        description['runtime_workspace_roots'] = [repository_role, *roots[1:]]
    # Only backend-validated native path flag positions are path authority.
    # Strings in models, env, config or guidance are never interpreted as paths.
    path_flags = {"-C", "--workdir", "--workspace", "--add-dir"}
    for index, token in enumerate(argv[:-1]):
        if type(token) is str and token in path_flags and argv[index + 1] == repo:
            argv[index + 1] = repository_role
    description["argv"] = argv
    for name in ("shared_scratch_directories",):
        authority[name] = [repository_role if value == repo else value for value in authority[name]]
    for name in ("scratch_directory", "group_report_directory"):
        if authority.get(name) == repo:
            authority[name] = repository_role
    result = {"feature": FEATURE, "revision": REVISION,
              "profile": description, "invocation_authority": authority,
              "capabilities": backend.capabilities.as_dict()}
    if managed_paths is not None:
        _normalize_managed(result, invocation, managed_paths)
    return result



def _normalize_managed(result, invocation, proof):
    from ..conversation_managed_paths import ManagedPaths
    if not isinstance(proof, ManagedPaths):
        raise ConversationRefused('managed path canonicalization requires owner-local proof')
    roles = proof.roles()
    if not roles:
        return
    ordered = [path for path in (invocation.scratch_directory, *invocation.shared_scratch_directories) if path]
    if len(ordered) != len(roles) or set(ordered) != set(roles):
        raise ConversationRefused('managed path proof does not match final grant slots')
    def marker(path):
        return {'harflow_path_role': roles[path]}
    configured = result['profile'].get('configured_writable_roots', [])
    def grants(values, *, configured_prefix=False):
        offset = len(configured) if configured_prefix else 0
        if (not isinstance(values, list) or values[:offset] != configured[:offset]
                or values[offset:offset + len(ordered)] != ordered):
            raise ConversationRefused('native managed grant prefix changed')
        # Trailing caller extra grants remain literal, even equal to a managed path.
        return values[:offset] + [marker(path) for path in ordered] + values[offset + len(ordered):]
    profile, authority = result['profile'], result['invocation_authority']
    if invocation.scratch_directory:
        authority['scratch_directory'] = marker(invocation.scratch_directory)
    authority['shared_scratch_directories'] = [marker(path) for path in invocation.shared_scratch_directories]
    argv = profile['argv']
    positions = [index + 1 for index, value in enumerate(argv[:-1]) if value == '--add-dir']
    represented = False
    if positions:
        values = [argv[index] for index in positions]
        mapped = grants(values)
        for index, value in zip(positions, mapped):
            argv[index] = value
        represented = True
    pin = 'sandbox_workspace_write.writable_roots='
    for index, value in enumerate(argv):
        if isinstance(value, str) and value.startswith(pin) and not profile.get("configuration_witness"):
            native = json.loads(value[len(pin):])
            argv[index] = {'harflow_config_path': 'sandbox_workspace_write.writable_roots', 'value': grants(native, configured_prefix=True)}
            represented = True
    config = profile.get('config', {}).get('sandbox_workspace_write')
    if isinstance(config, dict) and 'writable_roots' in config:
        config['writable_roots'] = grants(config['writable_roots'], configured_prefix=True)
        represented = True
    if 'writable_roots' in profile and profile['writable_roots']:
        roots = profile['writable_roots']
        # Repository was already represented with its typed role above.
        profile['writable_roots'] = roots[:1] + grants(roots[1:], configured_prefix=True)
    if profile.get('runtime_workspace_roots') is not None:
        roots = profile['runtime_workspace_roots']
        profile['runtime_workspace_roots'] = roots[:1] + grants(roots[1:], configured_prefix=True)
    if not represented:
        raise ConversationRefused('managed grant has no mapped native permission seam')
    result['managed_root_roles'] = [{'role': roles[path], 'access': 'writable'} for path in ordered]


def describe_candidate(backend: Any, invocation: Invocation, *, repo_path: Path, freedom: str,
                       network: bool | None = None, profile_name: str | None = None):
    """Model-free candidate identity for an explicitly authorized qualification run."""
    preflight = _runtime_preflight(backend, invocation)
    identity, runtime_fingerprint = preflight.identity, preflight.runtime_fingerprint
    profile = conversation_profile(backend, invocation, version=identity.version,
                                   freedom=freedom, network=network, profile_name=profile_name,
                                   configuration_witness=preflight.configuration_witness)
    profile.setdefault("cwd", str(repo_path))
    profile["requested_network"] = network
    canonical = canonical_profile(backend, invocation, profile, repo_path=repo_path)
    return profile, runtime_fingerprint, fingerprint(canonical)


def prepare_qualified_launch(backend: Any, invocation: Invocation, *, repo_path: Path, freedom: str,
                             network: bool | None = None, profile_name: str | None = None) -> tuple[ConversationLaunch, ConversationQualification]:
    """Production source qualification; unsupported is explicit before native effects."""
    if not ACCEPTED_PROFILES:
        raise ConversationRefused("no accepted native conversation coding/profile qualification")
    profile, runtime, scope = describe_candidate(backend, invocation, repo_path=repo_path,
        freedom=freedom, network=network, profile_name=profile_name)
    row = next((row for row in ACCEPTED_PROFILES if row.get("runtime_fingerprint") == runtime
                and row.get("profile_fingerprint") == scope), None)
    if row is None:
        raise ConversationRefused("native conversation runtime/profile has no accepted qualification")
    evidence = row.get("evidence_sha256")
    if type(evidence) is not str or len(evidence) != 64 or any(c not in "0123456789abcdef" for c in evidence):
        raise ConversationRefused("accepted conversation evidence digest is invalid")
    port = restore_conversation_port(profile, validated_profile=profile)
    converted = replace(invocation, argv=profile["argv"], env=profile["env"],
                        conversation_context=profile, identity_markers=tuple(profile["argv"][:2]))
    receipt = ConversationQualification(runtime, scope, fingerprint(profile), evidence, profile["version"])
    return ConversationLaunch(converted, port), receipt


_RUNTIME_AUTHORITY = object()


@dataclass(frozen=True)
class RuntimePreflight:
    identity: qualification.Identity
    runtime_fingerprint: str
    executable_metadata: tuple[int, int, int, int]
    runtime_metadata: tuple[tuple[str, int, int, int, int], ...]
    launcher_resolutions: tuple[tuple[str, str], ...]
    authority: Any
    configuration_witness: Any = None


def runtime_preflight(backend: Any, invocation=None, policy=None) -> RuntimePreflight:
    """Fresh, model-free hashing/version discovery outside execution leaf locks."""
    if not ACCEPTED_PROFILES:
        raise ConversationRefused("no accepted native conversation coding/profile qualification")
    return _runtime_preflight(backend, invocation, policy)


def _runtime_preflight(backend: Any, invocation=None, policy=None) -> RuntimePreflight:
    """Also used by the private, explicitly authorized qualification factory."""
    identity, digest = identify_runtime(backend)
    info = Path(identity.executable).stat()
    paths = [identity.executable]
    launchers = [(backend.binary, identity.executable)]
    if backend.name == 'vibe':
        from .vibe_acp import runtime_identity
        native = runtime_identity()
        if fingerprint({'identity': asdict(identity), 'native_runtime': native}) != digest:
            raise ConversationRefused('Vibe ACP runtime changed during conversation preflight')
        paths.extend(row[0] for row in native['files'])
        launchers.append(('vibe-acp', str(Path(native['launcher']).resolve(strict=True))))
    if len(paths) > 4096:
        raise ConversationRefused('conversation runtime identity file bound exceeded')
    metadata = []
    for path in dict.fromkeys(paths):
        stat = Path(path).stat()
        metadata.append((path, stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns))
    witness = None
    if backend.name == 'codex' and invocation is not None:
        from .conversation_native import _option
        if _option(invocation.argv, '-s') == 'workspace-write':
            from .conversation_codex_config import baseline_argv, read_configuration
            cwd = _option(invocation.argv, '-C')
            witness = {'cwd': cwd, 'configuration': read_configuration(
                baseline_argv(invocation, 'workspace-write'), cwd=cwd,
                env=invocation.env, version=identity.version)}
    preflight = RuntimePreflight(identity, digest, (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns),
                                 tuple(metadata), tuple(launchers), _RUNTIME_AUTHORITY, witness)
    verify_runtime_preflight(preflight, backend)
    return preflight


def verify_runtime_preflight(preflight: RuntimePreflight, backend: Any) -> None:
    """Bounded metadata/resolution check; never changes argv/PATH or executes a CLI."""
    if not isinstance(preflight, RuntimePreflight) or preflight.authority is not _RUNTIME_AUTHORITY:
        raise ConversationRefused("conversation runtime preflight is not owner-local authority")
    if preflight.identity.backend != backend.name:
        raise ConversationRefused("conversation runtime preflight backend mismatch")
    search_path = os.environ.get('PATH', os.defpath)
    if len(search_path.encode()) > 65536 or len(search_path.split(os.pathsep)) > 256:
        raise ConversationRefused('conversation launcher search path exceeds its bound')
    if not 1 <= len(preflight.launcher_resolutions) <= 2:
        raise ConversationRefused('conversation launcher identity is missing')
    for launcher, expected in preflight.launcher_resolutions:
        selected = shutil.which(launcher, path=search_path)
        try:
            current = str(Path(selected).resolve(strict=True)) if selected else None
        except (OSError, RuntimeError):
            current = None
        if current != expected:
            raise ConversationRefused('conversation launcher resolution changed after preflight')
    info = Path(preflight.identity.executable).stat()
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != preflight.executable_metadata:
        raise ConversationRefused("conversation executable changed after preflight")
    for path, device, inode, size, modified in preflight.runtime_metadata:
        stat = Path(path).stat()
        if (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns) != (device, inode, size, modified):
            raise ConversationRefused('conversation protocol runtime changed after preflight')


def prepare_from_preflight(preflight: RuntimePreflight, backend: Any, invocation: Invocation, *,
                           repo_path: Path, freedom: str, network: bool | None = None,
                           profile_name: str | None = None, managed_paths=None) -> tuple[ConversationLaunch, ConversationQualification]:
    """Bounded synchronous final mapping; no hashes/version subprocesses here."""
    verify_runtime_preflight(preflight, backend)
    profile = conversation_profile(backend, invocation, version=preflight.identity.version,
        freedom=freedom, network=network, profile_name=profile_name,
        configuration_witness=preflight.configuration_witness)
    profile.setdefault('cwd', str(repo_path))
    profile['requested_network'] = network
    scope = fingerprint(canonical_profile(backend, invocation, profile, repo_path=repo_path, managed_paths=managed_paths))
    row = next((row for row in ACCEPTED_PROFILES if row.get('runtime_fingerprint') == preflight.runtime_fingerprint
                and row.get('profile_fingerprint') == scope), None)
    if row is None:
        raise ConversationRefused('native conversation runtime/profile has no accepted qualification')
    evidence = row.get('evidence_sha256')
    if type(evidence) is not str or len(evidence) != 64 or any(c not in '0123456789abcdef' for c in evidence):
        raise ConversationRefused('accepted conversation evidence digest is invalid')
    converted = replace(invocation, argv=profile['argv'], env=profile['env'], conversation_context=profile,
                        identity_markers=tuple(profile['argv'][:2]))
    port = restore_conversation_port(profile, validated_profile=profile)
    return ConversationLaunch(converted, port), ConversationQualification(preflight.runtime_fingerprint,
        scope, fingerprint(profile), evidence, preflight.identity.version)
