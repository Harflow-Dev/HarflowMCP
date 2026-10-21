#!/usr/bin/env python3
"""Validate immutable engine release inputs and exact publication artifacts."""
import argparse
import configparser
import email
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import time
import tomllib
import urllib.parse
import urllib.request
import zipfile

NAME = 'harflow-mcp'
NAMESPACE = 'io.github.Harflow-Dev/harflow-mcp'
MARKER = '<!-- mcp-name: ' + NAMESPACE + ' -->'
PUBLISHER_SHA256 = 'a06c9096dcb9727c13555b6be26c7effa707b01f06a4c561ba7a3635443cf2cc'
ENTRYPOINTS = {'harflow-mcp': 'harflow_mcp.server:main', 'harflow-server': 'harflow_mcp.server:main',
               'harflow-ctl': 'harflow_mcp.cli:main', 'harflow-setup': 'harflow_mcp.setup_client:main'}


def exact_version(value):
    if not isinstance(value, str) or not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', value):
        raise ValueError('expected exact three-part version')
    return value


def validate_source(root, tag=None, sha=None):
    root = Path(root)
    project = tomllib.loads((root / 'pyproject.toml').read_text())['project']
    version = exact_version(project['version'])
    if project['name'] != NAME or project['scripts'] != ENTRYPOINTS:
        raise ValueError('package identity or entrypoints mismatch')
    manifest = json.loads((root / 'server.json').read_text())
    expected = {'registryType': 'pypi', 'identifier': NAME, 'version': version,
                'runtimeHint': 'uvx', 'transport': {'type': 'stdio'}}
    if (manifest['name'] != NAMESPACE or manifest['version'] != version
            or manifest['packages'] != [expected]
            or manifest['$schema'] != 'https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json'
            or MARKER not in (root / 'README.md').read_text().splitlines()):
        raise ValueError('registry identity mismatch')
    if tag is not None:
        if tag != 'v' + version:
            raise ValueError('release tag/version mismatch')
        head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
        tagged = subprocess.check_output(['git', 'rev-parse', 'refs/tags/' + tag + '^{commit}'], cwd=root, text=True).strip()
        if not sha or not re.fullmatch('[0-9a-f]{40}', sha) or head != sha or tagged != sha:
            raise ValueError('immutable release source mismatch')
    return version


def validate_wheel(path, version):
    version = exact_version(version)
    if Path(path).name != 'harflow_mcp-' + version + '-py3-none-any.whl':
        raise ValueError('unexpected wheel filename')
    with zipfile.ZipFile(path) as wheel:
        prefix = 'harflow_mcp-' + version + '.dist-info/'
        metadata = email.message_from_bytes(wheel.read(prefix + 'METADATA'))
        if metadata['Name'] != NAME or metadata['Version'] != version or MARKER not in str(metadata.get_payload()).splitlines():
            raise ValueError('wheel metadata mismatch')
        entries = configparser.ConfigParser()
        entries.read_string(wheel.read(prefix + 'entry_points.txt').decode())
        if dict(entries['console_scripts']) != ENTRYPOINTS:
            raise ValueError('wheel entrypoints mismatch')
        for resource in ('harflow_mcp/server.py', 'harflow_mcp/backends/codex_native.toml', 'harflow_mcp/backends/codex_native_write.toml'):
            if not wheel.read(resource):
                raise ValueError('missing package resource')


def validate_artifacts(root, dist):
    version = validate_source(root)
    dist = Path(dist)
    wheel = dist / ('harflow_mcp-' + version + '-py3-none-any.whl')
    sdist = dist / ('harflow_mcp-' + version + '.tar.gz')
    if sorted(p.name for p in dist.iterdir()) != sorted([wheel.name, sdist.name]):
        raise ValueError('unexpected distribution files')
    validate_wheel(wheel, version)
    with tempfile.TemporaryDirectory() as temporary:
        temporary = Path(temporary)
        with tarfile.open(sdist) as archive:
            # sdists must contain only ordinary files/directories under one fixed root.
            prefix = 'harflow_mcp-' + version
            for member in archive.getmembers():
                parts = Path(member.name).parts
                if not parts or parts[0] != prefix or '..' in parts or member.name.startswith('/') or not (member.isfile() or member.isdir()):
                    raise ValueError('unsafe sdist member')
            archive.extractall(temporary, filter='data')
        source = temporary / prefix
        if MARKER not in (source / 'README.md').read_text().splitlines():
            raise ValueError('sdist ownership marker missing')
        rebuilt = temporary / 'rebuilt'
        subprocess.run(['uv', 'build', '--no-create-gitignore', '--wheel', '--out-dir', str(rebuilt), '--build-constraints', str(Path(root).resolve() / 'release-build-constraints.txt'), str(source)], check=True)
        validate_wheel(rebuilt / wheel.name, version)
    (dist / 'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + p.name + '\n' for p in (wheel, sdist)))
    return wheel


def verify_checksums(dist):
    dist = Path(dist)
    lines = (dist / 'SHA256SUMS').read_text().splitlines()
    if len(lines) != 2:
        raise ValueError('expected wheel and sdist checksums')
    seen = set()
    for line in lines:
        digest, name = line.split('  ')
        if not re.fullmatch('[a-f0-9]{64}', digest) or Path(name).name != name or name in seen or not name.endswith(('.whl', '.tar.gz')):
            raise ValueError('invalid checksum record')
        seen.add(name)
        if hashlib.sha256((dist / name).read_bytes()).hexdigest() != digest:
            raise ValueError('artifact checksum mismatch')


def validate_pypi_metadata(data, version, wheel=None):
    version = exact_version(version)
    filename = 'harflow_mcp-' + version + '-py3-none-any.whl'
    if data['info']['name'] != NAME or data['info']['version'] != version:
        raise ValueError('PyPI release identity mismatch')
    matches = [item for item in data['urls'] if item['filename'] == filename and item['packagetype'] == 'bdist_wheel']
    if len(matches) != 1:
        raise ValueError('exact canonical wheel missing')
    item = matches[0]
    url = urllib.parse.urlsplit(item['url'])
    digest = item['digests']['sha256']
    if url.scheme != 'https' or url.netloc != 'files.pythonhosted.org' or not url.path.endswith('/' + filename) or url.query or url.fragment or not isinstance(item['size'], int) or isinstance(item['size'], bool) or not 0 < item['size'] <= 100_000_000 or not re.fullmatch('[a-f0-9]{64}', digest):
        raise ValueError('invalid canonical wheel metadata')
    if wheel is not None and (hashlib.sha256(Path(wheel).read_bytes()).hexdigest() != digest or Path(wheel).stat().st_size != item['size']):
        raise ValueError('published wheel differs from built artifact')
    return item


def wait_for_pypi(version, wheel=None, fetch=None, sleep=time.sleep, attempts=12):
    exact_version(version)
    def default_fetch():
        with urllib.request.urlopen('https://pypi.org/pypi/harflow-mcp/' + version + '/json', timeout=30) as response:
            return json.load(response)
    for attempt in range(attempts):
        try:
            return validate_pypi_metadata((fetch or default_fetch)(), version, wheel)
        except (ValueError, KeyError, TypeError, OSError):
            if attempt + 1 == attempts:
                raise ValueError('exact PyPI release unavailable') from None
            sleep(10)


def verify_publisher(path):
    if hashlib.sha256(Path(path).read_bytes()).hexdigest() != PUBLISHER_SHA256:
        raise ValueError('publisher checksum mismatch')


def validate_version_info(data, version):
    exact_version(version)
    expected = {'product': 'harflow', 'component': 'engine', 'format': 'harflow.version-info',
                'v': 1, 'product_version': version}
    if not isinstance(data, dict) or type(data.get('v')) is not int or any(data.get(key) != value for key, value in expected.items()):
        raise ValueError('installed engine version-info mismatch')


def smoke(wheel):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        env = dict(os.environ, HOME=str(root / 'home'), HARFLOW_STATE_ROOT=str(root / 'state'), UV_CACHE_DIR=str(root / 'cache'))
        Path(env['HOME']).mkdir()
        subprocess.run(['uv', 'venv', str(root / 'venv')], check=True, env=env)
        python = root / 'venv/bin/python'
        subprocess.run(['uv', 'pip', 'install', '--python', str(python), str(Path(wheel).resolve())], check=True, env=env)
        info = subprocess.run([str(root / 'venv/bin/harflow-ctl'), 'version-info', '--json'], check=True, env=env, capture_output=True, text=True)
        validate_version_info(json.loads(info.stdout), exact_version(Path(wheel).name.split('-')[1]))
        # Build a local wheel index so the exact registry-client command can run offline.
        links = root / 'links'
        subprocess.run(['uvx', '--from', 'pip==25.3', 'pip', 'download', '--only-binary=:all:', '--dest', str(links), str(Path(wheel).resolve())], check=True, env=env)
        for command in ([str(root / 'venv/bin/harflow-mcp')], ['uvx', '--offline', '--no-index', '--find-links', str(links), 'harflow-mcp@' + exact_version(Path(wheel).name.split('-')[1])], ['uvx', '--offline', '--no-index', '--find-links', str(links), 'harflow-mcp']):
            # Use the installed SDK and retain stdin until each response arrives.
            subprocess.run([str(python), '-I', str(Path(__file__).with_name('release_stdio.py').resolve()), json.dumps(command)], env=env, timeout=45, check=True)



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['source', 'artifacts', 'checksums', 'smoke', 'pypi', 'publisher'])
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--dist', type=Path, default=Path('dist'))
    parser.add_argument('--tag')
    parser.add_argument('--sha')
    parser.add_argument('--version')
    parser.add_argument('--file', type=Path)
    args = parser.parse_args()
    if args.command == 'source': print(validate_source(args.root, args.tag, args.sha))
    elif args.command == 'artifacts': validate_artifacts(args.root, args.dist)
    elif args.command == 'checksums': verify_checksums(args.dist)
    elif args.command == 'smoke': smoke(args.file)
    elif args.command == 'pypi': wait_for_pypi(args.version, args.file)
    elif args.command == 'publisher': verify_publisher(args.file)


if __name__ == '__main__':
    main()
