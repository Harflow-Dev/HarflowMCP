"""Bounded, subprocess-free qualification of supported host loader layouts."""
import os
from pathlib import Path
import struct

MAX_IMAGES = 256
MAX_COMMAND_BYTES = 8 * 1024 * 1024


def _readonly_missing(path):
    # Only system shared-cache names on an independently observed read-only
    # volume qualify without an on-disk image. A familiar prefix alone proves nothing.
    if not any(path.is_relative_to(Path(root)) for root in ('/usr/lib', '/System/Library')):
        return False
    ancestor = path.parent
    while not ancestor.exists() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    return bool(os.statvfs(ancestor).f_flag & os.ST_RDONLY)


def _commands(path):
    with path.open('rb') as stream:
        magic = stream.read(4)
        slices = [(0, None)]
        if magic in (b'\xca\xfe\xba\xbe', b'\xca\xfe\xba\xbf'):
            count_data = stream.read(4)
            if len(count_data) != 4:
                raise ValueError('truncated runtime Mach-O architecture count')
            count = struct.unpack('>I', count_data)[0]
            if not 0 < count <= 32:
                raise ValueError('runtime Mach-O architecture count exceeds bound')
            wide = magic == b'\xca\xfe\xba\xbf'
            size = 32 if wide else 20
            table = stream.read(count * size)
            if len(table) != count * size:
                raise ValueError('truncated runtime Mach-O architecture table')
            slices = []
            for index in range(count):
                row = table[index * size:(index + 1) * size]
                offset, length = struct.unpack('>QQ' if wide else '>II', row[8:24] if wide else row[8:16])
                slices.append((offset, length))
        result = []
        for offset, length in slices:
            stream.seek(offset)
            header = stream.read(32)
            formats = {b'\xcf\xfa\xed\xfe': ('<', 32), b'\xce\xfa\xed\xfe': ('<', 28), b'\xfe\xed\xfa\xcf': ('>', 32), b'\xfe\xed\xfa\xce': ('>', 28)}
            if header[:4] not in formats:
                raise ValueError('unsupported runtime executable loader layout')
            endian, header_size = formats[header[:4]]
            if len(header) < header_size:
                raise ValueError('truncated runtime Mach-O header')
            count, total = struct.unpack(endian + 'II', header[16:24])
            if count > 65536 or total > MAX_COMMAND_BYTES or (length is not None and total + header_size > length):
                raise ValueError('runtime Mach-O load commands exceed bounds')
            stream.seek(offset + header_size)
            commands = stream.read(total)
            if len(commands) != total:
                raise ValueError('truncated runtime Mach-O load commands')
            position = 0
            for _ in range(count):
                if position + 8 > total:
                    raise ValueError('malformed runtime Mach-O load command')
                kind, size = struct.unpack(endian + 'II', commands[position:position + 8])
                if size < 8 or position + size > total:
                    raise ValueError('malformed runtime Mach-O load command')
                if kind == 0x27:
                    raise ValueError('embedded runtime loader environment is unqualified')
                if kind in (0xe, 0xc, 0x80000018, 0x8000001f, 0x80000023, 0x20, 0x8000001c):
                    if size < 12:
                        raise ValueError('malformed runtime Mach-O dependency')
                    name_offset = struct.unpack(endian + 'I', commands[position + 8:position + 12])[0]
                    minimum = 12 if kind in (0xe, 0x8000001c) else 24
                    if not minimum <= name_offset < size:
                        raise ValueError('malformed runtime Mach-O dependency offset')
                    raw = commands[position + name_offset:position + size]
                    if b'\0' not in raw:
                        raise ValueError('unterminated runtime Mach-O dependency')
                    result.append((kind, raw.split(b'\0', 1)[0].decode('utf-8')))
                position += size
            if position != total:
                raise ValueError('malformed runtime Mach-O command length')
        return result


def qualify_loaders(executables):
    """Return dependency files and lookup namespaces; unknown layouts fail closed."""
    if any(value for key, value in os.environ.items() if key.startswith('DYLD_') or key in ('PYTHONPATH', 'PYTHONHOME', 'PYTHONSTARTUP', 'PYTHONUSERBASE', 'NODE_OPTIONS', 'NODE_PATH')):
        raise ValueError('runtime loader environment overrides are unsupported with extra writable paths')
    files, roots = set(), set()
    pending = [(Path(path), Path(path).resolve(), ()) for path in executables]
    seen = set()
    while pending:
        lexical, main, inherited = pending.pop()
        target = lexical.resolve()
        state = (target, main, inherited)
        if state in seen:
            continue
        seen.add(state)
        if len(seen) > MAX_IMAGES:
            raise ValueError('runtime loader dependency closure exceeds bound')
        files.add(lexical)
        files.add(target)
        if not target.exists():
            if _readonly_missing(target):
                continue
            raise ValueError('runtime loader dependency is unresolved')
        with target.open('rb') as stream:
            data = stream.read(65537)
        if data.startswith(b'#!'):
            raise ValueError('runtime script loader dependencies cannot be qualified')
        commands = _commands(target)
        def expand(name):
            if name == '@loader_path':
                return target.parent
            if name == '@executable_path':
                return main.parent
            if name.startswith('@loader_path/'):
                return target.parent / name[len('@loader_path/'):]
            if name.startswith('@executable_path/'):
                return main.parent / name[len('@executable_path/'):]
            if name.startswith('/'):
                return Path(name)
            raise ValueError('runtime loader path cannot be qualified')
        rpaths = tuple(dict.fromkeys([*(expand(name) for kind, name in commands if kind == 0x8000001c), *inherited]))
        roots.update(rpaths)
        for kind, name in commands:
            if kind == 0x8000001c:
                continue
            if name.startswith('@rpath/'):
                candidates = [root / name[len('@rpath/'):] for root in rpaths]
                if not candidates:
                    raise ValueError('runtime loader rpath cannot be qualified')
                files.update(candidates)  # Includes missing earlier lookup candidates.
                selected = next((candidate for candidate in candidates if candidate.exists()), None)
                if selected is None:
                    raise ValueError('runtime loader rpath dependency is unresolved')
            else:
                selected = expand(name)
            pending.append((selected, main, rpaths))
    return files, roots
