"""Declared file-link groups reject unsafe state before verifier mutation."""

import hashlib
import io
import json
import tarfile
from pathlib import PurePosixPath

import pytest

from loom.trial import mutable_snapshot as snapshot
from loom.trial.workspace_snapshot import WorkspaceSnapshotError


def write_archive(path, entries):
    with tarfile.open(path, 'w') as stream:
        for name, kind, value in entries:
            item = tarfile.TarInfo(name)
            item.type = kind
            if kind == tarfile.REGTYPE:
                item.size = len(value)
                stream.addfile(item, io.BytesIO(value))
            else:
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    item.linkname = value
                stream.addfile(item)


def manifest(directory, groups):
    rows = []
    for index, (root, entries) in enumerate(groups.items()):
        path = directory / f'{index}.tar'
        write_archive(path, entries)
        rows.append({'path': root, 'archive': path.name, 'size_bytes': path.stat().st_size,
                     'expanded_bytes': sum(len(value) for _, kind, value in entries if kind == tarfile.REGTYPE),
                     'entries': len(entries), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
    (directory / 'manifest.json').write_text(json.dumps({'schema_version': 1, 'paths': rows}))
    return tuple(map(PurePosixPath, groups))


class UntouchedVerifier:
    async def exec(self, *args, **kwargs):
        pytest.fail('invalid archives reached verifier commands')

    async def replace_workspace_archive(self, *args, **kwargs):
        pytest.fail('invalid archives reached verifier replacement')


@pytest.mark.parametrize('case', ['cycle', 'missing', 'directory', 'private', 'suffix',
                                 'interior', 'symlink_parent', 'file_parent', 'hardlink_escape',
                                 'absent', 'digest', 'too_many_links', 'prefix_chain'])
async def test_unsafe_group_rejects_before_any_verifier_command(tmp_path, case):
    groups = {'/a': [('use', tarfile.SYMTYPE, '/b/link')],
              '/b': [('link', tarfile.SYMTYPE, '/a/real')],
              '/c': [('untouched', tarfile.REGTYPE, b'baseline')]}
    if case == 'cycle':
        groups['/b'][0] = ('link', tarfile.SYMTYPE, '/a/use')
    if case == 'directory':
        groups['/b'][0] = ('link', tarfile.DIRTYPE, '')
    if case == 'private':
        groups['/b'][0] = ('link', tarfile.SYMTYPE, '/tests/private')
    if case in {'suffix', 'interior'}:
        groups['/a'][0] = ('use', tarfile.SYMTYPE,
                          '/b/link/..' if case == 'suffix' else '/b/sub/../link')
        groups['/b'][0] = ('link', tarfile.REGTYPE, b'file')
    if case in {'symlink_parent', 'file_parent'}:
        groups['/a'][0] = ('use', tarfile.SYMTYPE, '/b/link/file')
        groups['/b'] = [('link', tarfile.SYMTYPE, '/etc'), ('link/file', tarfile.REGTYPE, b'file')]
        if case == 'file_parent':
            groups['/b'][0] = ('link', tarfile.REGTYPE, b'not a directory')
    if case == 'hardlink_escape':
        groups['/b'][0] = ('link', tarfile.LNKTYPE, '../a/real')
    if case == 'absent':
        groups['/b'] = []
    if case == 'digest':
        groups['/a'].append(('real', tarfile.REGTYPE, b'real'))
    if case in {'too_many_links', 'prefix_chain'}:
        count = 40 if case == 'too_many_links' else 39
        groups['/b'] = [('link' if i == 0 else f'link{i}', tarfile.SYMTYPE,
                        f'link{i+1}' if i < count - 1 else 'real') for i in range(count)]
        groups['/b'].append(('real', tarfile.REGTYPE, b'real'))
        if case == 'prefix_chain':
            groups['/a'] = [('use', tarfile.SYMTYPE, 'alias'), ('alias', tarfile.SYMTYPE, '/b/link')]
    roots = manifest(tmp_path, groups)
    if case == 'digest':
        write_archive(tmp_path / '0.tar', [('use', tarfile.SYMTYPE, '/b/link'),
                                          ('real', tarfile.REGTYPE, b'changed')])
    if case == 'absent':
        path = tmp_path / 'manifest.json'
        value = json.loads(path.read_text())
        value['schema_version'] = 3
        value['paths'][1]['state'] = 'absent'
        path.write_text(json.dumps(value))
    with pytest.raises(WorkspaceSnapshotError):
        await snapshot.import_mutable_paths(UntouchedVerifier(), roots, tmp_path, workdir=PurePosixPath('/app'))


async def test_export_stops_at_aggregate_budget_before_exporting_another_root(tmp_path, monkeypatch):
    exported = []

    async def present(*args, **kwargs):
        return True

    async def hardlinks(*args):
        pass

    async def export(driver, root, archive, **kwargs):
        exported.append(root)
        write_archive(archive, [('data', tarfile.REGTYPE, b'x')])

    monkeypatch.setattr(snapshot, '_check_root', present)
    monkeypatch.setattr(snapshot, '_check_cross_root_hardlinks', hardlinks)
    monkeypatch.setattr(snapshot, '_export_workspace_archive', export)
    monkeypatch.setattr(snapshot, 'MAX_MUTABLE_BYTES', 15_000)
    roots = tuple(map(PurePosixPath, ['/a', '/b', '/c']))
    with pytest.raises(WorkspaceSnapshotError, match='aggregate'):
        await snapshot.export_mutable_paths(object(), roots, tmp_path, workdir=PurePosixPath('/app'))
    assert exported == list(roots[:2])
    assert not (tmp_path / 'manifest.json').exists()


async def test_immutable_alias_hop_counts_toward_cross_root_limit(tmp_path):
    groups = {'/a': [('use', tarfile.SYMTYPE, '/b/link0')], '/b': [
        (f'link{i}', tarfile.SYMTYPE, f'link{i+1}' if i < 38 else '/refs/alias')
        for i in range(39)]}
    roots = manifest(tmp_path, groups)

    class References(UntouchedVerifier):
        async def replace_mutable_archives(self, *args, **kwargs):
            pytest.fail('overlong link chain reached group replacement')

        async def inspect_reference_symlink(self, path):
            return 'real'

        async def inspect_reference_file(self, path, *, max_bytes):
            return {'path': str(path), 'size_bytes': 4, 'mode': 0o755, 'uid': 0, 'gid': 0,
                    'sha256': hashlib.sha256(b'real').hexdigest()}

    refs = (PurePosixPath('/refs/alias'), PurePosixPath('/refs/real'))
    alias = {'/refs/alias': 'real'}
    path = tmp_path / 'manifest.json'
    value = json.loads(path.read_text())
    value.update(schema_version=2, reference_files=await snapshot._reference_evidence(
        References(), refs, reference_symlinks=alias))
    path.write_text(json.dumps(value))
    with pytest.raises(WorkspaceSnapshotError, match='40 links'):
        await snapshot.import_mutable_paths(References(), roots, tmp_path, workdir=PurePosixPath('/app'),
                                            reference_files=refs, reference_symlinks=alias)


async def test_cross_root_group_requires_native_staging_before_verifier_commands(tmp_path):
    roots = manifest(tmp_path, {'/a': [('use', tarfile.SYMTYPE, '/b/real')],
                               '/b': [('real', tarfile.REGTYPE, b'real')]})
    with pytest.raises(WorkspaceSnapshotError, match='staged mutable file-link group'):
        await snapshot.import_mutable_paths(UntouchedVerifier(), roots, tmp_path, workdir=PurePosixPath('/app'))


def test_exactly_forty_links_including_immutable_alias_is_valid(tmp_path):
    from loom.trial.mutable_links import mutable_file_targets

    groups = {'/a': [('use', tarfile.SYMTYPE, '/b/link0')], '/b': [
        (f'link{i}', tarfile.SYMTYPE, f'link{i+1}' if i < 37 else '/refs/alias')
        for i in range(38)]}
    roots = manifest(tmp_path, groups)
    archives = {root: tmp_path / f'{i}.tar' for i, root in enumerate(roots)}
    refs = (PurePosixPath('/refs/alias'), PurePosixPath('/refs/real'))
    targets, cross_root = mutable_file_targets(archives, refs, {'/refs/alias': 'real'})
    assert cross_root and targets[PurePosixPath('/a/use')] == 40
    for root, archive in archives.items():
        snapshot._archive_evidence(archive, root, refs, allow_relative_references=True,
                                   transferred_file_targets={p: n for p, n in targets.items()
                                                             if not p.is_relative_to(root)})
