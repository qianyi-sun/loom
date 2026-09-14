Create a deterministic archive manifest for a release file tree.

The input tree is rooted at `/app/archive_src`. Write a Python program at `/app/build_manifest.py` and run it with:

```bash
python /app/build_manifest.py --root /app/archive_src --output /app/archive_manifest.json
```

The output file `/app/archive_manifest.json` must be a JSON object with exactly one key, `files`. `files` must be a list of objects sorted by relative path. Each file object must have exactly:

- `path`: the POSIX-style path relative to `/app/archive_src`
- `size_bytes`: the file size in bytes
- `sha256`: the lowercase SHA-256 hex digest of the file contents

Include regular files under the tree, but exclude:

- hidden files or directories whose path component starts with `.`
- files ending in `.tmp`
- files under directories named `tmp`, `backup`, or `backups`

Do not include directories in the manifest. The manifest must be deterministic across repeated runs.
