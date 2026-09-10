# Harbor compatibility patch

`harbor-current-user-tool-probes.patch` applies to Harbor commit
`527d50deb63a5d279e8c20593c18a2cbc7f61f9e` after the pinned package is installed
in `Dockerfile.worker`. The original Git dependency metadata is preserved.

For #1550, read-only tmux/asciinema version checks and platform/package-manager
probes use the environment default user. Actual package installation and source
builds retain their explicit root request. This lets preinstalled tools run in
non-root sandboxes without weakening user enforcement or adding runtime shims.

Remove this patch, its Dockerfile application and patch-only build dependency
after updating the Harbor pin to an upstream commit that includes this behavior.
The opt-in native Terminus smoke exercises the installed Harbor source and checks
that missing tools still request root for installation.
