"""ModalDriver — Loom Driver Protocol against Modal Sandbox.

Import the public driver explicitly:

    from loom_drivers.modal.driver import ModalDriver

We deliberately do NOT re-export at the package level so that
`import loom_drivers.modal` does NOT pull in the `modal` SDK.
The SDK is loaded lazily inside ``loom_drivers.modal.client``.
"""
