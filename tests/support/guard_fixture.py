"""Exception-safe ownership of the test-only guard's real service thread."""

from contextlib import contextmanager
from threading import Thread


@contextmanager
def running_guard(service):
    failure = []

    def run():
        try:
            service.start()
        except BaseException as error:
            failure.append(error)

    thread = Thread(target=run)
    try:
        thread.start()
        yield failure
    finally:
        service.stop()
        if thread.ident is not None:
            thread.join(timeout=5)
        assert not thread.is_alive(), "fixture guard thread did not stop"
        service.close()
