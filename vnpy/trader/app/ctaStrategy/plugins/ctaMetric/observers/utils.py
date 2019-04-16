import os
import logging

from ...utils import handle_url


def run_observer(cls, path, url=None):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")
    cls(path, url).run()


_default_path = "/v1/push"


def get_open_falcon_url(url):
    url = url or os.environ.get("OPEN_FALCON_URL", "http://localhost:1988")
    return handle_url(url, default_path=_default_path)
