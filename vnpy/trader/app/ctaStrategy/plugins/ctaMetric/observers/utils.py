
import logging

def run_observer(cls, path, url=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    cls(path, url).run()
