import os
import logging
import random
from functools import wraps

from filelock import Timeout, FileLock

VNPY_ROOT = os.environ.get("VNPY_ROOT", os.path.expanduser("~/.vnpy"))
VNPY_PORT_RANGE = os.environ.get("VNPY_PORTS_RANGE", "20000-29999")
VNPY_RS_SETTING_FILE = os.environ.get("VNPY_RS_SETTING_FILE",
                                      "RS_setting.json")
try:
    port_range = list(map(int, VNPY_PORT_RANGE.split("-")))
    assert len(port_range) == 2
except Exception as e:
    logging.exception(e)
    port_range = list(map(int, "20000-29999".split("-")))

PORTSID_DIR = os.path.join(VNPY_ROOT, "rs_portid")
os.makedirs(PORTSID_DIR, exist_ok=True)
lock_path = os.path.join(PORTSID_DIR, "vnpy_rs_portid.lock")
portid_suffix = ".portid"

lock = FileLock(lock_path)


def with_lock(timeout=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock.acquire(timeout):
                return func(*args, **kwargs)

        return wrapper

    return decorator


@with_lock(timeout=3)
def get_portids(name, n, prefers=None):
    used_ports = set()
    for fname in os.listdir(PORTSID_DIR):
        filepath = os.path.join(PORTSID_DIR, fname)
        if os.path.isfile(filepath) and filepath.endswith(portid_suffix):
            with open(filepath) as f:
                lines = f.readlines()
            for line in lines:
                try:
                    used_ports.add(int(line))
                except:
                    continue
    ret = []
    for _ in range(n):
        if prefers:
            port = prefers.pop(0)
        else:
            port = random.randint(*port_range)
        while port in used_ports:
            port = random.randint(*port_range)
        ret.append(port)
    portid_file = os.path.join(PORTSID_DIR, name + portid_suffix)
    with open(portid_file, "w") as f:
        f.writelines([str(p) + "\n" for p in ret])
    return ret


@with_lock(timeout=-1)
def release_portids(name):
    portid_file = os.path.join(PORTSID_DIR, name + portid_suffix)
    try:
        os.remove(portid_file)
    except:
        pass
