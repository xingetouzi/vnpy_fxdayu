from urllib.parse import urlparse, urlunparse


def handle_url(url, default_path=""):
    r = urlparse(url)
    if not r.path or r.path == "/":
        url = urlunparse(r._replace(path=default_path))
    return url