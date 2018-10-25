import hmac
import six

def hmac_new(key, data, digestmod=None):
    if six.PY2:
        return hmac.new(key, data, digestmod=digestmod)
    else:
        bkey = key.encode()
        bdata = data.encode()
        return hmac.new(bkey, bdata, digestmod=digestmod)
