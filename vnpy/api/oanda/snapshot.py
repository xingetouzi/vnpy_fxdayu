import six
from warnings import warn

from vnpy.api.oanda.utils import Singleton

def is_transaction_id_older(tid1, tid2):
    return int(tid2) > int(tid1)


class OandaSnapshot(object):
    def __init__(self, *args, **kwargs):
        self._last_transaction_id = -1
        self._dict = {}

    @property
    def dict(self):
        return self._dict

    @property
    def last_transaction_id(self):
        return int(self._last_transaction_id)

    def is_older(self, transaction_id):
        return int(transaction_id) > self.last_transaction_id


class OandaOrders(OandaSnapshot):
    def __setitem__(self, id, order):
        self._dict[id] = order

    def __getitem__(self, id):
        return self._dict[id]

    def __delattr__(self, id):
        return self._dict.__delitem__(id)

    def update(self, orders, last_transaction_id):
        if self.is_older(last_transaction_id):
            self._dict.update(orders)
            self._last_transaction_id = last_transaction_id
            return True
        else:
            return False


class OandaPositions(OandaSnapshot):
    def __setitem__(self, id, pos):
        self._dict[id] = pos

    def __getitem__(self, id):
        return self._dict[id]

    def __delattr__(self, id):
        return self._dict.__delitem__(id)
    
    def update(self, positions, last_transaction_id):
        if self.is_older(last_transaction_id):
            self._dict = positions
            self._last_transaction_id = last_transaction_id
            return True
        else:
            return False


class OandaAccountDetails(six.with_metaclass(Singleton, OandaSnapshot)):
    def __setitem__(self, id, account):
        old = self._dict.get(id, None)
        if old and not is_transaction_id_older(old, account.lastTransactionID):
            return 
        else:
            self._dict[id] = account

    def __getitem__(self, id):
        return self._dict[id]

    def __delitem__(self, id):
        return self._dict.__delitem__(id)


class OandaTrades(OandaSnapshot):
    pass


# TODO: keep lastTransactionID sync in all snapshot
class OandaAccount(object):
    def __init__(self, id):
        self._id = id
        self._orders = OandaOrders()
        self._trades = OandaTrades()
        self._positions = OandaPositions()
        self._details = OandaAccountDetails()
        self._last_transaction_id = -1

    @property
    def last_transaction_id(self):
        return int(self._last_transaction_id)

    @property
    def id(self):
        return self._id

    @property
    def orders(self):
        return self._orders

    @property
    def trades(self):
        return self._trades

    @property
    def positions(self):
        return self._positions

    @property
    def details(self):
        return self._details[self.id]

    @details.setter
    def details(self, value):
        self._details[self.id] = value