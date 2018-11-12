from collections import OrderedDict

class AccountFilter(object):
    def filter_accounts(self, accounts):
        raise NotImplementedError


class FirstAccountFilter(object):
    def filter_accounts(self, accounts):
        ids = list(accounts.keys())
        new = OrderedDict()
        new[ids[0]] = accounts[ids[0]]
        return new