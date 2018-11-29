from collections import defaultdict

from ..base import AsyncApiWorker
from ..models.transaction import OandaOrderCancelRejectTransaction

class SolidOrderWorker(AsyncApiWorker):
    """Worker handle rejected order to prevent repeated order cancel"""

    def __init__(self, api):
        super(SolidOrderWorker, self).__init__(api)
        self._rejected_orders = defaultdict(dict)

    def process_transaction(self, trans, account_id):
        if isinstance(trans, OandaOrderCancelRejectTransaction):
            rejected_orders = self._rejected_orders[account_id]
            if trans.clientOrderID not in rejected_orders:
                rejected_orders[trans.clientOrderID] = trans
                self.query_rejected_order(trans)

    def query_rejected_order(self, trans):
        self._rejected_orders[trans.clientOrderID] = trans
    
    def process_cancel_order(self, req, account_id):
        """Skip rejected order and push same rejected transaction.
        
        Parameters
        ----------
        req : vnpy.api.oanda.models.request.OandaOrderSpecifier
            OandaOrderSpecifier contains clientOrderID
        account_id : str
            account
        """
        rejected_orders = self._rejected_orders[account_id]
        if req.clientOrderID in rejected_orders:
            self.api.on_transaction(rejected_orders[req.clientOrderID])
            return True