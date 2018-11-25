from abc import ABCMeta, abstractmethod

from six import with_metaclass


class AbstractOandaGateway(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def getClientOrderID(self, id, clientExtensions):
        """Return the clientOrderID of a oanda order according to the exchangeID or clientExtensions info.
        
        Parameters
        ----------
        id : str
            Exchange order id of the order
        clientExtensions : vnpy.api.oanda.models.OandaClientExtensions
            Data like:
            {
                # 
                # The Client ID of the Order/Trade
                # 
                id : (ClientID),

                # 
                # A tag associated with the Order/Trade
                # 
                tag : (ClientTag),

                # 
                # A comment associated with the Order/Trade
                # 
                comment : (ClientComment)
            }
        
        Raises
        ------
        NotImplementedError
            Must be implemented by subclass.

        Return
        ------
        clientOrderId: str or None
            The corresponding client order id, return None when couldn't find then client order id.
        """

        raise NotImplementedError

    @abstractmethod
    def getOrder(self, clOrderID):
        """Return the order by the client order id.
        
        Parameters
        ----------
        clOrderID : str
            Client order id.
        
        Raises
        ------
        NotImplementedError
            Must be implemented by subclass.
        
        Return
        ------
        order: vnpy.trader.vtObject.VnOrderData or None
            The corresponding order, return None when counld't find the order.
        """

        raise NotImplementedError