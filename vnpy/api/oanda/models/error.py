class OandaRequestError(Exception):
    """
    Oanda http request error
    """
    def __init__(self, *args, **kwargs):
        super(OandaRequestError, self).__init__(*args, **kwargs)
        self.status = None
        self.data = None

    @classmethod
    def new(cls, status, data):
        obj = cls()
        obj.status = status
        obj.data = data
        return obj

    def __str__(self):
        return "status: %s, data: %s" % (self.status, self.data)