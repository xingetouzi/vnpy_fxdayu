MSG_SEP = b"\n"
# url
ACCOUNTS_ENDPOINT = "/v3/accounts"
ACCOUNT_SUMMARY_ENDPOINT = "/v3/accounts/{accountID}/summary"
INSTRUMENTS_ENDPOINT = "/v3/accounts/{accountID}/instruments"
INSTRUMENTS_CANDLES_ENDPOINT = "/v3/accounts/{accountID}/instruments/{instrument}/candles"
ORDER_ENDPOINT = "/v3/accounts/{accountID}/orders"
POSITION_ENDPOINT = "/v3/accounts/{accountID}/positions"
PRICE_ENDPOINT = "/v3/accounts/{accountID}/pricing"
PRICE_STREAM_ENDPOINT = "/v3/accounts/{accountID}/pricing/stream"
TRANSACTION_SINCEID_ENDPOINT = "/v3/accounts/{accountID}/transactions/sinceid"
TRANSACTION_STREAM_ENDPOINT = "/v3/accounts/{accountID}/transactions/stream"
# timeout
TRANSACTION_STREAM_HEARTBEAT_TIMEOUT = 15 # 事务信道心跳超时时间
PRICE_STREAM_HEARTBEAT_TIMEOUT = 15 # 价格信道心跳超时时间