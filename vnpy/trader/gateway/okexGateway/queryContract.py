import json
import requests
from vnpy.trader.vtFunction import getJsonPath, getTempPath
if __name__ == "__main__":
    REST_HOST = 'https://www.okex.com/api/futures/v3/instruments'

    r = requests.get(REST_HOST,timeout = 10)
    result = eval(r.text)
    contracts = {}
    for contract in result:
        dayu_sym = contract["underlying_index"] + "-" + str.upper(contract["alias"]).replace("_","-")
        contracts[dayu_sym] = contract["instrument_id"]

    with open("temp/future.json") as f:
        f.dump(contracts)
