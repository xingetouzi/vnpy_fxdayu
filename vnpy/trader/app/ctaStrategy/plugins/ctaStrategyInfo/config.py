from vnpy.trader.app import ctaStrategy as _ctaStrategy
from vnpy.trader.app import rpcService as _rpcService

GATEWAY_SETTING_SUFFIX = "_connect.json"
CTA_SETTING_FILE = "CTA_setting.json"
CTA_SETTING_MODULE_FILE = _ctaStrategy.__file__
RS_SETTING_FILE = "RS_setting.json"
RS_SETTING_MODULE_FILE = _rpcService.__file__
