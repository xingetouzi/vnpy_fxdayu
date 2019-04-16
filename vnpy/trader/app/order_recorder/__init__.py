# encoding: UTF-8

from __future__ import absolute_import
from .orEngine import OrEngine
from .uiOrWidget import OrEngineManager

appName = 'OrderRecorder'
appDisplayName = u'订单记录'
appEngine = OrEngine
appWidget = OrEngineManager
appIco = 'dr.ico'