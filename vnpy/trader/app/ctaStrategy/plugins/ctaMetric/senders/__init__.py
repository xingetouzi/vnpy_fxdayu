from .log import LogfileMetricSender
from .sqlite import SqliteMetricSender
from ..base import set_sender

set_sender(SqliteMetricSender)