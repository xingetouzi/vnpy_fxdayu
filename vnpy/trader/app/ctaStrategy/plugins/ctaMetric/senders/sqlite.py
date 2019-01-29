import logging
import json
from logging.handlers import TimedRotatingFileHandler

from six import with_metaclass
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Float, UniqueConstraint, Index
from sqlalchemy.orm import sessionmaker

from vnpy.trader.utils import Singleton
from vnpy.trader.vtFunction import getTempPath
from ..base import MetricSender

Base = declarative_base()

class OpenFalconMetric(Base):
    __tablename__ = 'metrics'

    id = Column(Integer, primary_key=True)
    endpoint = Column(String(length=100, convert_unicode=True))
    metric = Column(String(length=100, convert_unicode=True))
    timestamp = Column(Integer())
    step = Column(Integer())
    value = Column(Float())
    counterType = Column(String(length=10, convert_unicode=True))
    tags = Column(String(length=200, convert_unicode=True), nullable=False)
    __table_args__ = (
        UniqueConstraint('endpoint', 'metric', name='endpoint_metric_uc'),
        Index("endpoint_metric_index", "endpoint", "metric")
    )

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.endpoint = dct["endpoint"]
        obj.metric = dct["metric"]
        obj.timestamp = dct["timestamp"]
        obj.step = dct["step"]
        obj.value = dct["value"]
        obj.counterType = dct["counterType"]
        obj.tags = dct["tags"]
        return obj

    def to_dict(self):
        dct = {}
        dct["endpoint"] = self.endpoint
        dct["metric"] = self.metric
        dct["timestamp"] = self.timestamp
        dct["step"] = self.step
        dct["value"] = self.value
        dct["counterType"] = self.counterType
        dct["tags"] = self.tags


class SqliteMetricSender(with_metaclass(Singleton, MetricSender)):
    def __init__(self):
        super(SqliteMetricSender, self).__init__()
        filename = "ctaMetric.sqlite"
        filepath = getTempPath(filename)
        engine = create_engine('sqlite:///%s' % filepath)
        self.engine = engine
        self.ensure_table()
        Session = sessionmaker()
        Session.configure(bind=engine)
        self.session = Session()

    def ensure_table(self):
        Base.metadata.create_all(self.engine)

    def pushMetrics(self, metrics):
        new = {(metric.endpoint, metric.metric): OpenFalconMetric.from_dict(metric.__dict__) for metric in metrics}
        old = {(metric.endpoint, metric.metric): metric for metric in self.session.query(OpenFalconMetric).all()}
        objs = []
        for k, v in new.items():
            if k in old:
                obj = old[k]
                obj.timestamp = v.timestamp
                obj.step = v.step
                obj.value = v.value
                obj.counterType = v.counterType
                obj.tags = v.tags
                objs.append(obj)
            else:
                objs.append(v)
        self.session.bulk_save_objects(objs)
        self.session.commit()
