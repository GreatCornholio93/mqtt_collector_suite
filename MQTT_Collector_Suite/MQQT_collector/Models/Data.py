from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, Text, JSON, ForeignKeyConstraint
from sqlalchemy.orm import relationship
from DatabaseBusiness.database_manager import Base
import datetime

from Models.topic import Topic


class Data(Base):
	__tablename__ = 'data'
	id = Column('id', Integer, primary_key=True, unique=True)
	date_time = Column('date_time', DateTime)
	value = Column('value', JSON, unique=False)
	qos = Column('qos', Integer, unique=False)
	topic_specific_value = (Column('topic_specific_value', String(64)))

	topic_value = (Column(String(64)))
	broker_name = (Column(String(64)))

	__table_args__ = (ForeignKeyConstraint([topic_value, broker_name],
										   [Topic.topic_value, Topic.broker_name]),
					  {})

	topic = relationship('Topic', back_populates='datas', cascade="all,delete")

	def as_dict(self):
		return {c.name: getattr(self, c.name) for c in self.__table__.columns}
