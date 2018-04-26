from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, Text, JSON
from sqlalchemy.orm import relationship
from Models.broker import Broker
from DatabaseBusiness.database_manager import Base


class Topic(Base):
	"""
	Representation of one registered topic for specific Broker.
	:param topic_value: Topic pattern, e.g. 'home/room/temperature', 'office/desk/meteostation'.
	:param data_type: Type incoming data (defined in SimpleRegexParser), e.g. 'RAW', 'REGEX', 'JSON', 'BYTES', ...
	:param simple_regex: User pattern from config, e.g. '%JSON', 'data: %d:%d', '%RAW', .... It can be empty if data_type is RAW.
	:param regex: Contain regular expression in python regex format for parsing messages. Only if data_type is 'REGEX' otherwise can be empty.
	:param types: List of types contained in regular expression. Only if data_type is 'REGEX' otherwise can be empty. E.g. ['INT', 'INT', 'FLOAT', 'BOOL', 'STR']
	"""
	DATA_TYPE_REGEX = 'REGEX'
	DATA_TYPE_JSON = 'JSON'
	DATA_TYPE_STATS = 'STATS'
	DATA_TYPE_RAW = 'RAW'
	VALUE_TYPE_INT = 'INT'
	VALUE_TYPE_FLOAT = 'FLOAT'
	VALUE_TYPE_BOOL = 'BOOL'
	VALUE_TYPE_STRING = 'STR'

	__tablename__ = 'topic'
	# id = Column('id', Integer, primary_key=False, unique=True)
	topic_value = Column('topic_value', String(64), primary_key=True)
	simple_regex = Column('simple_regex', String(64), primary_key=False, unique=False)
	regex = Column('regex', String(64), primary_key=False, unique=False)
	types = Column('types', JSON, primary_key=False, unique=False)
	data_type = Column('data_type', String(16), primary_key=False, unique=False)

	broker_name = Column('broker_name', String(64), ForeignKey('broker.broker_name'), primary_key=True)
	broker = relationship('Broker', single_parent=True, back_populates='topics', cascade='save-update, merge, delete, delete-orphan')

	datas = relationship('Data', back_populates='topic', cascade='all, delete')

	def has_wildcard(self):
		return "#" in self.topic_value or "+" in self.topic_value

	def __repr__(self):
		return "Topic: {}".format(self.__dict__)
