from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func, Text
from sqlalchemy.orm import relationship
from DatabaseBusiness.database_manager import Base


class Broker(Base):
	__tablename__ = 'broker'
	# def __init__(self, broker_name, broker_address, topics, server_port=1883, **kwargs):
	broker_addr = Column('broker_addr', String(64), primary_key=False, unique=False)
	server_port = Column('server_port', Integer, primary_key=False, unique=False)
	broker_name = Column('broker_name', String(64), primary_key=True, unique=True)
	user_login = Column('user_login', String(64), primary_key=False, unique=False)
	user_pass = Column('user_pass', String(64), primary_key=False, unique=False)

	topics = relationship('Topic', lazy='joined', back_populates='broker')
