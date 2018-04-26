from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager
from Logger.logger import Logger
from configparser import NoSectionError, NoOptionError

from configurator import Configurator

try:
	# engine = create_engine(conf.get('db', 'connection_string'), convert_unicode=True)
	conf = Configurator()
	connection_string = conf.get_database_connection_string()
	engine = create_engine(connection_string)
	Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
except (NoSectionError, NoOptionError):
	log = Logger.get_logger()
	log.critical("Missing config record 'connection_string' in section [db]. Cannot found path to database file.")

Base = declarative_base()


def init_db():
	"""
	Initialize database from models
	"""
	from Models.broker import Broker
	from Models.topic import Topic
	from Models.Data import Data
	from Models.Result import Result

	Base.metadata.create_all(engine)
	log = Logger.get_logger()
	log.debug('Initializing database.')


def _drop_db():
	"""
	Drop all data from database.
	"""
	# Base.metadata.create_all(engine)
	pass


def get_session():
	"""
	Provide session instance, user is responsible for closing instance
	:return: Session instance
	"""
	return Session()


@contextmanager
def keep_session():
	"""
	Provide session for scope of "with" operator
	:return: Session instance
	"""
	session = Session()
	try:
		yield session
		session.flush()
		session.commit()
	except:
		session.rollback()
		raise
	finally:
		session.close()


@contextmanager
def keep_weak_session():
	"""
	Provide session for scope of "with" operator.
	Before closing session all objects are expunged from session so they can exists as detached objects.
	:return: Session instance
	"""
	session = Session()
	try:
		yield session
		session.expunge_all()
		session.commit()
	except:
		session.rollback()
		raise
	finally:
		session.close()

@contextmanager
def keep_test_session():
	"""
	Provide session for scope of "with" operator.
	Before closing session all objects are expunged from session so they can exists as detached objects.
	:return: Session instance
	"""
	session = Session()
	session.expire_on_commit = False
	try:
		yield session
		session.expunge_all()
		session.commit()
	except:
		session.rollback()
		raise
	finally:
		session.close()
