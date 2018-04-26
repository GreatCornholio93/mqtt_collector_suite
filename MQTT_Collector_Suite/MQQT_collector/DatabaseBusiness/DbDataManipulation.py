import datetime

from DatabaseBusiness import database_manager, DbDataExtractor
from Models.topic import Topic
from Models.Data import Data
from sqlalchemy import and_, or_, Numeric
import configparser

from configurator import Configurator


def delete_topic(broker_name, topic_value):
	with database_manager.keep_session() as session:
		# firstly must be deleted datas related to given topic_value
		if "#" in topic_value or "+" in topic_value:
			session.query(Data).filter(and_(Data.topic_value == topic_value, Data.broker_name == broker_name)).delete()
		else:
			session.query(Data).filter(
				and_(Data.topic_specific_value == topic_value, Data.broker_name == broker_name)).delete()
		# now topic entry can be deleted
		session.query(Topic).filter(and_(Topic.topic_value == topic_value, Topic.broker_name == broker_name)).delete()


def delete_data_in_range(broker_name, topic_value, start_time, end_time):
	with database_manager.keep_session() as session:
		if "#" in topic_value or "+" in topic_value:
			q = session.query(Data).filter(
				and_(Data.topic_value == topic_value, Data.broker_name == broker_name))
		else:
			q = session.query(Data).filter(
				and_(Data.topic_specific_value == topic_value, Data.broker_name == broker_name))
		q = q.filter(Data.date_time.between(start_time, end_time)).delete(synchronize_session=False)


def delete_entries_from_x(broker_name, topic_value, timeval_obj):
	date_to = datetime.datetime.now()
	date_from = date_to - timeval_obj
	with database_manager.keep_session() as session:
		if "#" in topic_value or "+" in topic_value:
			q = session.query(Data).filter(
				and_(Data.topic_value == topic_value, Data.broker_name == broker_name))
		else:
			q = session.query(Data).filter(
				and_(Data.topic_specific_value == topic_value, Data.broker_name == broker_name))
		q = q.filter(Data.date_time.between(date_from, date_to)).delete(synchronize_session=False)


def update_database(topic, requested_broker_name, args):
	if args.get('from') and args.get('to'):
		date_from = datetime.datetime.strptime(args.get('from'), "%Y-%m-%d-%H:%M:%S")
		date_to = datetime.datetime.strptime(args.get('to'), "%Y-%m-%d-%H:%M:%S")
		delete_data_in_range(requested_broker_name, topic, date_from, date_to)
	elif args.get('from'):
		date_from = datetime.datetime.strptime(args.get('from'), "%Y-%m-%d-%H:%M:%S")
		date_to = datetime.datetime.now()
		delete_data_in_range(requested_broker_name, topic, date_from, date_to)
	elif args.get('last'):
		timeval_obj = DbDataExtractor.parse_time(args.get('last'))
		delete_entries_from_x(requested_broker_name, topic, timeval_obj)
	else:
		# delete all data from topic
		delete_topic(requested_broker_name, topic)
	return None


def delete_all_data_older_than(broker_name, topic_value, args):
	date_to = datetime.datetime.now()
	new_timeval = DbDataExtractor.parse_time(args.get('last'))
	date_from = date_to - new_timeval

	# print(new_timeval)
	with database_manager.keep_session() as session:
		if "#" in topic_value or "+" in topic_value:
			q = (session.query(Data).filter(and_(
				Data.date_time < date_from, Data.topic_value == topic_value, Data.broker_name == broker_name))).delete()
		else:
			q = (session.query(Data).filter(and_(
				Data.date_time < date_from, Data.topic_specific_value == topic_value,
				Data.broker_name == broker_name))).delete()


def clean_db():
	date_to = datetime.datetime.now()
	conf = Configurator()
	condition_time = conf.get_delete_data_older_than_value()
	print("condition_time: ", condition_time)
	# v condition time je neco jako 1d, 2h atp..
	date_time_obj = DbDataExtractor.parse_time(condition_time)
	date_from = date_to - date_time_obj
	print(date_from)
	with database_manager.keep_session() as session:
		q_count = (session.query(Data).filter(
			Data.date_time < date_from)).all()
		q = (session.query(Data).filter(
			Data.date_time < date_from)).count()
	return len(q_count)
