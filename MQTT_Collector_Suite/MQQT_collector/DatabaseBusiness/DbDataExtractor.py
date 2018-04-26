import datetime
import json
import re
from sqlalchemy import and_, or_, Numeric

import AdvancedStats
from AdvancedStats import AdvancedStatisticsOps
from DatabaseBusiness import database_manager
from sqlalchemy.sql import func, expression
from Models.broker import Broker
from Models.Data import Data
from Models.topic import Topic
from Logger.logger import Logger

log = Logger.get_logger()

sum_ops_mapper = {
	'sum': func.sum,
	'avg': func.avg,
	'min': func.min,
	'max': func.max
}

advanced_ops_mapper = ["regression", "", "prediction"]


def is_number(s):
	try:
		float(s)  # for int, long and float
	except ValueError:
		return False

	return True


def give_me_all_data_from_given_topic(topic, requested_broker_name, date_from=None, date_to=None):
	with database_manager.keep_session() as session:
		# Taking care of wildcards in topic
		# if topic contains wildcard we need to check topic_value column
		# if topic does not contain wildcard we need to check specific topic value (topic_specific_value)
		record = session.query(Data).filter(or_(Data.topic_specific_value == topic, Data.topic_value == topic)).first()
		if "#" in topic or "+" in topic:
			q = session.query(Data).filter(and_(Data.topic_value == topic, Data.broker_name == requested_broker_name))
			if date_from and date_to:
				q = q.filter(Data.date_time.between(date_from, date_to))

		else:
			q = session.query(Data).filter(
				and_(Data.topic_specific_value == topic, Data.broker_name == requested_broker_name))
			if date_from and date_to:
				q = q.filter(Data.date_time.between(date_from, date_to))

		all_datas = q.all()
		if all_datas:
			result_list = []
			for data_unit in all_datas:
				data_unit_edit = data_unit.as_dict()
				data_unit_edit["date_time"] = str(data_unit_edit["date_time"])
				result_list.append(data_unit_edit)
			return result_list, record.topic.data_type
		else:
			return "INFO: There are no datas stored for the specific conditions.", None


def give_me_all_data_from_given_topic_with_ops(topic, requested_broker_name, date_from=None, date_to=None, ops=None,
											   key=None):
	if ops.lower() in sum_ops_mapper:
		ops_func = sum_ops_mapper[ops.lower()]
	else:
		log.warning("Unsupported operation, use only sum, avg, min, max...")
		return None, None
	with database_manager.keep_session() as db:
		record = db.query(Data).filter(or_(Data.topic_specific_value == topic, Data.topic_value == topic)).first()

		# process REGEX type rows
		if record.topic.data_type == Topic.DATA_TYPE_REGEX:
			results = []
			for index, value_type in enumerate(record.topic.types):
				# list in data types in given REGEX, only numeric summarization is allowed
				if value_type == Topic.VALUE_TYPE_INT or value_type == Topic.VALUE_TYPE_FLOAT:
					if ops in advanced_ops_mapper:
						q = db.query().filter(
							and_(Data.topic_value == record.topic_value, Data.broker_name == requested_broker_name))
						if date_from and date_to:
							q = q.filter(Data.date_time.between(date_from, date_to))
						val = q.first()
					else:
						q = db.query(ops_func(expression.cast(Data.value[index], Numeric(10, 4)))).filter(
							and_(Data.topic_value == record.topic_value, Data.broker_name == requested_broker_name))

					if date_from and date_to:
						q = q.filter(Data.date_time.between(date_from, date_to))
					val = q.first()
					results.append(float(val[0]))

				else:
					# this value cannot be summarized because it is not numeric value
					log.warning(
						"{} the value from topic {} cannot by summarized because is not numeric.".format(index + 1,
																										 topic))
					results.append(None)
			return results, record.topic.data_type

		elif record.topic.data_type == Topic.DATA_TYPE_JSON:
			results = []
			if key:
				value_from_key = record.value[key]
				if is_number(str(value_from_key)):
					if ops in advanced_ops_mapper:
						if "#" in topic or "+" in topic:
							q = db.query(Data.date_time, (expression.cast(Data.value[key], Numeric(10, 4)))).filter(
								and_(Data.topic_value == record.topic_value, Data.broker_name == requested_broker_name))
						else:
							q = db.query(Data.date_time, (expression.cast(Data.value[key], Numeric(10, 4)))).filter(
								and_(Data.topic_specific_value == record.topic_specific_value, Data.broker_name == requested_broker_name))
						if date_from and date_to:
							q = q.filter(Data.date_time.between(date_from, date_to))
						val = q.all()
						#result = AdvancedStatisticsOps.complete_regression(val)
					else:
						q = db.query(ops_func(expression.cast(Data.value[key], Numeric(10, 4)))).filter(
							and_(Data.topic_value == record.topic_value, Data.broker_name == requested_broker_name))
						if date_from and date_to:
							q = q.filter(Data.date_time.between(date_from, date_to))

						val = q.first()
						results.append(float(val[0]))
				else:
					log.warning("There is no numeric value for specified key")
					results.append(None)

				return results, record.topic.data_type

			else:
				log.warning("If you want to operate with JSONs please specify the key of the value")
			# TODO implement json parsing similar like for REGEX, add another parameter key, which indicate which key will be summarized
			pass

	return None, None


def obtain_data(topic, requested_broker_name, args):
	if args.get('from') and args.get('to'):
		date_from = datetime.datetime.strptime(args.get('from'), "%Y-%m-%d-%H:%M:%S")
		date_to = datetime.datetime.strptime(args.get('to'), "%Y-%m-%d-%H:%M:%S")
		if args.get('ops') and args.get('ops').lower() not in advanced_ops_mapper:
			return give_me_all_data_from_given_topic_with_ops(topic, requested_broker_name, date_from, date_to,
															  args.get('ops'), key=args.get('key'))
		else:
			return give_me_all_data_from_given_topic(topic, requested_broker_name, date_from, date_to)

	elif args.get('from'):
		date_from = datetime.datetime.strptime(args.get('from'), "%Y-%m-%d-%H:%M:%S")
		date_to = datetime.datetime.now()
		if args.get('ops'):
			return give_me_all_data_from_given_topic_with_ops(topic, requested_broker_name, date_from, date_to,
															  args.get('ops'), key=args.get('key'))
		else:
			return give_me_all_data_from_given_topic(topic, requested_broker_name, date_from, date_to)

	elif args.get('last'):
		date_to = datetime.datetime.now()
		print("DEBUG", date_to)
		timeval_obj = parse_time(args.get('last'))
		date_from = date_to - timeval_obj
		print("DEBUG", date_from)
		if args.get('ops') and args.get('ops').lower() not in advanced_ops_mapper:

			return give_me_all_data_from_given_topic_with_ops(topic, requested_broker_name, date_from, date_to,
															  args.get('ops'), key=args.get('key'))
		else:
			return give_me_all_data_from_given_topic(topic, requested_broker_name, date_from, date_to)
	else:
		if args.get('ops') and args.get('ops').lower() not in advanced_ops_mapper:
			#print("EXECUTED")
			return give_me_all_data_from_given_topic_with_ops(topic, requested_broker_name, ops=args.get('ops'),
															  key=args.get('key'))
		else:
			#print("WITHOUT OPS")
			return give_me_all_data_from_given_topic(topic, requested_broker_name)


def parse_time(time_str):
	regex = re.compile(r'((?P<years>\d+?)y)?((?P<months>\d+?)M)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')
	parts = regex.match(time_str)
	if not parts:
		return
	parts = parts.groupdict()
	time_params = {'days': 0}
	for name, param in parts.items():
		if param and name == 'months':
			time_params['days'] += int(param) * 30
		elif param and name == 'years':
			time_params['days'] += int(param) * 365
		elif param:
			time_params[name] = int(param)
	return datetime.timedelta(**time_params)

def get_db_overview():
	brokers_topics_dict = {}
	topics_data_dict = {}
	with database_manager.keep_weak_session() as session:
		brokers = session.query(Broker).all()

		for broker in brokers:
			topics_data_dict = {}


			for topic in broker.topics:
				datas_count = session.query(Data).filter(Data.topic_value == topic.topic_value).count()
				topics_data_dict[topic.topic_value] = datas_count

			brokers_topics_dict[broker.broker_name] = topics_data_dict

	return brokers_topics_dict