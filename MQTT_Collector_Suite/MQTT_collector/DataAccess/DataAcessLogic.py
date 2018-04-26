import json

from DataAccess import MatplotlibWrapper
from DatabaseBusiness import DbDataExtractor, DbDataManipulation
from Logger.logger import Logger
from Ops.BrokerOpsMapper import BrokerOpsMapper
from AdvancedStats import AdvancedStatisticsOps

log = Logger.get_logger()


class DataAccessLogic(object):
	def __init__(self, response_topic, message, broker_name):
		self.BO = BrokerOpsMapper.get_broker_ops_mapper(broker_name)
		self.response_topic = response_topic
		cmd_mapper = {
			'get-stats': self.get_stats_ops,
			'get-graph': self.get_graph_ops,
			'get-help': self.get_help_ops,
			'delete-from-db': self.database_ops,
			'delete-data-older-than': self.delete_data_after,
			'clean-db': self.run_clean_db_procedure,
			'get-overview': self.get_overview
		}
		query_list = message.split(' ')
		cmd = cmd_mapper.get(query_list[0])
		if query_list[0] != "get-help" and query_list[0] != "clean-db":
			if len(query_list) < 3:
				self.BO.send_data(self.response_topic, 2, False, "ERROR: Wrong count of arguments")
				return
			requested_broker_name = query_list[1]
			topic = query_list[2]

			if not cmd:
				raise ValueError("Command not found")

			args = {}
			try:
				for query_part in [query_list[i:i + 2] for i in range(3, len(query_list), 2)]:
					args[query_part[0]] = query_part[1]
				cmd(topic, requested_broker_name, args)
			except IndexError:
				log.error("Wrong args")
				self.BO.send_data(self.response_topic, 2, False, "ERROR: Incorrect arguments")
		else:
			cmd("", "", {})

	def get_stats_ops(self, topic, requested_broker_name, args):
		query_result, record_type = DbDataExtractor.obtain_data(topic, requested_broker_name, args)
		query_dict = {
			"result": query_result,
			"record_type": record_type
		}
		self.BO.send_data(self.response_topic, 2, False, json.dumps(query_dict))

	def get_graph_ops(self, topic, requested_broker_name, args):
		print("topic, ", topic)
		print("request_broker_name: ", requested_broker_name)
		query_result, record_datatype = DbDataExtractor.obtain_data(topic, requested_broker_name, args)
		if 'ops' in args and args.get('ops').lower() == "regression":
			print(query_result)
			graph_result_dict = AdvancedStatisticsOps.regresion_ops(query_result, record_datatype, key=args.get('key'))
		elif 'ops' in args and args.get('ops').lower() == "prediction":
			print(query_result)
			graph_result_dict = AdvancedStatisticsOps.prediction(query_result, record_datatype, number_of_future_vals=args.get('predict'), time_spec=args.get('seconds'), key=args.get('key'))
		else:
			graph_result_x, graph_result_y = MatplotlibWrapper.create_graph(query_result, record_datatype, graph_type=args.get('type'), send_as_bits=args.get('send_as_bits'),
																			ylabel=args.get('ylabel'), key=args.get('key'))
			graph_result_dict = {
				"xaxis": graph_result_x,
				"yaxis": graph_result_y,
				"record_type": record_datatype
			}

		self.BO.send_data(self.response_topic, 2, False, json.dumps(graph_result_dict))

	# self.BO.send_data(self.response_topic, 2, False, "GRAPH RESPONSE")
	def database_ops(self, topic, requested_broker_name, args):
		result = DbDataManipulation.update_database(topic, requested_broker_name, args)

	def delete_data_after(self, topic, requsted_broker_name, args):
		DbDataManipulation.delete_all_data_older_than(requsted_broker_name, topic, args)

	def run_clean_db_procedure(self, topic, requested_broker_name, args):
		print("Number of deleted: ", DbDataManipulation.clean_db())

	def get_overview(self):
		result_dicts = DbDataExtractor.get_db_overview()
		result_string = ""
		total = 0
		for broker_name, topic_data_dict in result_dicts.items():
			total_count_per_broker = 0
			result_string += "BROKER NAME: {0}\n".format(broker_name)
			result_string += "=====================================\n"
			for topic_name, topic_count in topic_data_dict.items():
				result_string += "Topic value: {0} -- Message count: {1}\n".format(topic_name,topic_count)
				total_count_per_broker += topic_count
			result_string += "Total for broker: {0}\n".format(total_count_per_broker)
			total += total_count_per_broker
		result_string += "Total for all brokers: {0}\n".format(total)

		self.BO.send_data(self.response_topic, 2, False, result_string)

	def get_help_ops(self, topic, requested_broker_name, args):
		s = """ 
		Hello user, there are following possibilities
		for usage of this software:
		------------------------------------------------------
		get-help : Print out help message
		get-stats: Return stats for the specified broker and topic
					Example of usage:
					get-stats [Broker name] [Topic name] [...]
		get-graph: Return graphs
		"""
		# print("HELP requested", args)
		self.BO.send_data(self.response_topic, 2, False, str(s))
