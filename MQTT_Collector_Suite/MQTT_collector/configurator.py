import configparser
import ast
import os.path


class Configurator(object):
	def __init__(self):
		self.path_to_brokers_config = "config/brokers.ini"
		self.path_to_database_config = "config/database.ini"
		self.config_parser = configparser.RawConfigParser()

	def get_broker_info_from_config(self):

		broker_topics_regx = []
		list_of_broker_params = []

		try:
			if not os.path.exists(self.path_to_brokers_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_brokers_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				broker_name = section
				broker_addr = section_info["server_address"]
				broker_port = section_info["server_port"]
				broker_topics = section_info["topics"]

				statistics_topic = section_info.get("statistics", None)
				# TODO: Check if statistics_topic contains wildcard char
				if statistics_topic:
					statistics_topic_toup = (statistics_topic.replace('"', ''), '%STATS')
					statistics_topic_toup_wildcarded = ("{0}/+".format(statistics_topic.replace('"', '')), '%STATS')
				broker_topics = ast.literal_eval(broker_topics)
				config_list = []
				for topic_regex in broker_topics:
					pattern = [item.strip() for item in topic_regex.split(';')]
					pattern += [''] * (3 - len(pattern))
					pattern = tuple(pattern)
					toup_conf = (pattern[0], pattern[1])
					config_list.append(toup_conf)
					broker_topics_regx.append(pattern)
				if statistics_topic_toup:
					config_list.append(statistics_topic_toup)
					# for moar info check issue #3
					config_list.append(statistics_topic_toup_wildcarded)
				broker_params = (broker_name, broker_addr, config_list, broker_port)
				list_of_broker_params.append(broker_params)
			return list_of_broker_params
		except configparser.Error as e:
			# TODO: log into log
			print(e)

	def get_delete_data_older_than_value(self):
		delete_data_older_than = None
		try:
			if not os.path.exists(self.path_to_database_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_database_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				delete_data_older_than = section_info.get("delete_data_older_than", None)
			return delete_data_older_than
		except configparser.Error as e:
			print(e)

	def get_max_size_limit(self):
		max_size_limit = None
		try:
			if not os.path.exists(self.path_to_database_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_database_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				max_size_limit = section_info.get("stored_val_max_size_in_bytes", None)
			return max_size_limit
		except configparser.Error as e:
			print(e)

	def get_number_of_workers(self):
		number_of_workers = None
		try:
			if not os.path.exists(self.path_to_brokers_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_brokers_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				number_of_workers = section_info.get("number_of_workers", None)
				if not number_of_workers:
					number_of_workers = 1
			return number_of_workers
		except configparser.Error as e:
			print(e)

	def get_database_connection_string(self):
		try:
			if not os.path.exists(self.path_to_database_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_database_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				sql_server_type = section_info.get("sql_server_type", None)
				sql_server_address = section_info.get("sql_server_address", None)
				sql_server_database_name = section_info.get("sql_server_database_name", None)
				sql_server_login = section_info.get("sql_server_login", None)
				sql_server_password = section_info.get("sql_server_password", None)
				sql_server_port = section_info.get("sql_server_port", None)
				self.delete_data_older_than = section_info.get("delete_data_older_than", None)
			if not sql_server_database_name or not sql_server_address or not sql_server_type or not sql_server_login or not sql_server_password:
				print("ERROR: Database config file is not properly filled")
				exit(1)
			# Lets create connection string
			if sql_server_port:
				connection_string = "{0}://{1}:{2}@{3}:{4}/{5}".format(sql_server_type, sql_server_login,
																	   sql_server_password,
																	   sql_server_address, sql_server_port,
																	   sql_server_database_name)
			else:
				connection_string = "{0}://{1}:{2}@{3}/{4}".format(sql_server_type, sql_server_login,
																   sql_server_password,
																   sql_server_address,
																   sql_server_database_name)
			return connection_string
		except configparser.Error as e:
			# TODO: log into log
			print(e)
