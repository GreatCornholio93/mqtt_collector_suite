import configparser
import ast
import os.path


class Configurator(object):
	def __init__(self):
		self.path_to_config = "config/config.ini"
		self.config_parser = configparser.RawConfigParser()

	def get_broker_info_from_config(self):
		try:
			if not os.path.exists(self.path_to_config):
				print("THERE IS NO CONFIG FILE")
				exit(1)
			self.config_parser.read(self.path_to_config)
			sections = self.config_parser.sections()
			for section in sections:
				section_info = self.config_parser[section]
				broker_addr = section_info["server_address"]
				broker_port = section_info["server_port"]

				statistics_topic = section_info.get("statistics", None)

				result_dict = {
					"broker_addr":broker_addr,
					"broker_port":broker_port,
					"statistics": statistics_topic
					}
				return result_dict

		except configparser.Error as e:
			# TODO: log into log
			print(e)