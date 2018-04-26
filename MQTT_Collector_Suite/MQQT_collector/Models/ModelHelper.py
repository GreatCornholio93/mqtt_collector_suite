from Models.broker import Broker
from MessageBusiness.SimpleRegexParser import SimpleRegexParser


class ModelHelper(object):
	def __init__(self):
		pass

	@staticmethod
	def build_broker_from_list(list_broker_params):
		list_of_brokers = []
		for broker_params in list_broker_params:
			new_broker = Broker(broker_name=broker_params[0], broker_addr=broker_params[1], server_port=broker_params[3])

			srp = SimpleRegexParser(broker_params[2])
			topics = srp.convert_to_topics()
			for topic in topics:
				topic.broker_name = new_broker.broker_name

			new_broker.topics = topics
			list_of_brokers.append(new_broker)
		return list_of_brokers
