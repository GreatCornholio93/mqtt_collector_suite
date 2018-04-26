class BrokerOpsMapper(object):
	broker_opss = {}

	@staticmethod
	def add_into_broker_ops_mapper(broker_name, broker_ops_instance):
		BrokerOpsMapper.broker_opss[broker_name] = broker_ops_instance

	@staticmethod
	def get_broker_ops_mapper(broker_name):
		return BrokerOpsMapper.broker_opss[broker_name]
