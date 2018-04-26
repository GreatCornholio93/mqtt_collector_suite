from DatabaseBusiness import database_manager
from MQTTBusiness.mqtt_manager import MqttManager
from MessageBusiness.MessageParser import MessageParser
from Models.topic import Topic


class BrokerOps(object):
	def __init__(self, broker):
		self.broker = broker
		with database_manager.keep_test_session() as session:
			topics = session.query(Topic).filter(Topic.broker_name == broker.broker_name).all()
			self.mp = MessageParser(topics)
			self.mqtt_manager = MqttManager(broker.broker_addr, broker.server_port, topics, self.mp.callback_pooler)

	# setting callback

	def connect(self):
		self.mqtt_manager.connect_to_broker(self._register_topics)

	def _register_topics(self, client, userdata, flags, rc):
		for item in self.broker.topics:
			# topic = helperclass.get('type') # TODO implement this later
			if item and item.data_type != Topic.DATA_TYPE_STATS:
				self.mqtt_manager.register_topic(item.topic_value, self.mp.callback_pooler)
			elif item and item.data_type == Topic.DATA_TYPE_STATS:
				self.mqtt_manager.register_topic(item.topic_value, self.mp.data_callback)

	def send_data(self, topic, qos, retain, payload=None):
		self.mqtt_manager.publish_data(topic, qos, retain, payload)
