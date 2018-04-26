import paho.mqtt.client as mqtt

from Logger.logger import Logger


class MqttManager(object):
	def __init__(self, broker_addr, broker_port, topic_list, callback_method=None):
		self.logger = Logger.get_logger()
		self.broker_addr = broker_addr
		self.broker_port = broker_port
		self.topic_list = topic_list
		self.logger.debug("Creating MQTT instance for %s broker" % self.broker_addr)
		self.client = mqtt.Client("anon")
		self.on_message_listener = None
		self.callback_method = callback_method

	def on_connect(self, client, userdata, flags, rc):
		"""
		Deprecated not used anymore
		:param client:
		:param userdata:
		:param flags:
		:param rc:
		:return:
		"""
		# self.logger.debug("Subscribing")
		self.logger.debug("Topic list: ", self.topic_list)
		for topic in self.topic_list:
			self.logger.debug("Subscribing: %s" % topic.topic_value)
			client.subscribe(topic.topic_value)

	def on_message(self, client, userdata, msg):
		# pass
		self.logger.debug("Accepting message from %s" % msg.payload)
		if self.on_message_listener:
			self.on_message_listener(client, userdata, msg)

	def connect_to_broker(self, register_topic_method):
		"""
		Initiate connection to broker
		:param register_topic_method: handle on_connect event
		:return:
		"""
		self.client.connect(self.broker_addr)
		self.client.on_connect = register_topic_method
		# self.client.on_message = self.callback_method
		# self.client.on_message = write_to_debug.pls
		self.client.loop_forever()

	def register_topic(self, topic, callback):
		self.client.subscribe(topic)
		self.client.message_callback_add(topic, callback)

	def publish_data(self, topic, qos, retain, payload=None):
		if payload:
			self.client.publish(topic, payload, qos, retain)
