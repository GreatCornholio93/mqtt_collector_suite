import json
import uuid
from threading import Event
from MqttManager.MqttConnection import MqttHandler


class DataTransferer(object):
	BASE_REQUEST_TOPIC = "stats/testing/"
	BASE_RESPONSE_TOPIC = "stats/testing/response/"

	def __init__(self, server_address, port=1883, user_name="", password="", client_id=""):
		self.mqtt = None
		if server_address:
			self.mqtt = MqttHandler(server_address, port, username=user_name, password=password, client_id=client_id)

		self.request_pool = dict()
		self.response_pool = dict()

	def register_topic(self, topic):
		"""
		Register specific topic.
		:param topic: Topic string.
		"""
		self.mqtt.register_topic(topic, self.mqtt_api_response_callback)

	def register_request(self):
		"""
		Generate and register request ID.
		:return: Unique request ID.
		"""
		uid = str(uuid.uuid4())
		self.request_pool[uid] = Event()  # create new waiting event default se to False
		return uid

	def mqtt_api_response_callback(self, client, userdata, msg):
		"""
		Callback for incoming responses from server.
		"""
		response_id = msg.topic.split('/')[-1]
		if response_id in self.request_pool:
			print("Incoming response on request {}.".format(response_id))
			# print(msg.payload)
			try:
				response = msg.payload.decode('utf-8')
				# print("DEBUG: ", response)
				self.response_pool[response_id] = response
			except ValueError:
				print("Cannot parse response from server.")
				self.response_pool[response_id] = None

			event = self.request_pool[response_id]
			event.set()

		else:
			print("unknown response id")

	def wait_for_response(self, request_id, timeout=20):
		"""
		Check if request with given request_id receive response from server.
		Return server response if is available otherwise block program and wait for response.
		:param request_id: ID of request.
		:param timeout: Waiting timeout in seconds.
		:return: Response object dictionary.
		"""
		if request_id in self.request_pool:
			event = self.request_pool[request_id]
			if event.wait(timeout):
				return self.response_pool.pop(request_id)
			else:
				print("Request '{}' timeout. EspHubServer is probably unavailable.".format(request_id))
				return None

	def get_request_topic(self, uuid):
		"""
		Provides topic for MQTT api request.
		:param uuid: Unique request ID.
		:return: Request topic.
		"""
		return "{}{}".format(DataTransferer.BASE_REQUEST_TOPIC, uuid)

	def get_help(self):
		uid = self.register_request()
		self.mqtt.publish(self.get_request_topic(uid), "get-help", qos=1)

		response = self.wait_for_response(uid)

		return response

	def get_data_graph(self, broker_name, topic_name, args):
		"""
		:param broker_name: string that contains name of the broker
		:param topic_name: string that contains name of the topic
		:param start_time: string that contains start time (TODO: specify format)
		:param end_time: string that contains end time (TODO: specify format)
		:param last: example: 10s (for 10 seconds), 1d (1 day) etc.
		:return: there should be returned json that was received from broker
		"""
		# if not self.mqtt:
		#	print(
		#		"Get-devices command is available only with broker credentials (option -b). For more info see: "
		#		"ImageTransmitter --help.")
		#	return

		request_string = "get-graph {} {}".format(broker_name, topic_name)
		for key, val in args.items():
			if val:
				request_string += " {} {}".format(key, val)
		# print("DEBUG", request_string)
		uid = self.register_request()
		self.mqtt.publish(self.get_request_topic(uid), request_string, qos=1)

		response = self.wait_for_response(uid)
		return response


	def get_data_stats(self, broker_name, topic_name, args):
		"""
		:param broker_name: string that contains name of the broker
		:param topic_name: string that contains name of the topic
		:param start_time: string that contains start time (TODO: specify format)
		:param end_time: string that contains end time (TODO: specify format)
		:param last: example: 10s (for 10 seconds), 1d (1 day) etc.
		:return: there should be returned json that was received from broker
		"""
		# if not self.mqtt:
		#	print(
		#		"Get-devices command is available only with broker credentials (option -b). For more info see: "
		#		"ImageTransmitter --help.")
		#	return

		request_string = "get-stats {} {}".format(broker_name, topic_name)
		for key, val in args.items():
			if val:
				request_string += " {} {}".format(key, val)
		# print("DEBUG", request_string)
		uid = self.register_request()
		self.mqtt.publish(self.get_request_topic(uid), request_string, qos=1)

		response = self.wait_for_response(uid)

		return response

