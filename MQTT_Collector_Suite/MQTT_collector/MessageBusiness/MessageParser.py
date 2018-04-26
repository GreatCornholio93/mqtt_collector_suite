import datetime
import json
import re
from concurrent.futures import ThreadPoolExecutor
from json import JSONDecodeError

import sys
from Models.topic import *
from multiprocessing import Event, Queue, Process

from DataAccess.DataAcessLogic import DataAccessLogic
from DatabaseBusiness import database_manager
from Logger.logger import Logger
from MessageBusiness.SimpleRegexParser import SimpleRegexParser
from Models.Data import Data

from configurator import Configurator

log = Logger.get_logger()

class MyMessage(object):
	def __init__(self, msg):
		self.topic = msg.topic
		self.qos = msg.qos
		self.payload = msg.payload

class MessageParser(object):
	def __init__(self, topics):
		"""
		Initiate Message parser for specific topics.
		:param topics: List of Topic objects
		:type list
		"""

		# contains pairs data_type_name : callback_for_given_type
		self.callback_mapper = {'JSON': self._json_callback,
								'RAW': self._raw_callback,
								'REGEX': self._regex_callback,
								'STATS': self._stats_callback  # special mark for statistics request from user
								}
		self.regex_mapper = {}  # contain pairs topic_pattern_in_regex_format : topic_pattern_in_standard_format
		self.topic_mapper = {}  # contain pairs topic_pattern_string : topic_object
		conf = Configurator()
		self.max_data_size = conf.get_max_size_limit()
		number_of_workers = conf.get_number_of_workers()

		for topic in topics:
			self.topic_mapper[topic.topic_value] = topic
			if topic.has_wildcard():
				self.regex_mapper[self.translate_topic_to_regex(topic.topic_value)] = topic.topic_value
		print(self.topic_mapper)
		self.end_event = Event()
		self.result_queue = Queue()
		workers = []
		for i in range(int(number_of_workers)):
			p = Process(target=self.callback_worker)
			workers.append(p)
			p.start()

	def translate_topic_to_regex(self, topic_with_wildcard):
		"""
		Replace wildcard characters in topic pattern with regular expressions
		:return: Topic value with regular expressions instead of wildcards.
		"""
		parts = topic_with_wildcard.split('/')
		regex_parts = []
		position = 0
		wildcard_end = r''
		for part in parts:
			if part == '+':
				# handle + wildcard character
				if position == len(parts) - 1:
					regex_parts.append(r'[\w\s\-\?\[\]\"\|.;,!$%^&*()=@§{}¨:<>]*[\/|\s]?')
				else:
					regex_parts.append(r'[\w\s\-\?\[\]\"\|.;,!$%^&*()=@§{}¨:<>]*')
			elif part == '#':
				# handle # wildcard chracter
				if position != len(parts) - 1:
					log.error("Invalid topic format '{}'. Wildcard character '#' must be last character.".format(
						topic_with_wildcard))
				wildcard_end = r'[\w\s\/\-\?\[\]\"\|.;,!$%^&*()=@§{}¨:<>]*'
			else:
				# no wildcard character
				regex_parts.append(part)

			position += 1

		return '^{}$'.format(r'\/'.join(regex_parts) + wildcard_end)

	def find_matching_pattern(self, incoming_topic):
		"""
		Find regular expression from regex_mapper which match given incoming_topic and return wildcard version of given topic.
		:param incoming_topic: Incoming topic from broker without wildcards.
		:type incoming_topic: str
		:return: Wildcard topic.
		"""
		incoming_topic = incoming_topic.replace('\\', '/')
		for topic_regex, topic in self.regex_mapper.items():
			evaluator = re.match(topic_regex, incoming_topic)
			if evaluator:
				return topic
		log.error(
			"Given topic '{}' does not match any stored topic pattern. Given topic probably was not registered.".format(
				incoming_topic))

	def callback_pooler(self, client, user_data, msg):
		new_msg = MyMessage(msg)
		self.result_queue.put((client._host, new_msg))

	def callback_worker(self):

		while not self.end_event.is_set():
			queue_item = self.result_queue.get()
			self.data_callback(queue_item[0], None, queue_item[1])

	def data_callback(self, client, user_data, msg):
		"""
		Public data parsing callback
		:param client:
		:param user_data:
		:param msg:
		:return:
		"""
		log.debug("Incoming data from '{}' in topic '{}'.".format(client, msg.topic))
		try:
			# accept callback and take out topic from it
			callback_topic = msg.topic
			topic_obj = None
			callback = None

			# check topic_mapper and if there is specific topic take it out
			if callback_topic in self.topic_mapper:
				topic_obj = self.topic_mapper[callback_topic]
				# get data type
				callback = self.callback_mapper.get(topic_obj.data_type)
			else:
				# if topic is not in mapper, find_matching_pattern is called
				wildcard_topic = self.find_matching_pattern(callback_topic)
				# print("CALLBACK TOPIC: %s" % (callback_topic))
				# print("WILDCARD TOPIC: %s" % (wildcard_topic))
				if wildcard_topic:
					# use evaluated topic for search of correct topic object
					topic_obj = self.topic_mapper[wildcard_topic]
					# new topic is added to mapper
					self.topic_mapper[callback_topic] = topic_obj
					callback = self.callback_mapper.get(topic_obj.data_type)

			# call callback with parameters and topic object
			# print(str(topic_obj.__dict__))
			if callback and topic_obj:
				callback(topic_obj, msg)

		except UnicodeDecodeError as e:
			log.error("Receive invalid unicode character.")
			log.error(e)

	def _raw_callback(self, topic, incoming_data):
		"""
		Callback to parse raw data.
		:param topic:
		:param incoming_data:
		:return:
		"""
		log.debug("_raw_callback was called")
		size_in_bytes = sys.getsizeof(incoming_data.payload)
		if self.max_data_size:
			if size_in_bytes < int(self.max_data_size):
				with database_manager.keep_session() as session:
					new_data = Data(date_time=datetime.datetime.now(),
									topic_value=topic.topic_value,
									broker_name=topic.broker_name,
									value=str(incoming_data.payload, 'utf-8'),
									qos=incoming_data.qos,
									topic_specific_value=incoming_data.topic)

					# print("Size of RAW message is: ", size_in_bytes)
					session.add(new_data)
			else:
				log.debug(
					"The input data is {0}B which is more than limit {1}".format(size_in_bytes, self.max_data_size))

	def _regex_callback(self, topic, incoming_data):
		"""
		Parse callback with regular expression
		:param topic:
		:param incoming_data:
		:return:
		"""
		log.debug("_regex callback was called")
		size_in_bytes = sys.getsizeof(incoming_data.payload)
		if self.max_data_size:
			if size_in_bytes < int(self.max_data_size):
				data_list = SimpleRegexParser.evaluate_regex(incoming_data.payload, topic.regex, topic.types)
				with database_manager.keep_session() as session:
					new_data = Data(date_time=datetime.datetime.now(),
									topic_value=topic.topic_value,
									broker_name=topic.broker_name,
									value=data_list,
									qos=incoming_data.qos,
									topic_specific_value=incoming_data.topic)
					size_in_bytes = sys.getsizeof(incoming_data.payload)
					# print("Size of REGEX message is: ", size_in_bytes)
					session.add(new_data)
			else:
				log.debug(
					"The input data is {0}B which is more than limit {1}".format(size_in_bytes, self.max_data_size))

	def _json_callback(self, topic, incoming_data):
		"""
		Parsing JSON.
		:param topic:
		:param incoming_data:
		:return:
		"""
		log.debug("_json_callback was called")
		size_in_bytes = sys.getsizeof(incoming_data.payload)
		if self.max_data_size:
			if size_in_bytes < int(self.max_data_size):
				with database_manager.keep_session() as session:
					try:
						value = json.loads(str(incoming_data.payload, 'utf-8'))
						new_data = Data(date_time=datetime.datetime.now(),
										topic_value=topic.topic_value,
										broker_name=topic.broker_name,
										value=value,
										qos=incoming_data.qos,
										topic_specific_value=incoming_data.topic)
						size_in_bytes = sys.getsizeof(incoming_data.payload)
						# print("Size of JSON message is: ", size_in_bytes)
						session.add(new_data)
					except(JSONDecodeError, ValueError):
						log.error("Message from topic: %s is not JSON" % topic.topic_value)
			else:
				log.debug(
					"The input data is {0}B which is more than limit {1}".format(size_in_bytes, self.max_data_size))

	def _stats_callback(self, topic, incoming_data):
		"""
		:param topic: topic object
		:param incoming_data: payload mqtt object
		:return: hapiness
		"""

		# solving issue #3 where every response should be identified for specific user (for moar info check issue #3)
		if topic.has_wildcard():
			specific_topic = incoming_data.topic
			splitted_spec_topic = specific_topic.split('/')
			# get last part of the splitted specific topic
			request_id = splitted_spec_topic[-1]
			if request_id == 'response':
				log.debug("Response Echo - ignoring.")
				return

			modded_topic = topic.topic_value.replace('+', "response/%s" % request_id)
			self.DA = DataAccessLogic(modded_topic, str(incoming_data.payload, 'utf-8'), topic.broker_name)
		else:
			response_topic = "%s/response" % topic.topic_value
			self.DA = DataAccessLogic(response_topic, str(incoming_data.payload, 'utf-8'), topic.broker_name)
