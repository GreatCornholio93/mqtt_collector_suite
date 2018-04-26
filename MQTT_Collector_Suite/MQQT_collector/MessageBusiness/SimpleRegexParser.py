from Logger.logger import Logger
from Models.topic import Topic
import re

log = Logger.get_logger()


class SimpleRegexParser(object):
	SPECIAL_SEQUENCE = {'%JSON': ('', 'JSON'),
						'%BYTE': ('', 'BYTE'),
						'%RAW': ('', 'RAW'),
						'%STATS': ('', 'STATS')
						}

	CONVERT_TABLE = {'%d': ('([-+]?\d+)', 'INT'),
					 '%f': ('([-+]?\d+\.?\d+|[-+]?\d+)', 'FLOAT'),
					 '%b': ('(true|false)', 'BOOL'),
					 '%s': ('(\w+)', 'STRING')}
	BACK_CONVERT_TABLE = {'INT': lambda k: int(k),
						  'FLOAT': lambda k: float(k),
						  'BOOL': lambda k: bool(k),
						  'STRING': lambda k: k}

	def __init__(self, config_data=list()):
		"""
		Provide methods to conversion from custom simple regex 'sRegex' to valid pyRegex
		:param config_data: List of tuples contains topic and simpleRegex (e.g. [(topic, regex), (topic, regex)] )
		:type list
		:raises ValueError: Raise ValueError when duplicate or None topic detected
		"""
		self._config_data = None
		for item in config_data:
			if len(item) < 1:
				raise ValueError("Input items must contain at least value 'topic'")
		if self.check_topics_duplicity([item[0] for item in config_data]):
			self._config_data = config_data
		else:
			raise ValueError("Duplicate topics detected.")

	@staticmethod
	def evaluate_regex(data, regex, types):
		"""
		Evaluate regular expression and return data in defined format.
		:param data: Raw incoming data.
		:type data: str
		:param regex: Python regular expression.
		:type regex: str
		:param types: List of result types.
		:type types: list
		:return: List of parsed values.
		"""
		evaluator = re.match(regex, data.decode('utf-8'))
		if evaluator:
			groups = evaluator.groups()
			if len(groups) != len(types):
				raise ValueError(
					"Count of regex result does not match count of given types. Regex has {} results, given types has {} values.".format(
						len(groups), len(types)))

			results = []
			for i in range(0, len(groups)):
				convert_func = SimpleRegexParser.BACK_CONVERT_TABLE[types[i]]
				results.append(convert_func(groups[i]))
			return results
		else:
			log.warning("Given data does not match regex '{}'.".format(regex))

	def convert_regex(self, simple_regex):
		"""
		Convert sRegex to regular pyRegex
		:param simple_regex: Simplified regex expression.
		:type str
		:return: Regular pyRegex expression
		"""
		# find special sequences
		for sequence, value_pattern in self.SPECIAL_SEQUENCE.items():
			if simple_regex.find(sequence) > -1:
				return {'simple_regex': simple_regex, 'regex': '', 'data_type': value_pattern[1]}

		# find sequences
		types = []
		start = 0
		while start < len(simple_regex):
			matches = []
			for sequence, value_pattern in self.CONVERT_TABLE.items():
				matches.append((simple_regex.find(sequence, start), value_pattern[1]))

			ordered_sequence = [item for item in sorted(matches, key=lambda k: k[0]) if item[0] > -1]
			if ordered_sequence:
				first_sequence = ordered_sequence[0]
				start = first_sequence[0] + 1
				types.append(first_sequence[1])
			else:
				break

		# replace sequences
		regex = simple_regex
		for sequence, value_pattern in self.CONVERT_TABLE.items():
			regex = str.replace(regex, sequence, value_pattern[0])

		return {'regex': regex, 'types': types, 'data_type': 'REGEX'}

	def _check_if_regex_match_types(self, regex, types):
		"""
		Check if final regex groups match count of given types.
		:param regex: Regular expression in pyRegex format.
		:param types: List of types.
		:return: True/False
		"""
		return True if re.compile(regex).groups == len(types) else False

	def check_topics_duplicity(self, topics):
		"""
		Check if all topics are unique include overlapping e.g.
		:return True = no duplicity, False = duplicity
		"""
		# TODO check topics overlapping
		return True if len(topics) == len(set(topics)) else False

	def convert_regexes(self):
		"""
		Convert regexes into regular pyRegexes
		:return: list of dictionaries contains 'topic', 'regex', 'simple_regex', list of 'types'
		:raises ValueError: raiser value error if data are in incorrect format
		"""
		if self._config_data:
			result_lst = []
			for item in self._config_data:
				# check if config contains regex, if not use RAW format
				if len(item) == 1:
					result_lst.append({'topic': item[0], 'data_type': 'RAW'})
					continue
				if len(item) > 1 and item[1] == '':
					result_lst.append({'topic': item[0], 'data_type': 'RAW'})
					continue

				result = self.convert_regex(item[1])

				# in case when regex from user is valid pyRegex format - user have to define parsed types by own
				if not result.get('types') and len(item) >= 3:
					result['types'] = item[2]

				# check if count of types match count of regex groups
				if result['regex'] and not self._check_if_regex_match_types(result['regex'], result['types']):
					raise ValueError(
						'Count of regex groups does not match with count of types. Regex {} has {} groups and types list contain {} types.'. \
						format(result['regex'], re.compile(result['regex']).groups, len(result['types'])))

				result['topic'] = item[0]
				result['simple_regex'] = item[1]
				result_lst.append(result)
			return result_lst
		else:
			raise ValueError("No input data defined.")

	def convert_to_topics(self):
		"""
		Convert given regexes into Topic objects.
		:return: List of Topic objects.
		"""
		topics = []
		for topic_dic in self.convert_regexes():
			topics.append(Topic(topic_value=topic_dic.get('topic'), data_type=topic_dic.get('data_type', ''),
								simple_regex=topic_dic.get('simple_regex', ''),
								regex=topic_dic.get('regex', ''), types=topic_dic.get('types', list())))
		return topics
