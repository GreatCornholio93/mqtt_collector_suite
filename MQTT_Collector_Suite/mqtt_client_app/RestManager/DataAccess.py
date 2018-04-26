import requests


class DataAccess:
	def __init__(self, url):
		self.url = url

	def getData(self, broker_name, topic_name, start_time = None, end_time = None, last = None):
		request_string = None
		if broker_name and topic_name and start_time and end_time:
			request_string = "{0}/{1}/{2}?from={3}&to={4}".format(self.url, broker_name, topic_name, start_time, end_time)
		elif broker_name and topic_name and last:
			request_string = "{0}/{1}/{2}?last={3}".format(self.url, broker_name, topic_name, last)
		elif broker_name and topic_name and start_time and end_time is None:
			request_string = "{0}/{1}/{2}?from={3}".format(self.url, broker_name, topic_name, start_time)
		else:
			request_string = None

		if request_string:
			return requests.get(request_string)
		else:
			return "ERROR: Incorrect input parameters."