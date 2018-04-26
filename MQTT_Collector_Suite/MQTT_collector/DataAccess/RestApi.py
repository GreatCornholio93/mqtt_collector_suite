from flask import Flask, jsonify, request
from DatabaseBusiness import DbDataExtractor

app = Flask(__name__)


@app.route('/<string:broker_name>/<path:topic_name>')
def get_all_values_from_given_broker_and_topic(broker_name, topic_name):
	"""
	This method provides datas in json format those data are requested via HTTP protocol format is following
	/BROKER_NAME/TOPIC?from=xxx&to&last
	it uses flask framework
	there is also pseudo parameter args that contains values from query (values after ? char)
	:param broker_name: name of the broker which from user wants to get data
	:param topic_name: name of the topic which from user wants to get data
	:return: jsoned datas in best case otherwise jsoned error message
	"""
	print("broker name: ", broker_name)
	print("topic name: ", topic_name)
	from_date = request.args.get('from')
	to_date = request.args.get('to')
	last = request.args.get('last')
	response, type = DbDataExtractor.obtain_data(topic_name, broker_name, {"from": from_date, "to": to_date, "last": last})
	return jsonify(response)


def return_app():
	return app
