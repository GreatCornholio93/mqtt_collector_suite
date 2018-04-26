from MatplotlibOps import MatplotlibWrapper
from MqttManager import DataTransferer
from configurator import Configurator

import click
import json


@click.group()
@click.option('-h', '--host', type=str, default=None, help="MQTT broker address or name.")
@click.option('-p', '--port', type=int, default=1883, help="MQTT broker port number.")
@click.option('-u', '--user-name', type=str, default='', help="MQTT broker user name.")
@click.option('-P', '--password', type=str, default='', help="MQTT broker password.")
@click.option('-i', '--client-id', type=str, default='', help="Custom client ID for MQTT broker.")
@click.option('-t', '--request-topic', default=None,
			  help="MQTT topic for which MQTT data collector listen for requests.")
@click.option('-c', '--config', is_flag=True, default=False, help="Load settings from config file.")
@click.pass_context
def cli(ctx, host, port, user_name, password, client_id, request_topic, config):
	"""
	MQTT Tool (MQTTT) provide user interface for MQTT data collector library.
	With this library is possible to easily obtain data from server, show plots and control database content.
	"""
	if config:
		config_master = Configurator()
		config = config_master.get_broker_info_from_config()
		dt = DataTransferer.DataTransferer(config.get("broker_addr"))
		dt.register_topic("{}/response/+".format(config.get("statistics")))
		ctx.obj = dt
	elif host and request_topic:
		dt = DataTransferer.DataTransferer(host, port=port, user_name=user_name, password=password, client_id=client_id)
		dt.register_topic("{}/response/+".format(request_topic))  # register topic for server responses
		ctx.obj = dt
	else:
		click.echo("ERROR: missing option --host or --request-topic")
		exit(1)


@cli.command('get-graph')
@click.option('-b', '--requested-broker', required=True, help="Requested broker name.")
@click.option('-r', '--requested-topic', required=True, help="Requested data topic.")
@click.option('-e', '--export-graph', is_flag=True, default=False, help="Graph will be created in the program folder")
@click.option('--start', type=str, required=False, help="Show data from date (in format ...)")
@click.option('--end', type=str, required=False, help="Show data to date (in format ...)")
@click.option('--last', type=str, required=False, help="Show data from last m/h/d (in format ...)")
@click.option('-x', '--x-axis', type=str, default="X Axis", help="Define label of X Axis")
@click.option('-y', '--y-axis', type=str, default="Y Axis", help="Define label of y Axis")
@click.option('--key', type=str, required=False,
			  help="Displayed key from JSON if you request data from JSON format topic.")
@click.option('--ops', type=click.Choice(['regression']), required=False,
			  help="Select one of summarization operations.")
@click.pass_obj
def get_graph(dt, requested_broker, requested_topic, export_graph, start, end, last, x_axis, y_axis, key, ops):
	"""
	Prompt plot based on data from server.
	"""

	datas_in_json = dt.get_data_graph(requested_broker, requested_topic,
									  {'last': last, 'from': start, 'to': end, 'key': key, 'ops': ops})
	datas_converted = json.loads(datas_in_json)
	# # build graphz
	if ops and ops.lower() == "regression":
		print("Calling method for creating regression graph")
		MatplotlibWrapper.create_graph_regression(datas_converted['x_dates'], datas_converted['y_values'],
												  datas_converted['x_regression'],
												  datas_converted['y_linear_regression'],
												  datas_converted['y_quadratic_regression'],
												  datas_converted['y_new_quadratic_regression'])
	elif 'xaxis' in datas_converted and 'yaxis' in datas_converted and 'record_type' in datas_converted:
		MatplotlibWrapper.create_graph(datas_converted['xaxis'], datas_converted['yaxis'],
									   datas_converted['record_type'], graph_type=None,
									   send_as_bits=False, xlabel=x_axis, ylabel=y_axis,
									   export_graph=export_graph)


@cli.command('get-stats')
@click.option('-b', '--requested-broker', required=True, help="Requested broker name.")
@click.option('-r', '--requested-topic', required=True, help="Requested data topic.")
@click.option('--start', type=str, required=False, help="Show data from date (in format ...)")
@click.option('--end', type=str, required=False, help="Show data to date (in format ...)")
@click.option('--last', type=str, required=False, help="Show data from last m/h/d (in format ...)")
@click.option('--ops', type=click.Choice(['sum', 'avg', 'min', 'max']), required=False,
			  help="Select one of summarization operations.")
@click.option('--key', type=str, required=False,
			  help="Displayed key from JSON if you request data from JSON format topic.")
@click.pass_obj
def get_stats(dt, requested_broker, requested_topic, start, end, last, ops, key):
	"""
	:param dt:
	:param requested_broker:
	:param requested_topic:
	:param export_graph:
	:param start:
	:param end:
	:param last:
	:param ops:
	:param key:
	:return:
	"""
	datas_in_json = dt.get_data_stats(requested_broker, requested_topic,
									  {'last': last, 'from': start, 'to': end, 'ops': ops, 'key': key})
	datas_converted = json.loads(datas_in_json)
	print(datas_converted)


if __name__ == "__main__":
	cli()
