import json
from multiprocessing import Process, Event, Queue, current_process
import paho.mqtt.client as mqtt
# from MQTTBusiness.mqtt_manager import MqttManager
import click
import random
import string
import time

QOS_LEVEL = 1


def random_regex():
	"""
	regex: "%d %s"
	:return:
	"""
	num = str(random.randint(1, 100000))
	s = ''.join([random.choice(string.ascii_letters) for _ in range(8)])
	return "benchmark/rand-reg", "{} {}".format(num, s), QOS_LEVEL


def random_json():
	"""
	regex: "json"
	:return:
	"""
	num = str(random.randint(1, 100000))
	s = ''.join([random.choice(string.ascii_letters) for _ in range(8)])
	dict = {
		"testing": "testing text",
		"value": "{}".format(num)
	}
	return "benchmark/rand-json", json.dumps(dict), QOS_LEVEL


def random_number():
	"""
	regex: "%d"
	:return:
	"""
	return "benchmark/rand-num", str(int(time.time())-1524496293), QOS_LEVEL


def unix_timestamp():
	return "benchmark/unix-timestamp", str(int(time.time())), QOS_LEVEL


def random_string():
	"""
	raw
	:return:
	"""
	return "benchmark/rand-string", ''.join(
		[random.choice(string.ascii_letters + string.digits) for _ in range(32)]), QOS_LEVEL


def print_results(results_dict, total_count):
	click.echo("{:20}{:20}".format("Process name", "Message count"))
	click.echo("-" * 40)
	for key, value in results_dict.items():
		click.echo("{:20}{:20}".format(key, value))
	click.echo("-" * 40)
	click.echo("Total count: {}".format(total_count))


# all operations function must return tuple of 3 values: (topic, message, qos)
# message must be string
operations = {
	'rand-num': random_number,
	#'rand-string': random_string,
	#'rand-reg': random_regex,
	#'rand-json': random_json,
	#'rand-unix': unix_timestamp
}


def worker(event, result_queue, ops_name, interval, mqtt_config):
	# counter = 0
	ops = operations[ops_name]
	p = current_process()
	random.seed(p.name)

	client = mqtt.Client()
	client.connect(mqtt_config.get('host'), mqtt_config.get('port', 1883), mqtt_config.get('keep_alive', 60))
	client.loop_start()

	while not event.is_set():
		topic, result, qos = ops()

		client.publish(topic, payload=result, qos=qos, retain=False)
		# print(result)
		# counter += 1

		# wait for interval + random jitter
		event.wait(interval + (random.randint(0, int((interval * 1000) / 1000))))

		# if (counter % 30) == 0:
		result_queue.put((ops_name, 1))
	# counter = 0

	client.loop_stop()


@click.command()
@click.option('-p', '--processes', type=int, default=1, help="Count of parallel processes.")
@click.option('-i', '--interval', type=int, default=500, help="Publishing interval in milliseconds.")
@click.option('-q', '--qos', type=int, default=0, help="QOS level of published messages.")
@click.option('-l', '--limit', type=int, default=0, help="Count of messages which will be send. 0 is infinite.")
def cli(processes, interval, qos, limit):
	interval = interval / 1000
	QOS_LEVEL = qos
	mqtt_config = {
		'host': 'tanas.eu',
		'port': 1883,
	}
	ops_list = list(operations.keys())
	workers = []
	end_event = Event()
	result_queue = Queue()

	click.echo("Start benchmarking.")

	for ops in ops_list:
		for i in range(processes):
			p = Process(target=worker, args=(end_event, result_queue, ops, interval, mqtt_config))
			workers.append(p)
			p.start()

	results = {}
	total_counter = 0
	try:
		while True:
			msg_obj = result_queue.get()
			operation, count = msg_obj
			total_counter += count

			if operation in results:
				results[operation] += count
			else:
				results[operation] = count

			# if (total_counter % 10) == 0:
			click.clear()
			print_results(results, total_counter)
			if total_counter > limit and limit != 0:
				end_event.set()
				for w in workers:
					w.join(3)
				return

	except KeyboardInterrupt:
		print("keyboard press")
		end_event.set()
		for w in workers:
			w.join(3)


if __name__ == "__main__":
	cli()