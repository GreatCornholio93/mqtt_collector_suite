import uuid
import datetime
import io
import matplotlib
import matplotlib.pyplot as plt

from DatabaseBusiness import DbDataExtractor
from Logger.logger import Logger
from Models import topic
from Models.topic import Topic

log = Logger.get_logger()

graph_types_map = {
	'solid': '-',
	'dashed': '--',
	'dashdot': '-.',
	'dotted': ':'
}


def generate_file_name(extension):
	"""
	Generate unique name for the file, extension must be given
	:param extension: extension string
	:return: string that represents name of the file
	"""
	unique_filename = str(uuid.uuid4())
	return str("{}.{}".format(unique_filename, extension))


def create_graph(array_of_datas, type, graph_type=None, send_as_bits=False, ylabel="Y line numbers", key=None):
	"""
	This method will generate graph due to given conditions
	:param key: if you want to draw graph based on the JSON data please fill this attribute with key of the data - value referenced by key must be primitive data type
	:param array_of_datas: mandatory parameter array of the datas - graph will be generated from those datas
	:param graph_type: optional parameter - specifies look of the graph supported values: solid, dashed, dashdot, dotted
	:param send_as_bits: optional parameter accepts bool value - datas could be send as bits for some special cases
	:param ylabel: optional parameter - use string for labeling of the Y axis
	:return graph or bytes based on specified conditions
	"""
	date_times = []
	value_list = []
	merged_list = []
	if type == Topic.DATA_TYPE_REGEX:

		for item in array_of_datas:
			date_times.append(datetime.datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S"))
			value_list.append(item["value"])

		for index in range(len(value_list[0])):
			merged_list.append([item[index] for item in value_list])

	elif type == Topic.DATA_TYPE_JSON:
		if key:
			for item in array_of_datas:
				json_from_db = item["value"]
				if DbDataExtractor.is_number(json_from_db[key]):
					merged_list.append(json_from_db[key])
					date_times.append(datetime.datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S"))

			merged_list = [merged_list]
	dates = matplotlib.dates.date2num(date_times)
	list_of_vals = []
	for i in merged_list:
		list_of_vals.append(i)
		if graph_type:
			if graph_type not in graph_types_map:
				log.warning("Unsupported operation, use solid, dashed, dashdot, dotted...")
				return None
			type_of_graph = graph_types_map[graph_type.lower()]
			matplotlib.pyplot.plot_date(dates, i, str(type_of_graph))
		else:
			matplotlib.pyplot.plot_date(dates, i)

	plt.ylabel(ylabel)
	if str(send_as_bits).lower().strip() == "true":
		bfile = io.BytesIO()
		plt.savefig(bfile)
		return bfile.getvalue()
	elif str(send_as_bits).lower().strip() == "false" or send_as_bits is None:
		return [[item["date_time"] for item in array_of_datas], list_of_vals]
