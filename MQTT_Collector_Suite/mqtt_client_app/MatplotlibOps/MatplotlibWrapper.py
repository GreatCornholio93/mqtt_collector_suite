import datetime
import io
import uuid

import dateutil
import matplotlib.pyplot as plt
import matplotlib

graph_types_map = {
	'solid': '-',
	'dashed': '--',
	'dashdot': '-.',
	'dotted': ':'
}

DATA_TYPE_REGEX = 'REGEX'
DATA_TYPE_JSON = 'JSON'
DATA_TYPE_STATS = 'STATS'
DATA_TYPE_RAW = 'RAW'
VALUE_TYPE_INT = 'INT'
VALUE_TYPE_FLOAT = 'FLOAT'
VALUE_TYPE_BOOL = 'BOOL'
VALUE_TYPE_STRING = 'STR'


def is_number(s):
	try:
		float(s)  # for int, long and float
	except ValueError:
		return False

	return True


def generate_file_name(extension):
	"""
	Generate unique name for the file, extension must be given
	:param extension: extension string
	:return: string that represents name of the file
	"""
	unique_filename = str(uuid.uuid4())
	return str("{}.{}".format(unique_filename, extension))


def create_graph(x_axis, y_axis, type, graph_type="solid", send_as_bits=None, xlabel="X Axis", ylabel="Y Axis",
				 export_graph=False):
	"""
	This method will generate graph due to given conditions
	:param xlabel:
	:param key: if you want to draw graph based on the JSON data please fill this attribute with key of the data - value referenced by key must be primitive data type
	:param array_of_datas: mandatory parameter array of the datas - graph will be generated from those datas
	:param graph_type: optional parameter - specifies look of the graph supported values: solid, dashed, dashdot, dotted
	:param send_as_bits: optional parameter accepts bool value - datas could be send as bits for some special cases
	:param ylabel: optional parameter - use string for labeling of the Y axis
	:return graph or bytes based on specified conditions
	"""
	date_times = []
	value_list = []
	print("X AXIS LEN ", len(x_axis))
	print("Y AXIS LEN ", len(y_axis))
	if type == DATA_TYPE_REGEX:
		for i in range(len(x_axis)):
			date_times.append(datetime.datetime.strptime(x_axis[i], "%Y-%m-%d %H:%M:%S"))
			value_list.append(y_axis[0][i])

		dates = matplotlib.dates.date2num(date_times)

		if graph_type:
			if graph_type not in graph_types_map:
				print("Unsupported operation, use solid, dashed, dashdot, dotted...")
				return None
		else:
			graph_type = 'solid'
		type_of_graph = graph_types_map[graph_type.lower()]
		for i in y_axis:
			matplotlib.pyplot.plot_date(dates, i, type_of_graph)


	elif type == DATA_TYPE_JSON:
		json_from_db = y_axis
		# date_times.append(datetime.datetime.strptime(array_of_datas["xaxis"]))
		for date in x_axis:
			date_times.append(datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))

		dates = matplotlib.dates.date2num(date_times)
		list_of_vals = []
		for i in json_from_db:
			if graph_type:
				list_of_vals.append(i)
				if graph_type not in graph_types_map:
					print("Unsupported operation, use solid, dashed, dashdot, dotted...")
					return None
				type_of_graph = graph_types_map[graph_type.lower()]
				matplotlib.pyplot.plot_date(dates, i, 'solid')
			else:
				matplotlib.pyplot.plot_date(dates, i, '-')



	plt.xlabel(xlabel)
	plt.ylabel(ylabel)
	if str(send_as_bits).lower().strip() == "true":
		bfile = io.BytesIO()
		plt.savefig(bfile)
		return bfile.getvalue()
	elif str(send_as_bits).lower().strip() == "false":
		if export_graph:
			plt.savefig(generate_file_name("png"))
		else:
			plt.show()
		return None


def create_graph_regression(x_dates, y_values, x_regression, y_linear_regression, y_quadratic_regression, y_new_quadratic_prediction):
	date_times = []
	date_times_regression = []
	print(y_linear_regression)
	for i in range(len(x_dates)):
		date_times.append(datetime.datetime.strptime(x_dates[i], "%Y-%m-%d %H:%M:%S"))
	for i in range(len(x_regression)):
		date_times_regression.append(datetime.datetime.strptime(x_regression[i], "%Y-%m-%d %H:%M:%S"))
	dates = matplotlib.dates.date2num(date_times)
	dates_regression = matplotlib.dates.date2num(date_times_regression)
	for y_val in y_values:
		plt.plot_date(dates, y_val, marker='o', label="training data")  # plot original data

	#
	plt.plot_date(dates_regression, y_linear_regression, label='linear fit', linestyle='--', color='r') # linear prediction
	plt.plot_date(dates_regression, y_quadratic_regression, label='quadratic fit', color='g') # quadratic prediction
	plt.plot_date(dates, y_new_quadratic_prediction, linestyle='--', label="predicted quadratic fit")
	# plt.plot_date(dates, y_new_quadratic_prediction, marker='*', label='predicted values')

	plt.legend(loc='upper left')
	plt.show()
	pass
