import threading

import matplotlib
import json
import numpy
import pandas as pd
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import PolynomialFeatures
from sklearn.cross_validation import train_test_split
import matplotlib.pyplot as plt
from matplotlib import dates as mpl_dates
import numpy as np
from sklearn import datasets, linear_model, model_selection, preprocessing, cross_validation
from sklearn.metrics import mean_squared_error, r2_score
import time
from datetime import datetime, timedelta
# from statsmodels.tsa.ar_model import AR
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

from DatabaseBusiness import DbDataExtractor
from Models.topic import Topic
from Logger import logger

log = logger.Logger.get_logger()


def dtt2timestamp(dates):
	time_arr_stamp = []
	for dat in dates:
		# timestamp = (dat.hour * 60 + dat.minute) * 60 + dat.second
		timestamp = time.mktime(dat.timetuple())
		time_arr_stamp.append(timestamp)
	return time_arr_stamp


def regresion_ops(data_list, data_type, key=None):
	"""
	Calculate linear and quadratic regression on input data_list.
	:param data_list: List of Data records in serialized format (like dictionary)
	:param data_type: Type of data value, e.g. JSON, REGEX, ...
	:param key: key in dictionary if Data is type of JSON.
	:return: Dictionary of vectors which contain keys X axe values, and regressions.
	"""
	# TODO why is incoming data list in dictionary format why it is not an object type of Data ???
	x_dates = [mpl_dates.date2num(datetime.strptime(item["date_time"], "%Y-%m-%d %H:%M:%S")) for item in data_list]
	y_values = []

	if data_type == Topic.DATA_TYPE_REGEX:
		# TODO regression is not ready for regex when more than one value appear in regex e.g. "%d,%d,%d" ->  [1, 2, 3]
		raw_values = [item.get('value') for item in data_list]
		if not raw_values:
			log.warning("Query does not contain any data. Cannot calculate regression without data values.")
			return
		for index in range(len(raw_values[0])):
			y_values.append([item[index] for item in raw_values])

	elif data_type == Topic.DATA_TYPE_JSON:
		if key:
			for item in data_list:
				json_data = item.get('value')
				if json_data and DbDataExtractor.is_number(json_data.get(key)):
					y_values.append(json_data[key])

	else:
		log.error("Unrecognizable type of datas '{}'.".format(data_type))
		return

	original_axe_x = np.array(x_dates, dtype=np.float64).reshape((-1, 1))  # create numpy 1D array for X axe
	# create training and testing chunks
	x_training_list, x_test_list, y_train_list, y_test_list = train_test_split(original_axe_x, y_values[0],
																			   test_size=0.3,
																			   random_state=0)

	# linear regression
	lin_regression = LinearRegression()
	lin_regression.fit(x_training_list, y_train_list)
	y_linear_prediction = lin_regression.predict(x_test_list)

	# quadratic regression
	qua_regression = LinearRegression()
	poly = PolynomialFeatures(degree=5)
	x_poly_list = poly.fit_transform(original_axe_x)
	qua_regression.fit(x_poly_list, y_values[0])
	y_quadratic_prediction = qua_regression.predict(poly.fit_transform(x_test_list))
	y_new_quadratic_prediction = qua_regression.predict(x_poly_list)

	# test plot
	# plt.plot_date(x_dates, y_values, marker='o', label="training data") # plot original data
	# plt.plot_date(x_test_list, y_linear_prediction, label='linear fit', linestyle='--', color='r') # linear prediction
	# plt.plot_date(x_test_list, y_quadratic_prediction, label='quadratic fit', color='g') # quadratic prediction
	# plt.plot_date(original_axe_x, y_new_quadratic_prediction, marker='*', label='predicted values')
	# plt.plot_date(original_axe_x, y_new_quadratic_prediction, linestyle='--', label="predicted quadratic fit")
	# plt.legend(loc='upper left')
	# plt.xlabel("Year")
	# plt.ylabel("Predicted Growth")

	# convert matplot lib dates -> datetime object -> datetime string
	x_regressions_axe = [mpl_dates.num2date(item[0]).strftime("%Y-%m-%d %H:%M:%S") for item in x_test_list.tolist()]

	response = {'x_dates': [item['date_time'] for item in data_list],  # original X datetime values (in str ISO format)
				'y_values': y_values,
				'x_regression': x_regressions_axe,
				'y_linear_regression': y_linear_prediction.tolist(),
				'y_quadratic_regression': y_quadratic_prediction.tolist(),
				'y_new_quadratic_regression': y_new_quadratic_prediction.tolist(),
				}
	return response


def prediction(data_list, data_type, number_of_future_vals , time_spec,key=None):
	"""
	Calculate prediction
	:param data_list: List of Data records in serialized format (like dictionary)
	:param data_type: Type of data value, e.g. JSON, REGEX, ...
	:param number_of_future_vals: number of values to predict
	:param time_spec: number of seconds for date representation
	:param key: key in dictionary if Data is type of JSON.
	:return: Dictionary of vectors which contain keys X axe values, and predictions.
	"""
	x_dates = [item["date_time"] for item in data_list]

	y_values = []
	if data_type == Topic.DATA_TYPE_REGEX:
		# TODO regression is not ready for regex when more than one value appear in regex e.g. "%d,%d,%d" ->  [1, 2, 3]
		raw_values = [item.get('value') for item in data_list]
		if not raw_values:
			log.warning("Query does not contain any data. Cannot calculate regression without data values.")
			return
		for index in range(len(raw_values[0])):
			y_values.append([item[index] for item in raw_values])

	elif data_type == Topic.DATA_TYPE_JSON:
		if key:
			for item in data_list:
				json_data = item.get('value')
				if json_data and DbDataExtractor.is_number(json_data.get(key)):
					y_values.append(json_data[key])

	dti = pd.DatetimeIndex(x_dates)
	d = {'y': y_values[0]}
	df = pd.DataFrame(data=d, index=dti)

	array = df.values
	# print("test")

	X = df

	Y = df.values
	forecast_out = int(number_of_future_vals)  # predicting 30 days into future
	df['Prediction'] = df[['y']].shift(-forecast_out)  # label column with data shifted 30 units up

	X = np.array(df.drop(['Prediction'], 1))

	X = preprocessing.scale(X)

	X_forecast = X[-forecast_out:]  # set X_forecast equal to last 30
	X = X[:-forecast_out]  # remove last 30 from X

	y = np.array(df['Prediction'])
	y = y[:-forecast_out]

	X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2)

	# print("x train", X_train)
	# print("y train", y_train)
	# Training
	clf = LinearRegression()
	clf.fit(X_train, y_train)
	# Testing
	confidence = clf.score(X_test, y_test)

	print("confidence: ", confidence)

	forecast_prediction = clf.predict(X_forecast)

	# print(forecast_prediction)
	df.dropna(inplace=True)
	df['forecast'] = np.nan
	last_date = df.iloc[-1].name
	last_unix = last_date.timestamp()
	one_day = 1
	next_unix = last_unix + one_day


	resultx = []
	resulty = []
	last_date_new = x_dates[-1]
	last_date_new = datetime.strptime(last_date_new, "%Y-%m-%d %H:%M:%S")
	last_unix_new = last_date_new + timedelta(0, int(time_spec))
	for i in forecast_prediction:
		resulty.append(i)
		resultx.append(last_unix_new.strftime("%Y-%m-%d %H:%M:%S"))
		last_unix_new = last_unix_new + timedelta(0, int(time_spec))
	print("x_dates:",x_dates)
	print("y_vals:", y_values)

	response = {'x_dates': [item['date_time'] for item in data_list],  # original X datetime values (in str ISO format)
				'x_predicted':resultx,
					'y_values': y_values[0],
					'y_predicted': resulty,
					'confidence': confidence
					}
	return json.dumps(response)


	# date_times_regression2 = []
	# date_times_regression = []
	# print(resulty)
	# for i in range(len(x_dates)):
	# 	date_times_regression.append(datetime.strptime(x_dates[i], "%Y-%m-%d %H:%M:%S"))
	# dates_regression = matplotlib.dates.date2num(date_times_regression)
	#
	# for i in range(len(resultx)):
	# 	date_times_regression2.append(datetime.strptime(resultx[i], "%Y-%m-%d %H:%M:%S"))
	# dates_regression2 = matplotlib.dates.date2num(date_times_regression2)
	# print("y_val", len(y_values[0]))
	# plt.plot_date(dates_regression, y_values[0],'-', label="training data")
	# plt.plot_date(dates_regression2, resulty,'-', label="training data")
	# #plt.plot_date(dates_regression, resulty, label='linear fit', linestyle='--',
	# 			  #			  color='r')  # linear prediction
	# # plt.plot_date(dates, y_new_quadratic_prediction, marker='*', label='predicted values')
	#
	# #plt.legend(loc='upper left')
	# plt.show()
