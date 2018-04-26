from setuptools import setup, find_packages

setup(
	name='MqttCollectorSuite',
	version='0.1',
	author='Kevin Kiedron',
	packages=find_packages(),
	include_package_data=True,
	install_requires=[
		'Click',
		'paho-mqtt',
		'configparser',
		'sqlalchemy',
		'mysqlclient',
		'pandas',
		'sklearn',
		'matplotlib',
		'numpy',
		'scipy',
		'flask'
	]
)
