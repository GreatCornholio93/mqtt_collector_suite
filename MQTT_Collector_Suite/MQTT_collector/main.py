import threading

from sqlalchemy import and_

from DatabaseBusiness import database_manager, DbTaskScheduler
from Models.broker import Broker
from Models.topic import Topic
from Models.ModelHelper import ModelHelper
from Ops.BrokerOps import BrokerOps
from DatabaseBusiness.database_manager import *
from Ops.BrokerOpsMapper import BrokerOpsMapper
from DataAccess import RestApi


def mqtt_collector_run():
	init_db()
	# creating db scheduled task
	DbTaskScheduler.create_scheduled_cleanup()

	conf = Configurator()
	brokers = conf.get_broker_info_from_config()

	brokerz = ModelHelper.build_broker_from_list(brokers)

	# remove topics from DB which are not in the config
	with database_manager.keep_session() as session:
		for broker in brokerz:
			db_broker = session.query(Broker).filter(Broker.broker_name == broker.broker_name).first()
			if db_broker:
				for db_topic in db_broker.topics:
					if not db_topic.topic_value in [t.topic_value for t in broker.topics]:
						# print(db_topic)
						session.query(Topic).filter(and_(Topic.topic_value == db_topic.topic_value,
														 Topic.broker_name == db_broker.broker_name)).delete()

	# merge objects from config with DB objects
	with database_manager.keep_session() as session:
		for broker in brokerz:
			session.merge(broker)

	brokers = []
	with database_manager.keep_weak_session() as session:
		brokers = session.query(Broker).all()

	for broker in brokers:
		opz = BrokerOps(broker)
		BrokerOpsMapper.add_into_broker_ops_mapper(broker.broker_name, opz)
		opz.connect()


threads = []

if __name__ == '__main__':
	print("Starting MQTT Collector v0.0")
	print("Author: Kevin Kiedron (KIE0012)")
	flask_app = RestApi.return_app()
	flask_app.debug = False
	flask_app.use_reloader = False
	t1 = threading.Thread(target=mqtt_collector_run)
	threads.append(t1)
	t2 = threading.Thread(target=flask_app.run)
	threads.append(t2)
	t2.start()
	t1.start()
