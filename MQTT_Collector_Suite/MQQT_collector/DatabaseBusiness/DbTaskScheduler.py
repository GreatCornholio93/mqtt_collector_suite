from DatabaseBusiness import database_manager
from Logger.logger import Logger
from configurator import Configurator

log = Logger.get_logger()


def create_scheduled_cleanup():
	conf = Configurator()
	condition_time = conf.get_delete_data_older_than_value()
	delete_slq_string = "DROP EVENT IF EXISTS collector_scheduled_cleanup"
	sql_string = "CREATE EVENT collector_scheduled_cleanup ON SCHEDULE EVERY 1 HOUR ON COMPLETION NOT PRESERVE DO DELETE FROM " \
				 "data WHERE date_time < NOW() - INTERVAL :number_of_days DAY"
	with database_manager.keep_session() as session:
		# result = session.execute('SELECT * FROM my_table WHERE my_column = :val', {'val': 5})
		delete_res = session.execute(delete_slq_string)
		result = session.execute(sql_string, {'number_of_days': condition_time})
		log.debug("Scheduled task created... Data older than {0} days will be checked every hour and deleted.".format(
			condition_time))
