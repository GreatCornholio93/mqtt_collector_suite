import logging
import os


class Logger(object):
    logger = None
    
    def __init__(self):
        path = "logs/debug_log.log"
        log = logging.getLogger("MQTT_collector")
        log.setLevel(logging.DEBUG)
        lh = logging.FileHandler(path)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
        log.addHandler(ch)
        lh.setLevel(logging.DEBUG)
        lh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
        log.addHandler(lh)
        Logger.logger = log

    @staticmethod
    def get_logger():
        if not Logger.logger:
            Logger()
            return Logger.logger
        else:
            return Logger.logger