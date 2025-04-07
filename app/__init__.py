import os
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
import logging
from logging.handlers import RotatingFileHandler
import time

if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)

# logger file configuration
class GMTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        current_time = time.gmtime(record.created)
        if datefmt:
            return time.strftime(datefmt, current_time)
        else:
            return time.strftime("%Y-%m-%dT%H:%M:%S", current_time)

rot_handler = RotatingFileHandler('webserver.log', maxBytes=1000000, encoding="utf-8", backupCount=10)

rot_handler.setFormatter(GMTFormatter(
    fmt="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt='%Y-%m-%dT%H:%M:%S'))

webserver.logger = logging.getLogger("webserver")
webserver.logger.setLevel(logging.INFO)
webserver.logger.addHandler(rot_handler)

from app import routes

webserver.tasks_runner = ThreadPool()
webserver.tasks_runner.start()
webserver.job_counter = 1
