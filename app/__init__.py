import os
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)

from app import routes

webserver.tasks_runner = ThreadPool()
webserver.tasks_runner.start()
webserver.job_counter = 1
