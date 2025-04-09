import queue
from queue import Queue
from threading import Thread, Event, Lock
import os
import sys
import multiprocessing
import json
from app.data_ingestor import DataIngestor
import app.thread_utils
from app.thread_utils import ThreadUtils
from .utils import JobStatus

class ThreadPool:
    def __init__(self):
        self.jobs_queue = Queue()
        self.num_threads = 0
        self.threads = []
        self.shutdown_event = Event()
        self.job_counter = 0
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
        self.jobs = {}
        self.jobs_lock = Lock() # protects "jobs" dictionary
        self.done_jobs_counter = 0
        self.done_jobs_counter_lock = Lock() # protects "done_jobs_counter"

        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker
        if os.environ.get('TP_NUM_OF_THREADS'):
            # the variable is defined
            self.num_threads = os.environ.get('TP_NUM_OF_THREADS')
        else:
            # set the "num_threads" field to hardware threads number
            self.num_threads = multiprocessing.cpu_count()

    def start(self):
        # create the TaskRunners threads
        for i in range(self.num_threads):
            current_thread = TaskRunner(i, self)
            current_thread.start()
            self.threads.append(current_thread)

    def join(self):
        # wait for the threads to receive the "graceful_shutdown" command
        for current_thread in self.threads:
            current_thread.join()

class TaskRunner(Thread):
    def __init__(self, thread_id, thread_pool):
        # init necessary data structures
        Thread.__init__(self)
        self.thread_id = thread_id
        self.thread_pool = thread_pool

    def write_job_output_file(self, res, job_id):
        # store the result in the "./results" directory
        file_name = "./results/" + "out-" + str(job_id) + ".json"
        with open(file_name, "w", encoding="utf-8") as output_file:
            json.dump(res, output_file)

    def mark_job_as_done(self, job_id):
        with self.thread_pool.jobs_lock:
            self.thread_pool.jobs[job_id] = JobStatus.DONE

    def get_job_output(self, function, job) -> dict:
        job_id = job['job_id']

        result = {}
        if (job['endpoint'] == "best5") or (job['endpoint'] == "worst5"):
            result = function(job['question'],
                              self.thread_pool.data_ingestor.get_questions_best_is_min(),
                              self.thread_pool.data_ingestor.get_questions_best_is_max(),
                              self.thread_pool.data_ingestor.get_csv_file())

        elif ('question' in job) and ('state' in job):
            result = function(job['question'], job['state'], self.thread_pool.data_ingestor.get_csv_file())

        elif 'question' in job:
            result = function(job['question'], self.thread_pool.data_ingestor.get_csv_file())

        self.write_job_output_file(result, job_id)
        self.mark_job_as_done(job_id)

        with self.thread_pool.done_jobs_counter_lock:
            self.thread_pool.done_jobs_counter += 1

        return result

    def execute_job(self, job):
        endpoint = job['endpoint']

        if endpoint in ThreadUtils.endpoint_func_map:
            return self.get_job_output(ThreadUtils.endpoint_func_map[endpoint], job)

        return {
            "status": "error",
            "reason": "No such endpoint"
        }

    def run(self):
        while True:
            if not self.thread_pool.shutdown_event.is_set():
                if not self.thread_pool.jobs_queue.empty():
                    # get a pending job from queue
                    try:
                        job = self.thread_pool.jobs_queue.get_nowait()
                    except queue.Empty:
                        print("Queue is empty.")

                    # execute the job and save the result to disk
                    try:
                        self.execute_job(job)
                    except KeyError as e:
                        print(f"There was an error executing the job with id = {job['job_id']}: {e}")
                    except TypeError as e:
                        print(f"There was an error executing the job with id = {job['job_id']}: {e}")
            else:
                if self.thread_pool.jobs_queue.empty():
                    # "shutdown_event" has been set and the jobs queue is empty,
                    # exit the while loop
                    break

        # finish thread execution
        sys.exit()
