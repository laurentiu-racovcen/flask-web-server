from queue import Queue
from app.data_ingestor import DataIngestor
from threading import Thread, Event, Lock
import time
import os
import multiprocessing
import json

class ThreadPool:
    def __init__(self):
        self.jobs_queue = Queue()
        self.num_threads = 0
        self.threads = []
        self.shutting_down_lock = Lock()
        self.is_shutting_down = False
        self.job_counter = 0
        self.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
        self.jobs = {}
        self.jobs_lock = Lock() # protects "job_counter" and "jobs" fields

        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
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

    def state_mean(self, job_data):
        # job_data contains the following fields: question, state, job_id
        question = job_data['question']
        state = job_data['state']
        job_id = job_data['job_id']

        # set the index as "Question"
        q_all = self.thread_pool.data_ingestor.csv_file.set_index('Question')
        q_only = q_all.loc[question]

        # set the index as "LocationDesc"
        results_by_state = q_only.set_index('LocationDesc')

        # extract "Data_Value" column values
        results_by_state = results_by_state.loc[state, "Data_Value"]

        print(results_by_state)
        q_values_list = results_by_state.tolist()

        print(q_values_list)

        result_dict = {
            state: sum(q_values_list) / float(len(q_values_list))
        }

        # store the result in the "./results" directory
        with open("./results/" + "out-" + str(job_id) + ".json", "w") as output_file:
            json.dump(result_dict, output_file)
        
        # mark job as finished
        with self.thread_pool.jobs_lock:
            self.thread_pool.jobs[job_id] = "finished"

    def states_mean(self, job_data):
        # job_data contains the following fields: question, job_id
        question = job_data['question']
        job_id = job_data['job_id']

        # set the index as "Question"
        all_entries = self.thread_pool.data_ingestor.csv_file.set_index('Question')
        question_entries = all_entries.loc[question]

        # extract all states names from "LocationDesc" column, without duplicates
        states = list(set(question_entries["LocationDesc"]))

        # set the index as "LocationDesc"
        question_entries = question_entries.set_index('LocationDesc')

        results_dict = {}

        for state in states:
            # extract "Data_Value" column values
            state_entries = question_entries.loc[state, "Data_Value"]

            state_entries_values = state_entries.tolist()

            print("state = " + state )
            print(state_entries_values)

            results_dict[state] = sum(state_entries_values) / float(len(state_entries_values))

        # sort the states results in ascending order
        sorted_results_dict = dict(sorted(results_dict.items(), key = lambda item : item[1]))

        # store the result in the "./results" directory
        with open("./results/" + "out-" + str(job_id) + ".json", "w") as output_file:
            json.dump(sorted_results_dict, output_file)
        
        # mark job as finished
        with self.thread_pool.jobs_lock:
            self.thread_pool.jobs[job_id] = "finished"

    def execute_job(self, job):
        print("thread id = " + str(self.thread_id) + ", is executing the job: " + str(job))
        endpoint = job['endpoint']

        # remove the endpoint field, it's no longer in use
        del job['endpoint']

        print("endpoint is: " + endpoint)
        print("remaining data is: " + str(job))

        # append job id as json parameter
        param = {"job_id": self.thread_pool.job_counter}
        job.update(param)

        if endpoint == "states_mean":
            self.states_mean(job)
        elif endpoint == "state_mean":
            self.state_mean(job)
        # etc.

    def run(self):
        print("thread id = " + str(self.thread_id) + " has been initialized!")
        print(self.thread_pool.jobs_queue)
        while True:
            # Get pending job
            if not self.thread_pool.jobs_queue.empty():
                job = self.thread_pool.jobs_queue.get()
                # Execute the job and save the result to disk
                try:
                    self.execute_job(job)
                except:
                    print("There was an error executing the job.")

            # Repeat until graceful_shutdown
            # TODO
            else:
                with self.thread_pool.shutting_down_lock:
                    if self.thread_pool.is_shutting_down:
                        # graceful shutdown has been set
                        # and the jobs queue is empty
                        # finish thread execution
                        print("thread id = " + str(self.thread_id) + ", finished the execution.)")
                        break
