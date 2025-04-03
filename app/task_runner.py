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

    def get_questions_entries(self, question):
        # set the index as "Question"
        all_entries = self.thread_pool.data_ingestor.csv_file.set_index('Question')
        return all_entries.loc[question]

    def get_question_states(self, question):
        question_entries = self.get_questions_entries(question)

        # extract all unique states names from "LocationDesc" column
        return list(set(question_entries["LocationDesc"]))

    def state_mean(self, question, state):
        question_entries = self.get_questions_entries(question)

        # set the index as "LocationDesc"
        results_by_state = question_entries.set_index('LocationDesc')

        # extract all "Data_Value" column values corresponding to the state
        state_values = results_by_state.loc[state, "Data_Value"].tolist()

        result_dict = {
            state: sum(state_values) / float(len(state_values))
        }

        return result_dict

    def states_mean(self, question):
        states = self.get_question_states(question)
        results_dict = {}

        for state in states:
            state_result = self.state_mean(question, state)
            results_dict.update(state_result)

        # sort the states results in ascending order
        sorted_results_dict = dict(sorted(results_dict.items(), key = lambda item : item[1]))
        return sorted_results_dict
    
    def write_output_file(self, res, job_id):
        # store the result in the "./results" directory
        with open("./results/" + "out-" + str(job_id) + ".json", "w") as output_file:
            json.dump(res, output_file)
    
    def mark_job_as_finished(self, job_id):
        with self.thread_pool.jobs_lock:
            self.thread_pool.jobs[job_id] = "finished"

    def best5(self, question):
        states_mean = self.states_mean(question)

        if question in self.thread_pool.data_ingestor.questions_best_is_min:
            # the results are already sorted ascendingly, return the first 5 entries
            return dict(list(states_mean.items())[:5])
        elif question in self.thread_pool.data_ingestor.questions_best_is_max:
            last5_states = dict(list(states_mean.items())[-5:])

            # sort the last 5 states descendingly by mean and return them
            return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))
        else:
            raise Exception("best5 function has received an invalid question.")

    def worst5(self, question):
        states_mean = self.states_mean(question)

        if question in self.thread_pool.data_ingestor.questions_best_is_min:
            last5_states = dict(list(states_mean.items())[-5:])

            # sort the last 5 states descendingly by mean and return them
            return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))

        elif question in self.thread_pool.data_ingestor.questions_best_is_max:
            # the results are already sorted ascendingly, return the first 5 entries
            return dict(list(states_mean.items())[:5])
        else:
            raise Exception("worst5 function has received an invalid question.")

    def global_mean(self, question):
        question_entries = self.get_questions_entries(question)

        # extract all "Data_Value" column values
        question_values = list(question_entries["Data_Value"])

        result_dict = {
            "global_mean": sum(question_values) / float(len(question_values))
        }

        return result_dict

    def state_diff_from_mean(self, question, state):
        global_mean = self.global_mean(question)["global_mean"]
        state_mean = self.state_mean(question, state)[state]

        return {state: global_mean - state_mean}

    def diff_from_mean(self, question):
        states_mean = self.states_mean(question)

        result_dict = {}

        for state in states_mean.keys():
            state_diff_result = self.state_diff_from_mean(question, state)
            result_dict.update(state_diff_result)
           

        return result_dict

    def execute_job(self, job):
        print("thread id = " + str(self.thread_id) + ", is executing the job: " + str(job))
        endpoint = job['endpoint']
        job_id = job['job_id']

        # remove the endpoint field, it's no longer useful
        del job['endpoint']

        print("endpoint is: " + endpoint)
        print("remaining data is: " + str(job))

        if endpoint == "state_mean":
            # job_data contains the following fields: question, state, job_id
            result = self.state_mean(job['question'], job['state'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "states_mean":
            result = self.states_mean(job['question'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "best5":
            result = self.best5(job['question'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "worst5":
            result = self.worst5(job['question'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "global_mean":
            result = self.global_mean(job['question'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "state_diff_from_mean":
            result = self.state_diff_from_mean(job['question'], job['state'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
        elif endpoint == "diff_from_mean":
            result = self.diff_from_mean(job['question'])
            self.write_output_file(result, job_id)
            self.mark_job_as_finished(job_id)
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
                    print("There was an error executing the job with id = " + str(job['job_id']))

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
