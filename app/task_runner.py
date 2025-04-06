import queue
from queue import Queue
from threading import Thread, Event, Lock
import os
import sys
import multiprocessing
import json
from app.data_ingestor import DataIngestor
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

    def get_question_states(self, question, entries):
        question_entries = entries[entries["Question"] == question]

        # return all unique states names from "LocationDesc" column
        return list(set(question_entries["LocationDesc"]))

    def state_mean(self, question, state):
        all_entries = self.thread_pool.data_ingestor.get_csv_file()
        question_entries = all_entries[all_entries["Question"] == question]
        question_state_entries = question_entries[question_entries["LocationDesc"] == state]

        # extract all "Data_Value" column values
        state_values = question_state_entries["Data_Value"]

        result_dict = {
            state: sum(state_values) / float(len(state_values))
        }

        return result_dict

    def states_mean(self, question):
        states = self.get_question_states(question, self.thread_pool.data_ingestor.get_csv_file())
        results_dict = {}

        for state in states:
            state_result = self.state_mean(question, state)
            results_dict.update(state_result)

        # return sorted results in ascending oreder by values
        return dict(sorted(results_dict.items(), key = lambda item : item[1]))

    def write_job_output_file(self, res, job_id):
        # store the result in the "./results" directory
        file_name = "./results/" + "out-" + str(job_id) + ".json"
        with open(file_name, "w", encoding="utf-8") as output_file:
            json.dump(res, output_file)

    def mark_job_as_done(self, job_id):
        with self.thread_pool.jobs_lock:
            self.thread_pool.jobs[job_id] = JobStatus.DONE
            print(f"\nthread id = {self.thread_id}, finished job {job_id}")

    def best5(self, question):
        states_mean = self.states_mean(question)

        if question in self.thread_pool.data_ingestor.get_questions_best_is_min():
            # the results are already sorted ascendingly, return the first 5 entries
            return dict(list(states_mean.items())[:5])

        if question in self.thread_pool.data_ingestor.get_questions_best_is_max():
            last5_states = dict(list(states_mean.items())[-5:])

            # sort the last 5 states descendingly by mean and return them
            return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))

        # the given question is not one of the available questions
        return {}

    def worst5(self, question):
        states_mean = self.states_mean(question)

        if question in self.thread_pool.data_ingestor.get_questions_best_is_min():
            last5_states = dict(list(states_mean.items())[-5:])

            # sort the last 5 states descendingly by mean and return them
            return dict(sorted(last5_states.items(), key = lambda item : item[1], reverse=True))

        if question in self.thread_pool.data_ingestor.get_questions_best_is_max():
            # the results are already sorted ascendingly, return the first 5 entries
            return dict(list(states_mean.items())[:5])

        # the given question is not one of the available questions
        return {}

    def global_mean(self, question):
        all_entries = self.thread_pool.data_ingestor.get_csv_file()
        question_entries = all_entries[all_entries["Question"] == question]

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

    def get_stratification_category_entries(self, category, entries):
        return entries[entries["StratificationCategory1"] == category]

    def get_stratification_entries(self, stratification, entries):
        return entries[entries["Stratification1"] == stratification]

    def get_categories_names(self, entries):
        if (not entries.empty) and ("StratificationCategory1" in entries.columns):
            return entries["StratificationCategory1"].dropna().unique().tolist()

        # there are no entries or is no "StratificationCategory1" column
        return []

    def get_stratifications_names(self, entries):
        if (not entries.empty) and ("Stratification1" in entries.columns):
            return entries["Stratification1"].dropna().unique().tolist()

        # there are no entries or is no "Stratification1" column
        return []

    def get_stratification_mean(self, stratification, entries):
        str_entries = self.get_stratification_entries(stratification, entries)

        # extract all "Data_Value" column values
        data_values = list(str_entries["Data_Value"])

        # compute the mean of data values
        str_mean = sum(data_values) / float(len(data_values))

        return {stratification: str_mean}

    def state_mean_by_category(self, question, state):
        all_entries = self.thread_pool.data_ingestor.get_csv_file()
        question_entries = all_entries[all_entries["Question"] == question]
        question_state_entries = question_entries[question_entries["LocationDesc"] == state]

        # extract all unique "StratificationCategory1" column values corresponding to the state
        categories = self.get_categories_names(question_state_entries)

        strat_cat_results = {}
        for strat_cat in categories:
            category_entries = self.get_stratification_category_entries(strat_cat,
                                                                        question_state_entries)

            # extract all unique "Stratification1" column values of the current category
            stratifications = self.get_stratifications_names(category_entries)

            for strat in stratifications:
                strat_mean_result = self.get_stratification_mean(strat, category_entries)
                strat_cat_results.update(
                    { "('" + strat_cat + "', '" + strat + "')": strat_mean_result[strat]
                    }
                )

        # sort the results ascendingly by (category, stratification) key
        strat_cat_results = dict(sorted(strat_cat_results.items()))

        return {state: strat_cat_results}

    def mean_by_category(self, question):
        states = self.get_question_states(question, self.thread_pool.data_ingestor.get_csv_file())
        results_dict = {}

        for state in states:
            results = self.state_mean_by_category(question, state)

            # extract all the state results dictionary entries
            state_results = results[state]

            # extract the keys (category + stratification) of all the dictionary entries
            keys = state_results.keys()

            for key in keys:
                value = state_results[key]
                new_key = "('" + state + "', " + key[1:]
                results_dict.update({new_key: value})

        # return sorted results in ascending order by state name
        return dict(sorted(results_dict.items()))

    def get_job_output(self, function, job) -> dict:
        job_id = job['job_id']

        result = {}
        if ('question' in job) and ('state' in job):
            result = function(job['question'], job['state'])
        elif 'question' in job:
            result = function(job['question'])

        self.write_job_output_file(result, job_id)
        self.mark_job_as_done(job_id)

        with self.thread_pool.done_jobs_counter_lock:
            self.thread_pool.done_jobs_counter += 1

        return result

    def execute_job(self, job):
        print("thread id = " + str(self.thread_id) + ", started executing the job: " + str(job))
        endpoint = job['endpoint']

        # remove the endpoint field from job dictionary, it's no longer useful
        del job['endpoint']

        endpoint_func_map = {
            "state_mean": self.state_mean,
            "states_mean": self.states_mean,
            "best5": self.best5,
            "worst5": self.worst5,
            "global_mean": self.global_mean,
            "state_diff_from_mean": self.state_diff_from_mean,
            "diff_from_mean": self.diff_from_mean,
            "state_mean_by_category": self.state_mean_by_category,
            "mean_by_category": self.mean_by_category
        }

        if endpoint in endpoint_func_map:
            return self.get_job_output(endpoint_func_map[endpoint], job)

        return {
            "status": "error",
            "reason": "No such endpoint"
        }

    def run(self):
        print("thread id = " + str(self.thread_id) + " has been initialized!")
        print(self.thread_pool.jobs_queue)
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
                    print("thread id = " + str(self.thread_id) + ", finished the execution.")
                    break

        # finish thread execution
        sys.exit()
