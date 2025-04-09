import json
from flask import request, jsonify
from app import webserver
from .utils import JobStatus

def process_post_request(function_name: str, data: dict):
    '''
    Process a POST request
    '''
    webserver.logger.info("\"POST /api/%s\" - \"Received data: %s\"", function_name, data)

    if webserver.tasks_runner.shutdown_event.is_set():
        response = {
            'status': 'error',
            'reason': 'shutting down'
        }
        webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)
        return jsonify(response)

    # append the job to the queue
    try:
        job_id = append_job(data, function_name)
        response = {
            'status': 'success',
            'job_id': job_id
        }
        webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)
        return jsonify(response)

    except KeyError as e:
        webserver.logger.exception("Exception occured while appending the post request job to the queue: %s", e)
        response = {
            'status': 'error: exception while putting the post request job in queue'
        }
        webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)
        return jsonify(response)

    except TypeError as e:
        webserver.logger.exception("Exception occured while appending the post request job to the queue: %s", e)
        response = {
            'status': 'error: exception while putting the post request job in queue'
        }
        webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)
        return jsonify(response)

def get_job_result(job_id):
    '''
    Returns the job result of the corresponding job id
    '''
    if webserver.tasks_runner.jobs[job_id] == JobStatus.DONE:
        file_name = "./results/" + "out-" + str(job_id) + ".json"
        with open(file_name, "r", encoding="utf-8") as job_data:
            return {
                'status': 'done',
                'data': json.load(job_data)
            }
    elif webserver.tasks_runner.jobs[job_id] == JobStatus.RUNNING:
        return {
            'status': 'running'
        }
    else:
        return {
            'status': 'error',
            'reason': 'unrecognized job state'
        }

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    '''
    Returns the response of a request
    '''
    webserver.logger.info("\"GET /api/get_results/%s\"", job_id)

    if not job_id.isnumeric():
        webserver.logger.info("\"GET /api/get_results/%s\" -- The job id is not numeric.\"", job_id)
        response = {
            'status': 'error',
            'reason': 'the job id is not numeric'
        }
        webserver.logger.info("\"GET /api/get_results/%s\" - \"Responding with: %s\"", job_id, response)
        return jsonify(response)

    # convert job_id from string to int
    job_id = int(job_id)

    try:
        job_result = {}
        # check the requested job state
        with webserver.tasks_runner.jobs_lock:
            if job_id in webserver.tasks_runner.jobs:
                job_result = get_job_result(job_id)
            else:
                job_result = {
                    'status': 'error',
                    'reason': 'invalid job id'
                }

        webserver.logger.info("\"GET /api/get_results/%d\" - \"Responding with: %s\"", job_id, job_result)
        return jsonify(job_result)

    except KeyError as e:
        webserver.logger.info("Exception occured while checking the job status: %s", e)
        response = {
            'status': 'error: exception while checking the job status'
        }
        webserver.logger.info("\"GET /api/get_results/%d\" - \"Responding with: %s\"", job_id, response)
        return jsonify(response)

    except TypeError as e:
        webserver.logger.info("Exception occured while checking the job status: %s", e)
        response = {
            'status': 'error: exception while checking the job status'
        }
        webserver.logger.info("\"GET /api/get_results/%d\" - \"Responding with: %s\"", job_id, response)
        return jsonify(response)

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs_status():
    '''
    Returns the status of all jobs
    '''
    webserver.logger.info("\"GET /api/jobs\"")
    jobs = {}
    with webserver.tasks_runner.jobs_lock:
        jobs = webserver.tasks_runner.jobs

    result = {}
    # convert enum values into strings
    for key, value in jobs.items():
        if value == JobStatus.RUNNING:
            result[key] = "running"
        elif value == JobStatus.DONE:
            result[key] = "done"

    response = {
        'status': "done",
        "data": result
    }

    webserver.logger.info("\"GET /api/jobs\" - \"Responding with: %s\"", response)
    return jsonify(response)

@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    '''
    Returns the number of running jobs
    '''
    webserver.logger.info("\"GET /api/num_jobs\"")
    done_jobs_counter = 0

    with webserver.tasks_runner.done_jobs_counter_lock:
        done_jobs_counter = webserver.tasks_runner.done_jobs_counter

    response = {
            'status': "done",
            "data": webserver.tasks_runner.job_counter - done_jobs_counter
        }

    webserver.logger.info("\"GET /api/jobs\" - \"Responding with: %s\"", response)
    return jsonify(response)

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    '''
    Processes "states_mean" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("states_mean")

    return process_post_request("states_mean", request.json)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    '''
    Processes "state_mean" API requests
    '''

    if (not "question" in request.json) or (not "state" in request.json):
        return missing_question_or_state_message("state_mean")

    return process_post_request("state_mean", request.json)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    '''
    Processes "best5" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("best5")

    return process_post_request("best5", request.json)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    '''
    Processes "worst5" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("worst5")

    return process_post_request("worst5", request.json)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    '''
    Processes "global_mean" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("global_mean")

    return process_post_request("global_mean", request.json)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    '''
    Processes "diff_from_mean" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("diff_from_mean")

    return process_post_request("diff_from_mean", request.json)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    '''
    Processes "state_diff_from_mean" API requests
    '''

    if (not "question" in request.json) or (not "state" in request.json):
        return missing_question_or_state_message("state_diff_from_mean")

    return process_post_request("state_diff_from_mean", request.json)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    '''
    Processes "mean_by_category" API requests
    '''

    if not "question" in request.json:
        return missing_question_message("mean_by_category")

    return process_post_request("mean_by_category", request.json)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    '''
    Processes "state_mean_by_category" API requests
    '''

    if (not "question" in request.json) or (not "state" in request.json):
        return missing_question_or_state_message("state_mean_by_category")

    return process_post_request("state_mean_by_category", request.json)

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    '''
    Processes "graceful_shutdown" API requests
    '''
    webserver.logger.info("\"GET /api/graceful_shutdown\"")
    webserver.tasks_runner.shutdown_event.set()

    # the queue is not empty
    if not webserver.tasks_runner.jobs_queue.empty():
        response = {
            "status": "running",
        }

        webserver.logger.info("\"GET /graceful_shutdown\" - \"Responding with: %s\"", response)
        return jsonify(response)

    webserver.logger.info("The server has been shut down successfully.")

    # the queue is empty
    response = {
        "status": "done",
    }

    webserver.logger.info("\"GET /graceful_shutdown\" - \"Responding with: %s\"", response)
    return jsonify(response)

@webserver.route('/')
@webserver.route('/index')
def index():
    '''
    Processes "/" and "/index" API requests
    '''
    webserver.logger.info("\"GET /index\"")
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    '''
    Returns all defined routes
    '''
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes

def append_job(data: dict, function_name: str) -> int:
    '''
    Appends a job to the queue
    '''
    # append api endpoint name as json parameter
    param = {"endpoint": function_name}
    data.update(param)

    # increment the job counter
    webserver.tasks_runner.job_counter += 1

    # get current job id
    job_id = webserver.tasks_runner.job_counter

    # append the job id as dictionary parameter
    data.update({"job_id": job_id})

    with webserver.tasks_runner.jobs_lock:
        # add current job in jobs dictionary and set it as "running"
        webserver.tasks_runner.jobs[job_id] = JobStatus.RUNNING

    # append the job to the queue
    webserver.tasks_runner.jobs_queue.put(data)

    # return the corresponding job_id of the job
    return job_id

def missing_question_message(function_name):
    webserver.logger.info("\"POST /api/%s\" - \"The json does not contain the question field. Received json: %s\"", function_name, request.json)
    response = {
        'status': 'error',
        'reason': 'the json does not contain the question field'
    }
    webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)

    return jsonify(response)

def missing_question_or_state_message(function_name):
    webserver.logger.info("\"POST /api/%s\" - \"The json does not contain all the required fields. Received json: %s\"", function_name, request.json)
    response = {
        'status': 'error',
        'reason': 'the json does not contain all the required fields'
    }
    webserver.logger.info("\"POST /api/%s\" - \"Responding with: %s\"", function_name, response)

    return jsonify(response)
