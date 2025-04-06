import json
from flask import request, jsonify
from app import webserver
from .utils import JobStatus

def process_post_request(function_name: str, data: dict):
    # print(f"Got POST request: {data}")

    if webserver.tasks_runner.shutdown_event.is_set():
        return jsonify(
                {
                    'status': 'error',
                    'reason': 'shutting down'
                }
            )

    # append the job to the queue
    try:
        job_id = append_job(data, function_name)
        return jsonify(
                {
                    'status': 'success',
                    'job_id': job_id
                }
            )
    except KeyError as e:
        print(f"Exception occured while checking putting the post request job in queue: {e}")
        return jsonify(
                {
                    'status': 'error: exception while putting the post request job in queue'
                }
            )
    except TypeError as e:
        print(f"Exception occured while checking putting the post request job in queue: {e}")
        return jsonify(
                {
                    'status': 'error: exception while putting the post request job in queue'
                }
            )

def get_job_result(job_id):
    if webserver.tasks_runner.jobs[job_id] == JobStatus.DONE:
        file_name = "./results/" + "out-" + str(job_id) + ".json"
        with open(file_name, "r", encoding="utf-8") as job_data:
            return jsonify(
                {
                    'status': 'done',
                    'data': json.load(job_data)
                }
            )
    elif webserver.tasks_runner.jobs[job_id] == JobStatus.RUNNING:
        return jsonify(
            {
                'status': 'running'
            }
        )
    else:
        return jsonify(
            {
                'status': 'error',
                'reason': 'unrecognized job state'
            }
        )

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    # print(f"\nget_results function: received job_id = {job_id}")

    # TODO: do input validation

    # convert job_id from string to int
    job_id = int(job_id)

    try:
        job_result = {}
        # check the requested job state
        with webserver.tasks_runner.jobs_lock:
            # print("current job list:")
            # print(webserver.tasks_runner.jobs)
            if job_id in webserver.tasks_runner.jobs:
                job_result = get_job_result(job_id)
            else:
                job_result = jsonify(
                    {
                        'status': 'error',
                        'reason': 'invalid job_id'
                    }
                )
        return job_result
    except KeyError as e:
        print(f"Exception occured while checking the job status: {e}")
        return jsonify(
                {
                    'status': 'error: exception while checking the job status'
                }
            )
    except TypeError as e:
        print(f"Exception occured while checking the job status: {e}")
        return jsonify(
            {
                'status': 'error: exception while checking the job status'
            }
        )

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs_status():
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

    return jsonify(
        {
            'status': "done",
            "data": result
        }
    )

@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    done_jobs_counter = 0
    with webserver.tasks_runner.done_jobs_counter_lock:
        done_jobs_counter = webserver.tasks_runner.done_jobs_counter

    return jsonify(
        {
            'status': "done",
            "data": webserver.tasks_runner.job_counter - done_jobs_counter
        }
    )

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    return process_post_request("states_mean", request.json)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    return process_post_request("state_mean", request.json)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    return process_post_request("best5", request.json)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    return process_post_request("worst5", request.json)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    return process_post_request("global_mean", request.json)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    return process_post_request("diff_from_mean", request.json)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    return process_post_request("state_diff_from_mean", request.json)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    return process_post_request("mean_by_category", request.json)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    return process_post_request("state_mean_by_category", request.json)

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    print("setting 'is_shutting_down' to True...")
    webserver.tasks_runner.shutdown_event.set()

    # the queue is not empty
    if not webserver.tasks_runner.jobs_queue.empty():
        return jsonify(
            {
                "status": "running",
            }
        )

    print("The server has been shut down successfully.")

    # the queue is empty
    return jsonify(
        {
            "status": "done",
        }
    )

@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes

# append a job to the queue
def append_job(data: dict, function_name: str) -> int:
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
