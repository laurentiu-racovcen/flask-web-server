from app import webserver
from flask import request, jsonify
import json

def process_post_request(function_name, data):
    print(f"Got POST request: {data}")

    if server_is_shutting_down():
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
    except Exception as e:
        print(f"Exception occured while checking putting the post request job in queue: {e}")
        return jsonify(
                {
                    'status': 'error: exception while putting the post request job in queue'
                }
            )

def get_job_result(job_id):
    if webserver.tasks_runner.jobs[job_id] == "finished":
        with open("./results/" + "out-" + str(job_id) + ".json") as job_data:
            return jsonify(
                {
                    'status': 'done',
                    'data': json.load(job_data)
                }
            )
    elif webserver.tasks_runner.jobs[job_id] == "running":
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
    print(f"\nget_results function: received job_id = {job_id}")

    # TODO: do input validation

    # convert job_id from string to int
    job_id = int(job_id)

    try:
        job_result = {}
        # check the requested job state
        with webserver.tasks_runner.jobs_lock:
            print("current job list:")
            print(webserver.tasks_runner.jobs)
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
    except Exception as e:
        print(f"Exception occured while checking the job status: {e}")
        return jsonify(
                {
                    'status': 'error: exception while checking the job status'
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

# TODO
@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    with webserver.tasks_runner.shutting_down_lock:
        if not webserver.tasks_runner.is_shutting_down:
            print(f"setting 'is_shutting_down' to True...")
            webserver.tasks_runner.is_shutting_down = True

    # the queue is not empty
    if not webserver.tasks_runner.jobs_queue.empty():
        return jsonify(
            {
                "status": "running",
            }
        )

    # TODO: perform graceful shutdown of the server
    # os._exit(0)

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
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

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
def append_job(data, function_name):
    # append api endpoint name as json parameter
    param = {"endpoint": function_name}
    data.update(param)

    with webserver.tasks_runner.jobs_lock:
        # increment the job counter
        webserver.tasks_runner.job_counter += 1

        job_id = webserver.tasks_runner.job_counter

        # append job id as json parameter
        data.update({"job_id": job_id})

        # add current job in jobs dictionary and set it as "running"
        webserver.tasks_runner.jobs[job_id] = "running"

        # put the job in queue
        webserver.tasks_runner.jobs_queue.put(data)

        # return the corresponding job_id of the job
        return job_id

# check if the server is shutting down
def server_is_shutting_down():
    server_is_shutting_down = False
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            server_is_shutting_down = True
    return server_is_shutting_down
