from app import webserver
from flask import request, jsonify

import os
import json

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"\nget_results function: received job_id = {job_id}")

    # TODO: Check if job_id is valid

    # convert job_id string to int
    job_id = int(job_id)

    try:
        # check the requested job state
        with webserver.tasks_runner.jobs_lock:
            print("current job list:")
            print(webserver.tasks_runner.jobs)
            if job_id in webserver.tasks_runner.jobs:
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
            else:
                return jsonify(
                    {
                        'status': 'error',
                        'reason': 'Invalid job_id'
                    }
                )
    except:
        return jsonify(
                {
                    'status': 'error: exception while checking the job state'
                }
            )

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got POST request {data}")

    # check if the server is shutting down
    server_is_shutting_down = False
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            server_is_shutting_down = True
    
    if server_is_shutting_down:
        return jsonify(
                {
                    'status': 'error',
                    'reason': 'shutting down'
                }
            )

    print(f"received the POST data: {data}")

    # append api endpoint name as json parameter
    data.update({"endpoint": "states_mean"})

    # for storing the job_id of the json result
    job_id = 0

    try:
        with webserver.tasks_runner.jobs_lock:
            # increment the job counter
            webserver.tasks_runner.job_counter += 1

            job_id = webserver.tasks_runner.job_counter

            # append job id as json parameter
            data.update({"job_id": webserver.tasks_runner.job_counter})

            # add current job in jobs dictionary and set it as "running"
            webserver.tasks_runner.jobs[job_id] = "running"

            # put the job in queue
            webserver.tasks_runner.jobs_queue.put(data)

        return jsonify(
                {
                    'status': 'success',
                    'job_id': job_id
                }
            )
    except:
        return jsonify(
                {
                    'status': 'error: exception while putting the job in queue',
                }
            )

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # Get request data
    data = request.json
    print(f"Got POST request {data}")

    # check if the server is shutting down
    server_is_shutting_down = False
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            server_is_shutting_down = True
    
    if server_is_shutting_down:
        return jsonify(
                {
                    'status': 'error',
                    'reason': 'shutting down'
                }
            )

    print(f"(received the POST data: {data}")

    # append api endpoint name as json parameter
    param = {"endpoint": "state_mean"}
    data.update(param)

    # for storing the job_id of the json result
    job_id = 0

    try:
        with webserver.tasks_runner.jobs_lock:
            # increment the job counter
            webserver.tasks_runner.job_counter += 1

            job_id = webserver.tasks_runner.job_counter

            # append job id as json parameter
            data.update({"job_id": job_id})

            # add current job in jobs dictionary and set it as "running"
            webserver.tasks_runner.jobs[webserver.tasks_runner.job_counter] = "running"

            # put the job in queue
            webserver.tasks_runner.jobs_queue.put(data)

        return jsonify(
                {
                    'status': 'success',
                    'job_id': job_id
                }
            )
    except:
        return jsonify(
                {
                    'status': 'error: exception while putting the job in queue',
                }
            )

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO check:
    # if the threadpool variable "is_shutting_down" is True, send "shutting down" status message
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            return jsonify(
                {
                    "status": "error",
                    "reason": "shutting down"
                }
            )

    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id



    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/diff_from_mean', methods=['POST'])   
def diff_from_mean_request():
    # TODO check:
    # if the threadpool variable "is_shutting_down" is True, send "shutting down" status message
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            return jsonify(
                {
                    "status": "error",
                    "reason": "shutting down"
                }
            )

    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO check:
    # if the threadpool variable "is_shutting_down" is True, send "shutting down" status message
    with webserver.tasks_runner.shutting_down_lock:
        if webserver.tasks_runner.is_shutting_down:
            return jsonify(
                {
                    "status": "error",
                    "reason": "shutting down"
                }
            )

    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

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

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
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
