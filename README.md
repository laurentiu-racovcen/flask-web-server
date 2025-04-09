Name: Racovcen Laurențiu

Group: 334CD

# Homework 1

## Explanation of the chosen solution

* The chosen solution implements a Flask-based web server that exposes an API for performing various analyses on health-related data. The `__init__.py` file initializes the Flask application (`webserver`), configures logging, and sets up a thread pool (`ThreadPool`) from `app.task_runner.py` to handle incoming requests concurrently. The `DataIngestor` class in `data_ingestor.py` is responsible for reading and providing access to the data from a CSV file. API endpoints are defined in `routes.py`, which receive requests, enqueue jobs for the thread pool, and return results or job statuses. The actual data analysis logic is implemented in the functions within `thread_utils.py`. Each request triggers a job that is placed in the `ThreadPool`'s queue. Worker threads (`TaskRunner`) pick up these jobs, execute the corresponding analysis function, and store the results as JSON files in the `results` directory. The general approach is to separate the web server logic (handling requests and responses) from the data processing logic (data ingestion and analysis) using a thread pool for concurrency.

    ```python
    # Example from "__init__.py" showing Flask app and ThreadPool initialization
    from flask import Flask
    from app.task_runner import ThreadPool

    webserver = Flask(__name__)
    webserver.tasks_runner = ThreadPool()
    webserver.tasks_runner.start()
    ```

    ```python
    # Example from "data_ingestor.py" showing data loading
    import pandas

    class DataIngestor:
        def __init__(self, csv_path: str):
            self.__csv_file = pandas.read_csv(csv_path)
    ```

    ```python
    # Example from "routes.py" showing API endpoint definition
    from flask import request, jsonify
    from app import webserver

    @webserver.route('/api/states_mean', methods=['POST'])
    def states_mean_request():
        if not "question" in request.json:
            return missing_question_message("states_mean")
        return process_post_request("states_mean", request.json)
    ```

    ```python
    # Example from "task_runner.py" showing job execution
    class TaskRunner(Thread):
        # ...
        def execute_job(self, job):
            endpoint = job['endpoint']
            if endpoint in ThreadUtils.endpoint_func_map:
                return self.get_job_output(ThreadUtils.endpoint_func_map[endpoint], job)
            return {
                "status": "error",
                "reason": "No such endpoint"
            }
    ```

    ```python
    # Example from "thread_utils.py" showing a data analysis function
    def states_mean(question, entries):
        # ... logic to calculate the mean for each state ...
        return dict(sorted(results_dict.items(), key = lambda item : item[1]))
    ```

* **Do you find the homework useful?**

Yes, I've gotten hands-on practice with building a web server using Flask. Understanding how to design and implement the different API endpoints and connect them to the data analysis functions has really improved my understanding of backend development.

* **Do you find the implementation naive, effective, could have been better?**

I find the implementation reasonably effective for handling a moderate number of concurrent requests. The current implementation uses file-based storage for results, which is simple but might become inefficient for a large number of jobs. A more robust solution could involve using a database.

***Special case:***

* The implementation handles cases where required parameters (like 'question' or 'state') are missing in the request JSON, returning appropriate error messages.
* Exceptions handling.

Implementation
-

* **Specify whether the entire homework statement is implemented:**

Yes, the entire homework statement is implemented.

* **Interesting things discovered along the way:**

    - How quickly and easily web APIs can be built using the Flask framework in Python.
    - The role of logging in a server application.
    - The clear separation of concerns between the web server, task management, data ingestion, and data analysis logic. This modular design makes the codebase more organized and easier to maintain.

Resources used
-

https://ocw.cs.pub.ro/courses/asc/laboratoare/02

https://ocw.cs.pub.ro/courses/asc/laboratoare/03

https://flask.palletsprojects.com/en/stable/quickstart/#routing

https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

https://docs.python.org/3/library/unittest.htm

https://docs.python.org/3/library/logging.html

Git
-
1. Link to the repo: https://github.com/laurentiu-racovcen/ASC-HW1
