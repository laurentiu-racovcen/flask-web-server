# **Flask Web Server: Health Data Analysis API**

> This project implements a Flask-based web server that exposes REST API endpoints for analyzing health-related data. The server processes concurrent requests using a thread pool, executes data analysis tasks, and returns results. Users interact via HTTP requests to endpoints like `/api/states_mean`, `/api/state_mean`, etc.

## **Table of Contents**

1. [Overview](#overview)
2. [Key Components](#key-components)
3. [API Endpoints](#api-endpoints)
4. [Implementation Details](#implementation-details)
5. [Special Cases \& Error Handling](#special-cases--error-handling)
6. [Resources](#resources)

## **Overview**

The solution separates concerns into distinct layers:

- **Web Server**: Handles HTTP requests/responses (Flask)
- **Concurrency Management**: Thread pool for task processing (`task_runner.py`)
- **Data Ingestion**: CSV loading and access (`data_ingestor.py`)
- **Analysis Logic**: Statistical computations (`thread_utils.py`)

## **Key Components**

### **`__init__.py`**

Initializes the Flask application and thread pool:

```python
from flask import Flask
from app.task_runner import ThreadPool

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()
webserver.tasks_runner.start()
```


### **`data_ingestor.py`**

Loads and provides access to CSV data using Pandas:

```python
import pandas

class DataIngestor:
    def __init__(self, csv_path: str):
        self.__csv_file = pandas.read_csv(csv_path)
```


### **`routes.py`**

Defines API endpoints and request validation:

```python
@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    if not "question" in request.json:
        return missing_question_message("states_mean")
    return process_post_request("states_mean", request.json)
```


### **`task_runner.py`**

Manages job execution with worker threads:

```python
class TaskRunner(Thread):
    def execute_job(self, job):
        endpoint = job['endpoint']
        if endpoint in ThreadUtils.endpoint_func_map:
            return self.get_job_output(ThreadUtils.endpoint_func_map[endpoint], job)
        return {
            "status": "error",
            "reason": "No such endpoint"
        }
```


### **`thread_utils.py`**

Contains core analysis logic:

```python
def states_mean(question, entries):
        # logic to calculate the mean for each state ...
        return dict(sorted(results_dict.items(), key = lambda item : item[1]))
```


## **API Endpoints**

All endpoints accept `POST` requests with JSON payloads.


| Endpoint | Required Parameters | Description |
| :-- | :-- | :-- |
| `/api/states_mean` | `{"question": string}` | Mean results across all states |
| `/api/state_mean` | `{"question": string, "state": string}` | Mean for specific state |
| `/api/best5` | `{"question": string}` | Top 5 best-performing states |
| `/api/worst5` | `{"question": string}` | Bottom 5 worst-performing states |
| `/api/global_mean` | `{"question": string}` | Global mean across all data |
| `/api/diff_from_mean` | `{"question": string}` | Deviation from global mean |
| `/api/state_diff_from_mean` | `{"question": string, "state": string}` | State-specific deviation |
| `/api/mean_by_category` | `{"question": string}` | Mean grouped by category |
| `/api/state_mean_by_category` | `{"question": string, "state": string}` | State mean grouped by category |

## **Implementation Details**

### **Concurrency Model**

- Uses `ThreadPool` with configurable worker threads
- Jobs are queued and executed asynchronously
- Results stored as JSON files in `/results` directory


### **Data Flow**

1. Request received by Flask endpoint
2. Job enqueued in thread pool
3. Worker thread:
    - Fetches data via `DataIngestor`
    - Executes analysis function from `thread_utils.py`
4. Results serialized to JSON and returned

### **Validation**

- Input validation handled in `routes.py`
- Error responses for missing parameters (HTTP 400)
- Endpoint existence checking in `TaskRunner`


## **Special Cases & Error Handling**

| Scenario | Response |
| :-- | :-- |
| Missing `question` parameter | `{"error": "Missing 'question'"}, 400` |
| Invalid endpoint | `{"status": "error", "reason": "Invalid endpoint"}` |
| Data processing errors | Logged with traceback; HTTP 500 response |

**Exception Handling:**

- All analysis functions include `try/except` blocks
- Server logs errors with full stack traces for debugging


## **Resources**

- [Flask Quickstart](https://flask.palletsprojects.com/en/stable/quickstart/#routing)
- [Pandas Documentation](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
- [Python Threading](https://docs.python.org/3/library/threading.html)

