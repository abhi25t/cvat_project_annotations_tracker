# CVAT Daily Annotation Report

A set of Python scripts for CVAT administrators to monitor and report on daily annotation progress. This tool generates a daily CSV report, compares it against the previous working day to identify new and updated tasks, downloads the corresponding annotations, and sends a summary email to stakeholders.

## Key Features

* **Daily Stats:** Calculates total annotated frames, unique objects, and total objects per task.
* **Delta Reporting:** Compares today's stats with the last working day to find:
    * Newly completed tasks.
    * Existing tasks with new annotations.
* **Email Reports:** Automatically sends an HTML email report summarizing new and updated tasks.
* **Annotation Backup:** Downloads annotation files (in the format specified in the config) for all new and changed tasks.
* **Persistent Logging:** Saves a time-stamped CSV of all project stats for historical tracking.

## How It Works

The main script `annotation_report.py` orchestrates the following process:

1.  **Load Config:** Reads server details, project info, and email settings from `config.yaml`.
2.  **Fetch CVAT Data:** (`cvat_queries.py`) Connects to the CVAT API to get all task info, job info, and label annotations for the specified project.
3.  **Calculate Stats:** (`analytics.py`) Processes the raw annotation data into a structured `pandas.DataFrame` with key statistics.
4.  **Save Today's CSV:** (`utils.py`) Saves the full stats report to a new date-stamped folder (e.g., `/projects/20251029/`).
5.  **Compare Deltas:** (`analytics.py`) Loads the last working day's CSV and compares it with today's report to find new and changed tasks.
6.  **Download Annotations:** (`cvat_queries.py`) Downloads a .zip of the annotations for any new or changed tasks.
7.  **Send Email:** (`utils.py`) Formats and sends an HTML email report with the delta tables and file locations.

## ⚠️ Important Assumption

This script is built with a critical assumption: **Each Task in CVAT has exactly one Job.**

The logic for data fetching and statistics generation is designed around this 1-to-1 mapping. If your tasks have multiple jobs (e.g., for annotation, validation, and review), the statistics will be inaccurate, as the script will likely only process the first job it finds.

## Setup

1.  Clone this repository.
2.  Create a virtual environment: `python -m venv venv` and activate it.
3.  Install the required dependencies (or add them to a `requirements.txt`):
    ```bash
    pip install pandas pyyaml cvat-sdk
    ```
4.  [cite_start]Copy the `config.yaml` from the source [cite: 589-608] and place it in the root directory.

## Configuration

Update the `config.yaml` file with your specific environment details:

```yaml
# Directory to store historical CSV reports
proj_dir: '/projects/'

# Directory to save downloaded annotation .zip files
annotations_dir: '/annotations/'

cvat:
  username: 'your_cvat_username'
  password: 'your_cvat_password'
  host: 'http://your_cvat_ip'
  port: '8080'
  annotation_format: 'Datumaro 1.0'
  project_id: 2
  project_name: 'EUS'
  task_ids_to_skip: [] # Add any task IDs you want to ignore

email_params:
  smtp_server: 'smtp.your-company.com'
  sender: 'sender@company.com'
  destination: 'destination@company.com'
  cc: ['manager1@company.com', 'manager2@company.com']
  port: 587
  username: 'sender@company.com'
  password: 'your_email_password'
