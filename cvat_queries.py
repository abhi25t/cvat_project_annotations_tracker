
import copy
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from cvat_sdk import make_client
from cvat_sdk.api_client import Configuration, ApiClient, exceptions
from cvat_sdk.api_client.models import PatchedTaskWriteRequest, PatchedJobWriteRequest
from cvat_sdk.api_client.exceptions import ServiceException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cvat_configuration(proj_config):
    host = proj_config['cvat']['host'] + ':' + proj_config['cvat']['port']
    username = proj_config['cvat']['username']
    password = proj_config['cvat']['password']
    configuration = Configuration(host=host, username=username, password=password, )
    return configuration

def get_task_ids_of_project(cvat_config, project_name):
    '''
    Loops through project's page

    :param cvat_config:
    :param project_name:
    :return:
    '''
    all_task_ids = []
    page = 1
    page_size = 100  # A reasonable page size to avoid overwhelming the server

    with ApiClient(cvat_config) as api_client:
        logging.info(f"Fetching all tasks for project '{project_name}'...")
        while True:
            try:
                # Fetch one page of tasks
                (tasks_page, response) = api_client.tasks_api.list(
                    project_name=project_name,
                    page=page,
                    page_size=page_size,
                )

                # Add the task IDs from the current page to our master list
                if tasks_page.results:
                    for task in tasks_page.results:
                        all_task_ids.append(task.id)
                    logging.info(
                        f"  ... fetched page {page}, found {len(tasks_page.results)} tasks. Total so far: {len(all_task_ids)}")

                # If the 'next' attribute is None, it means we are on the last page
                if not tasks_page.next:
                    break  # Exit the loop

                # Otherwise, prepare to fetch the next page
                page += 1

            except exceptions.ApiException as e:
                logging.error(f"Exception when calling TasksApi.list() on page {page}: {e}")
                break  # Exit the loop if an error occurs

    logging.info(f"✅ Finished. Found a total of {len(all_task_ids)} tasks for the project.")

    return all_task_ids

def get_task_ids_to_name(cvat_config, project_name):
    all_task_ids = {}
    page = 1
    page_size = 100  # A reasonable page size to avoid overwhelming the server

    with ApiClient(cvat_config) as api_client:
        logging.info(f"Fetching all tasks for project '{project_name}'...")
        while True:
            try:
                # Fetch one page of tasks
                (tasks_page, response) = api_client.tasks_api.list(
                    project_name=project_name,
                    page=page,
                    page_size=page_size,
                )

                # Add the task IDs from the current page to our master list
                if tasks_page.results:
                    for task in tasks_page.results:
                        all_task_ids[task.id] = task.name
                    logging.info(
                        f"  ... fetched page {page}, found {len(tasks_page.results)} tasks. Total so far: {len(all_task_ids)}")

                # If the 'next' attribute is None, it means we are on the last page
                if not tasks_page.next:
                    break  # Exit the loop

                # Otherwise, prepare to fetch the next page
                page += 1

            except exceptions.ApiException as e:
                logging.error(f"Exception when calling TasksApi.list() on page {page}: {e}")
                break  # Exit the loop if an error occurs

    logging.info(f"✅ Finished. Found a total of {len(all_task_ids)} tasks for the project.")

    # name_to_task_id_map = {value: key for key, value in task_id_2_name_map.items()}

    return all_task_ids

def get_complete_label_mapping(cvat_config, project_id, verbose=False):
    """Get complete label ID to name mapping from project"""
    logging.info(f"Fetching label_id and name for project...")
    with ApiClient(cvat_config) as client:
        try:
            # Get labels from the project using pagination to get all
            label_id_to_name = {}
            page = 1
            page_size = 500

            while True:
                labels_list, response = client.labels_api.list(
                    project_id=project_id,
                    page=page,
                    page_size=page_size
                )

                if hasattr(labels_list, 'results'):
                    for label in labels_list.results:
                        label_id_to_name[label.id] = label.name
                elif isinstance(labels_list, list):
                    for label in labels_list:
                        label_id_to_name[label.id] = label.name

                # Check if there are more pages
                if hasattr(labels_list, 'count') and len(label_id_to_name) >= labels_list.count:
                    break
                if not hasattr(labels_list, 'results') or len(labels_list.results) < page_size:
                    break
                page += 1

            if verbose:
                logging.info("Complete label mapping:")
                for label_id, label_name in sorted(label_id_to_name.items()):
                    logging.info(f"  Label ID {label_id}: {label_name}")

            return label_id_to_name

        except Exception as e:
            logging.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return {}

def get_labels_per_frame(cvat_config, task_id, label_mapping, verbose=False):
    logging.info(f"Connecting to CVAT Server to get labels for Task ID {task_id}")
    with ApiClient(cvat_config) as client:
        try:
            # Retrieve annotations
            annotations, _ = client.tasks_api.retrieve_annotations(id=task_id)

            # Get labels per frame (only shapes)
            labels_per_frame = defaultdict(list)

            if hasattr(annotations, 'shapes') and annotations.shapes:
                for shape in annotations.shapes:
                    label_name = label_mapping.get(shape.label_id, f"Unknown_label_{shape.label_id}")
                    labels_per_frame[shape.frame].append(label_name)

            # Convert to regular dict and sort
            result = {frame: labels for frame, labels in sorted(labels_per_frame.items())}

            if verbose:
                logging.info(f"\nLabels per frame for Task {task_id}:")
                for frame, labels in result.items():
                    logging.info(f"Frame {frame}: {labels}")

            return result

        except ServiceException as e:
            # print(f"Exception object: {e}")     # TEMPORARY DEBUG CODE
            # print(f"Available attributes: {dir(e)}")
            logging.error(f"❗️ CVAT Server Error on Task ID {task_id}: Status {e.status}. Skipping this task.")
            return {}  # Return empty dict to continue the loop

        except Exception as e:
            # print(f"Error: {e}")
            logging.error(f"❗️ An unexpected error occurred on Task ID {task_id}: {e}. Skipping this task.")
            # import traceback
            # traceback.print_exc()
            return {}

def get_labels_for_all_tasks(cvat_config, all_task_ids, label_id_to_name, task_ids_to_skip):
    labels_per_task = {}
    for task_id in all_task_ids:
        if task_id in task_ids_to_skip:
            continue
        labels_per_frame = get_labels_per_frame(cvat_config, task_id, label_id_to_name)
        labels_per_task[task_id] = copy.deepcopy(labels_per_frame)
    return labels_per_task

def get_all_task_info_in_project(cvat_config, project_name):
    all_tasks_info = {}
    tasks_page = 1
    page_size = 50

    with ApiClient(cvat_config) as api_client:
        logging.info(f"Fetching all tasks and their jobs for project '{project_name}'...")
        while True:
            try:
                (tasks_page_data, _) = api_client.tasks_api.list(
                    project_name=project_name,
                    page=tasks_page,
                    page_size=page_size,
                )

                if not tasks_page_data.results:
                    break  # No more tasks

                # Process each task found on the page
                for task in tasks_page_data.results:
                    logging.info(f"Processing Task {task.id}: '{task.name}'")
                    task_assignee = task.assignee.username if task.assignee else 'Unassigned'

                    jobs_info = {}
                    jobs_page = 1

                    # 2. Inner loop to paginate through all JOBS for the current task
                    while True:
                        (jobs_page_data, _) = api_client.jobs_api.list(
                            task_id=task.id,
                            page=jobs_page,
                            page_size=page_size
                        )

                        if not jobs_page_data.results:
                            break  # No more jobs for this task

                        for job in jobs_page_data.results:
                            job_assignee = job.assignee.username if job.assignee else 'Unassigned'
                            frame_count = job.stop_frame - job.start_frame + 1
                            jobs_info[job.id] = {
                                'job_name': f"Job #{job.id} ({job.stage})",
                                'assignee': job_assignee,
                                'frame_count': frame_count,
                            }

                        if not jobs_page_data.next:
                            break  # Last page of jobs for this task
                        jobs_page += 1

                    # print(f"  --> Found {len(jobs_info)} jobs for this task.")

                    all_tasks_info[task.id] = {
                        'task_name': task.name,
                        'task_assignee': task_assignee,
                        'jobs': jobs_info
                    }

                if not tasks_page_data.next:
                    break  # Last page of tasks
                tasks_page += 1

            except exceptions.ApiException as e:
                logging.error(f"An API Exception occurred: {e}")
                break

    logging.info(f"✅ Finished. Compiled data for {len(all_tasks_info)} tasks.")

    return all_tasks_info

def get_cvat_data(proj_config):
    project_name = proj_config['cvat']['project_name']
    project_id = proj_config['cvat']['project_id']
    task_ids_to_skip = proj_config['cvat']['task_ids_to_skip']

    cvat_config = get_cvat_configuration(proj_config)
    label_id_to_name = get_complete_label_mapping(cvat_config, project_id)
    all_task_ids = get_task_ids_of_project(cvat_config, project_name)
    labels_per_task = get_labels_for_all_tasks(cvat_config, all_task_ids, label_id_to_name, task_ids_to_skip)
    all_tasks_info = get_all_task_info_in_project(cvat_config, project_name)

    return all_tasks_info, labels_per_task

def get_taskid_2_jobid(cvat_config, project_name):
    # Assuming - each task has only one job
    taskid_2_jobid_map = {}
    tasks_page = 1
    page_size = 50

    with ApiClient(cvat_config) as api_client:
        logging.info(f"Fetching all tasks and their jobs for project {project_name}...")
        while True:
            try:
                (tasks_page_data, _) = api_client.tasks_api.list(
                    project_name=project_name,
                    page=tasks_page,
                    page_size=page_size, )

                if not tasks_page_data.results:
                    break  # No more tasks

                # Process each task found on the page
                for task in tasks_page_data.results:
                    logging.info(f"Processing Task {task.id}: '{task.name}'")
                    (jobs_page_data, _) = api_client.jobs_api.list(task_id=task.id, page=1, page_size=page_size)

                    if len(jobs_page_data.results) > 1:
                        logging.error(f'More than 1 job for {task.id = }')
                        continue

                    job = jobs_page_data.results[0]
                    taskid_2_jobid_map[task.id] = job.id

                if not tasks_page_data.next:
                    break  # Last page of tasks
                tasks_page += 1

            except exceptions.ApiException as e:
                logging.error(f"An API Exception occurred: {e}")
                break

    logging.info(f"✅ Got job_id for {len(taskid_2_jobid_map)} tasks in {project_name} project.")

    return taskid_2_jobid_map

def assign_task_to_user(cvat_config_object, task_id, user_id):
    logging.info (f'Assigning {task_id = } to {user_id = }')
    with ApiClient(cvat_config_object) as client:
        task, response = client.tasks_api.retrieve(id=task_id)
        logging.info(f"Found task: {task.name} (ID: {task.id})")
        try:
            if task.assignee:
                logging.warning(f"Current assignee for task_id {task_id}: {task.assignee['username']}")
        except: pass
        # Create a request object with only the fields you want to change.
        update_spec = PatchedTaskWriteRequest(assignee_id=user_id)
        logging.info('PatchedTaskWriteRequest created')
        try:
            # Call the partial_update method.
            client.tasks_api.partial_update(id=task_id, patched_task_write_request=update_spec)
            logging.info(f"Assigned task {task_id} to user {user_id}")   # {task.assignee['username']}
        except exceptions.ApiException as e:
            logging.error(f"An API error occurred during assignment: {e}")

def assign_job_to_user(cvat_config_object, job_id, user_id):
    logging.info (f'Assigning {job_id = } to {user_id = }')
    with ApiClient(cvat_config_object) as client:

        # Retrieve the job details (optional, for verification)
        job, response = client.jobs_api.retrieve(id=job_id)
        logging.info(f"Found job ID: {job.id} from (Task ID: {job.task_id})")
        try:
            if job.assignee:
                logging.warning(f"Current assignee for job_id {job_id}: {job.assignee}")
        except:
            pass
        # Create a request object to update the assignee
        update_spec = PatchedJobWriteRequest(assignee=user_id)
        logging.info('PatchedJobWriteRequest created')
        try:
            # Call the partial_update method for the job
            client.jobs_api.partial_update(id=job_id, patched_job_write_request=update_spec)

            logging.info(f"Assigned job {job_id} to user {user_id}")
        except exceptions.ApiException as e:
            logging.error(f"An API error occurred during job assignment: {e}")

def download_taskid_annotations(proj_config, task_id, annotations_dir, task_name):
    username      = proj_config['cvat']['username']
    password      = proj_config['cvat']['password']
    host          = proj_config['cvat']['host']
    port          = proj_config['cvat']['port']
    annotation_format = proj_config['cvat']['annotation_format']

    filename = task_name
    try:
        filename = Path(task_name).stem
    except: pass

    output_filename = f"{filename}_datumaro_annotations.zip"
    out_path = Path(annotations_dir, output_filename)
    logging.info(f'Downloading annotation file for {task_id = }, {task_name =}')
    try:
        with make_client(host, port=port, credentials=(username, password)) as client:
            task = client.tasks.retrieve(task_id)
            task.export_dataset(annotation_format, out_path, include_images=False)
            logging.info(f' ✅ Successfully Downloaded annotation file for {task_id = }, {task_name =}')
    except:
        logging.error(f'Error in downloading annotation file for {task_id = }, {task_name =}')
    return output_filename

def download_new_tasks_annotations(proj_config, new_tasks_df, changed_tasks_df):
    annotations_dir = proj_config['annotations_dir']
    today_str = datetime.now().strftime('%Y%m%d')
    today_annotations_dir = Path(annotations_dir, today_str)
    today_annotations_dir.mkdir(exist_ok=True)
    today_annotation_filenames = []

    new_tasks = dict(zip(new_tasks_df['task_id'], new_tasks_df['task_name']))
    changed_tasks = dict(zip(changed_tasks_df['task_id'], changed_tasks_df['task_name']))
    new_tasks.update(changed_tasks)

    for task_id in new_tasks:
        task_name = new_tasks[task_id]
        annotation_filename = download_taskid_annotations(proj_config, task_id, today_annotations_dir, task_name)
        today_annotation_filenames.append(annotation_filename)
    return today_annotation_filenames

