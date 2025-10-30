import logging
import pandas as pd
from collections import defaultdict
from utils import get_last_working_day_df


def compare_with_last_working_day(proj_dir, today_stats_df):
    """
    Compares the current day's stats with the last working day's stats.

    Args:
        proj_dir (str): The parent directory containing date-stamped subfolders.
        today_stats_df (pd.DataFrame): The DataFrame of today's annotation stats.

    Returns:
        A tuple of two DataFrames: (new_tasks_df, changed_tasks_df).
        Returns (None, None) if a previous file cannot be found.
    """

    last_working_day_df = get_last_working_day_df(proj_dir)
    if last_working_day_df is None:
        return None, None

    #Prepare DataFrames by temporarily setting 'task_id' as the index ---
    current_df_indexed = today_stats_df.set_index('task_id')
    last_day_df_indexed = last_working_day_df.set_index('task_id')

    current_ids = current_df_indexed.index
    last_day_ids = last_day_df_indexed.index

    # Filter for NEW tasks
    new_task_ids = current_ids.difference(last_day_ids)
    new_tasks_df = current_df_indexed.loc[new_task_ids]

    # --- Filter for CHANGED tasks ---
    common_task_ids = current_ids.intersection(last_day_ids)
    # Align both dataframes to only common tasks
    current_common = current_df_indexed.loc[common_task_ids]
    last_day_common = last_day_df_indexed.loc[common_task_ids]

    # Find rows where the annotation counts have changed
    changed_mask = (current_common['frames_annotated'] != last_day_common['frames_annotated']) | \
                   (current_common['total_obj_annotated'] != last_day_common['total_obj_annotated'])

    changed_tasks_df = current_common[changed_mask].copy()

    if not changed_tasks_df.empty:
        # Calculate the amount of change (delta)
        frames_delta = current_common['frames_annotated'] - last_day_common['frames_annotated']
        objects_delta = current_common['total_obj_annotated'] - last_day_common['total_obj_annotated']

        # Add the change columns to the changed_tasks_df
        changed_tasks_df['frames_added'] = frames_delta[changed_mask]
        changed_tasks_df['obj_added'] = objects_delta[changed_mask]

    # Reset the index to turn 'task_id' back into a column.
    new_tasks_df = new_tasks_df.reset_index()
    changed_tasks_df = changed_tasks_df.reset_index()
    return new_tasks_df, changed_tasks_df

def get_task_stats_in_project(all_tasks_info):
    #Assuming - each task has only one job

    project_stats = {}

    for task_id, task_data in all_tasks_info.items():
        task_assignee = task_data['task_assignee']

        # Since each task has only one job, we can directly access it
        jobs_dict = task_data['jobs']
        job_id = list(jobs_dict.keys())[0]
        job_assignee = jobs_dict[job_id]['assignee']
        frame_count = jobs_dict[job_id]['frame_count']

        final_assignee = None

        # --- Assignee Resolution Logic ---

        # 1. Skip if there is a direct conflict
        if task_assignee != 'Unassigned' and job_assignee != 'Unassigned' and task_assignee != job_assignee:
            logging.warning(
                f"⚠️ Skipping Task ID {task_id}: Mismatch -> Task assignee '{task_assignee}' vs Job assignee '{job_assignee}'.")
            continue

        # 2. If one is 'Unassigned', use the other
        if task_assignee != 'Unassigned':
            final_assignee = task_assignee
        elif job_assignee != 'Unassigned':
            final_assignee = job_assignee
        # 3. If both are 'Unassigned', final_assignee remains None (as initialized)

        # Add the cleaned data to the results
        project_stats[task_id] = {
            'task_name': task_data['task_name'],
            'job_id': int(job_id),  # Convert job_id to integer for consistency
            'frame_count': frame_count,
            'assignee': final_assignee
        }

    return project_stats

def get_annotation_stats(project_stats, labels_per_task):
    data_for_df = []

    for task_id_str, stats in project_stats.items():
        task_id = int(task_id_str)

        # Use .get() to safely handle cases where a task has no annotations
        task_annotations = labels_per_task.get(task_id, {})

        number_of_frames_annotated = len(task_annotations)
        if number_of_frames_annotated == 0:
            continue

        unique_objects = set()
        total_objects_annotated = 0
        for frame_labels in task_annotations.values():
            unique_objects.update(frame_labels)  # .update() adds all items from a list to a set
            unique_objects_in_frame = set(frame_labels)
            total_objects_annotated += len(unique_objects_in_frame)

        unique_objects_annotated = len(unique_objects)

        data_for_df.append({
            'task_id': task_id,
            'job_id': stats['job_id'],
            'task_name': stats['task_name'],
            'frames': stats['frame_count'],
            'Assignee': stats['assignee'],
            'frames_annotated': number_of_frames_annotated,
            'unique_obj_annotated': unique_objects_annotated,
            'total_obj_annotated': total_objects_annotated
        })

    # Create the DataFrame from our list of dictionaries
    if not data_for_df:
        # Return an empty DataFrame with correct columns if there's no data
        return pd.DataFrame(columns=[
            'task_id', 'job_id', 'task_name', 'frames', 'Assignee',
            'frames_annotated', 'unique_obj_annotated', 'total_obj_annotated'
        ])

    df = pd.DataFrame(data_for_df)

    # Sort the DataFrame as requested
    df_sorted = df.sort_values(by=['Assignee', 'task_id'])

    return df_sorted #.set_index('task_id')

def get_all_label_counts_in_project(labels_per_task, verbose=False):
    all_label_counts = defaultdict(int)

    # Iterate through the outer dictionary's values (which are the inner dicts)
    for task_data in labels_per_task.values():
        # Iterate through the inner dictionary's values (which are the lists of strings)
        for labels_list in task_data.values():
            # Iterate through each label in the list
            for label in labels_list:
                # Increment the count for that label
                all_label_counts[label] += 1

    if verbose:
        for label in all_label_counts:
            logging.info(f'{label} = {all_label_counts[label]}')

    return all_label_counts


