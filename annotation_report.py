import logging

from analytics import compare_with_last_working_day, get_task_stats_in_project, get_annotation_stats
from utils import send_email, load_config, save_csv
from cvat_queries import get_cvat_data, download_new_tasks_annotations

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_todays_eus_csv(proj_config, all_tasks_info, labels_per_task):
    proj_dir = proj_config['proj_dir']
    project_name = proj_config['cvat']['project_name']
    task_stats_in_project = get_task_stats_in_project(all_tasks_info)
    final_stats_df = get_annotation_stats(task_stats_in_project, labels_per_task)
    csv_path = save_csv(project_name, proj_dir, final_stats_df)
    return csv_path, final_stats_df


if __name__ == '__main__':
    config = load_config()
    proj_dir = config['proj_dir']
    all_tasks_info, labels_per_task = get_cvat_data(config)
    today_csv_path, today_stats_df = save_todays_eus_csv(config, all_tasks_info, labels_per_task)
    new_tasks_df, changed_tasks_df = compare_with_last_working_day(proj_dir, today_stats_df)
    today_annotation_filenames = download_new_tasks_annotations(config, new_tasks_df, changed_tasks_df)
    send_email(config, new_tasks_df, changed_tasks_df, today_csv_path, today_annotation_filenames)

