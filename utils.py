import os
import yaml
import logging
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def get_last_working_day_df(directory_path):
    today_str = datetime.now().strftime('%Y%m%d')
    # Find the last working day's CSV file ---
    # Get all subdirectories that are valid dates and are not today
    date_folders = [d for d in os.listdir(directory_path)
                    if os.path.isdir(os.path.join(directory_path, d))
                    and d.isdigit() and len(d) == 8 and d < today_str]

    if not date_folders:
        logging.error("💡 No previous working day's folder found. Cannot perform comparison.")
        return None

    last_working_day_folder = sorted(date_folders)[-1]
    last_day_path = os.path.join(directory_path, last_working_day_folder)

    # Find the relevant CSV file within that folder
    last_csv_file = None
    for file in os.listdir(last_day_path):
        if '_annotation_stats_' in file and file.endswith('.csv'):
            last_csv_file = file
            break

    if not last_csv_file:
        logging.error(f"⚠️ No annotation stats CSV found in the last working day's folder: {last_day_path}")
        return None

    last_csv_path = os.path.join(last_day_path, last_csv_file)
    logging.info(f" Last working day CSV path: {last_csv_path}")
    try:
        last_working_day_df = pd.read_csv(last_csv_path)
        return last_working_day_df
    except FileNotFoundError:
        logging.error(f"❌ Error: The directory '{directory_path}' was not found.")
        return None
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred: {e}")
        return None

def save_csv(proj_name, directory_path, final_stats_df):
    """
    Saves a DataFrame to a CSV file inside a new, date-stamped folder.

    Args:
        proj_name (str): The name of the project, used in the filename.
        directory_path (str): The parent directory where the new folder will be created.
        final_stats_df (pd.DataFrame): The DataFrame to save.
    """
    now = datetime.now()
    date_folder_str = now.strftime('%Y%m%d')
    datetime_file_str = now.strftime('%Y%m%d_%H%M')

    full_folder_path = os.path.join(directory_path, date_folder_str)
    filename = f"{proj_name}_annotation_stats_{datetime_file_str}.csv"
    try:
        os.makedirs(full_folder_path, exist_ok=True)
        full_file_path = os.path.join(full_folder_path, filename)
        final_stats_df.to_csv(full_file_path, index=False)
        logging.info(f"✅ Successfully saved stats to: {full_file_path}")
        return full_file_path
    except Exception as e:
        logging.warning(f"⚠️ Primary save location failed: {e}")
        logging.info("Attempting to save to home directory as a fallback...")
        home_dir = os.path.expanduser('~')
        fallback_file_path = os.path.join(home_dir, filename)
        try:
            # Attempt to save the file to the fallback location
            final_stats_df.to_csv(fallback_file_path, index=False)
            logging.info(f"⚠️ saved stats to fallback location: {fallback_file_path}")
            return fallback_file_path
        except Exception as e_fallback:
            logging.error(f"❌ Fallback save location also failed: {e_fallback}")
            logging.error("Could not save the file.")
            return None

def send_email(config, new_tasks_df, changed_tasks_df, today_csv_path, today_annotation_filenames):
    email_params = config['email_params']
    annotations_dir = config['annotations_dir']   #.split('=')[-1]
    today_str = datetime.now().strftime('%Y%m%d')

    num_new = len(new_tasks_df) + len(changed_tasks_df)

    # Convert DataFrames to HTML, adding a message for empty frames
    if new_tasks_df.empty:
        new_tasks_html = "<p>No new tasks were added today.</p>"
    else:
        new_tasks_html = new_tasks_df.to_html(index=False)

    if changed_tasks_df.empty:
        changed_tasks_html = "<p>No changes were detected in existing tasks.</p>"
    else:
        changed_tasks_html = changed_tasks_df.to_html(index=False)

    # Generate the HTML for the list of annotation files
    if today_annotation_filenames:
        # Create an HTML list item <li> for each filename
        file_list_html = ''.join([f"<li>{f}</li>" for f in today_annotation_filenames])

        # Assemble the full HTML section with a heading and the bulleted list
        annotation_files_section_html = f"""
        <h3>{num_new} New annotation files downloaded :</h3>
        <p> at: {annotations_dir}/{today_str} </p>
        <ul>
            {file_list_html}
        </ul>
        """
    else:
        annotation_files_section_html = "<p>No new annotation files were downloaded today.</p>"

    content = f"""
    <html>
      <head>
        <style>
          body {{ font-family: sans-serif; }}
          table {{ border-collapse: collapse; width: 80%; }}
          th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; }}
          th {{ background-color: #f2f2f2; }}
          ul {{ margin-top: 5px; }}
        </style>
      </head>
      <body>
        <h2>Daily EUS Annotation Report</h2>
        <h3>New Tasks Done Today</h3>
        {new_tasks_html}
        <br>
        <h3>Updates to Existing Tasks</h3>
        {changed_tasks_html}
        <hr>
        <br>
        <p>{annotation_files_section_html}</p>
        <br>
        <p>Today's full CSV report is saved at: {today_csv_path}</p>
      </body>
    </html>
    """
    subject = f"EUS annotation report for {today_str}"

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = email_params['sender']
    msg['To'] = email_params['destination']
    msg['CC'] = ', '.join(email_params['cc'])
    msg.attach(MIMEText(content, 'html'))
    recipients = [email_params['destination']] + email_params['cc']
    try:
        with smtplib.SMTP(email_params['smtp_server'], email_params['port']) as server:
            server.starttls()
            server.login(email_params['username'], email_params['password'])
            server.sendmail(email_params['sender'], recipients, msg.as_string())
            logging.info(f'Email sent to {recipients}')
    except Exception as e:
        logging.error(f"Could not send email for {today_str}: {e}")

def load_config(config_path='config.yaml'):
    """Loads the configuration from a YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Error: Configuration file not found at '{config_path}'")
        exit()
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        exit()

