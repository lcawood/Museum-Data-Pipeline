'''Python script to collect data from AWS S3 bucket and thereafter
combine all .csv files into a single .csv file, and to combine all
.json files into a single csv file.'''

import json
import logging
import os
from os import environ

import boto3
from dotenv import load_dotenv
import pandas as pd


def download_files_from_s3_bucket(folder_path: str) -> None:
    '''Download all relevant files from s3 bucket into a local folder.'''

    # Access the environment variables (credentials)
    aws_access_key_id = environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = environ["AWS_SECRET_ACCESS_KEY"]
    bucket_name = environ["BUCKET_NAME"]
    relevant_file_names = environ["RELEVANT_FILE_NAMES"].split(',')

    success = True
    try:
        # Create a client using these credentials
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

        # Create the local folder, if it doesn't already exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Download only the relevant files from the bucket
        objects = s3_client.list_objects(Bucket=bucket_name)['Contents']

        for obj in objects:
            for name in relevant_file_names:
                if name in obj['Key'] and ('.json' in obj['Key'] or '.csv' in obj['Key']):
                    file_name = obj['Key']
                    full_path = os.path.join(folder_path, file_name)
                    s3_client.download_file(bucket_name, file_name, full_path)
                else:
                    pass
    except Exception as e:
        success = False
        logging.error('Failed to download from s3 bucket: %s', str(e))
    finally:
        if success:
            logging.info('Successfully downloaded from s3 bucket')


def combine_csv_files(folder_path: str) -> None:
    '''For all .csv files within a folder, combines all the data into one 
    .csv file, saves this new file in the same folder, then removes the 
    individual .csv files.'''

    combined_csv_filename = environ['COMBINED_CSV_FILENAME']

    data_frames = []
    files_to_delete = []

    success = True
    try:
        # Find the .csv files and collect the data
        for file_name in os.listdir(folder_path):
            if '.csv' in file_name:
                full_path = os.path.join(folder_path, file_name)
                data = pd.read_csv(full_path)
                data_frames.append(data)
                files_to_delete.append(full_path)

        # Concatenate the collected data
        combined_data = pd.concat(data_frames)

        # Save the concatenated data to a new file
        combined_data.to_csv(f"{folder_path}/{combined_csv_filename}", index=False)

        # Remove individual .csv files
        for file in files_to_delete:
            os.remove(file)

    except Exception as e:
        success = False
        logging.error('Failed to combine .csv files: %s', str(e))
    finally:
        if success:
            logging.info('Successfully combined .csv files')


def combine_json_files(folder_path: str) -> None:
    '''For all .json files within a folder, combines all the data into one 
    .csv file, saves this new file in the same folder, then removes the 
    individual .json files.'''

    json_to_csv_filename = environ['JSON_TO_CSV_FILENAME']

    combined_data = []
    files_to_delete = []


    success = True
    try:
        # Find the .json files and collect the data
        for file_name in os.listdir(folder_path):
            if '.json' in file_name:
                full_path = os.path.join(folder_path, file_name)
                with open(full_path, 'r', encoding="utf-8") as json_file:
                    data = json.load(json_file)
                    combined_data.append(data)
                files_to_delete.append(full_path)

        # Convert combined data to a dataframe
        df = pd.DataFrame(combined_data)

        # Write the data in the dataframe to a .csv file
        csv_file = f"{folder_path}/{json_to_csv_filename}"
        df.to_csv(csv_file, index=False)

        # Remove individual .csv files
        for file in files_to_delete:
            os.remove(file)

    except Exception as e:
        success = False
        logging.error('Failed to combine .json files: %s', str(e))
    finally:
        if success:
            logging.info('Successfully combined .json files')


def convert_site_to_exhibition_id() -> None:
    '''Converts the site number in the kiosk museum data file to the 
    exhibition ID for consistency with other tables. For example site x 
    is converted to EXH_0X or EXH_X, depending on if X has 1 or 2 digits.'''


    museum_hist_data_folder_path = environ['MUSEUM_HIST_DATA_FOLDER_PATH']
    success = True
    try:
        # Load the csv file into a DataFrame
        data = pd.read_csv(museum_hist_data_folder_path)
        # Conversion
        data['site'] = data['site'].apply(lambda x: f'EXH_0{x:01}')

        # Save the changes and overwrite the file
        data.to_csv(museum_hist_data_folder_path, index=False)
    except Exception as e:
        success = False
        logging.error('Failed to convert site data: %s', {str(e)})
    finally:
        if success:
            logging.info('Successfully converted site data')

if __name__ == "__main__":

    # Load environment variables from .env
    load_dotenv()

    # Configuration of logging messages
    log_file_name = environ['LOG_FILE_NAME']
    logging.basicConfig(filename=log_file_name, level=logging.INFO,
                    format = '%(asctime)s-%(name)s-%(levelname)s-%(message)s')

    museum_data_folder_path = environ["MUSEUM_DATA_FOLDER_PATH"]
    download_files_from_s3_bucket(museum_data_folder_path)
    combine_csv_files(museum_data_folder_path)
    combine_json_files(museum_data_folder_path)
    convert_site_to_exhibition_id()
    