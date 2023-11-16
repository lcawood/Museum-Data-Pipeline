'''A Python script to move data from .csv files into a database. 
Database contains multiple tables and data entries in the .csv files 
are migrated to tables as appropriate.'''

import logging
from os import environ

from dotenv import load_dotenv
import pandas as pd
import psycopg2
import psycopg2.extras


def connect_to_db():
    '''Makes a connection to the database.'''

    success = True
    try:
        connection = psycopg2.connect(
                host=environ["DATABASE_URL"],
                dbname=environ["DATABASE_NAME"],
                user=environ["DATABASE_USERNAME"],
                password=environ["DATABASE_PASSWORD"],
                port=environ["DATABASE_PORT"]
                )
        return connection
    except Exception as e:
        success = False
        logging.error('Failed to connect to database: %s', str(e))
        return None
    finally:
        if success:
            logging.info('Successfully connected to database')

# Helper Function
def tuple_switch_one(val: int, list_of_tuples: list) -> int:
    '''Given a list of any length containing distinct tuples of length two, 
    and also an integer value, if this value is the second element in the
    tuple, returns the first element in the tuple.'''

    for tpl in list_of_tuples:
        if val == tpl[1]:
            return tpl[0]
    raise ValueError("Not found!")


def migrate_data_to_vote_table(connection) -> None:
    '''Moves the relevant data in the kiosk data file to the vote table
    in the museum database. The raw data contains the vote (0-4), and this is mapped
    to its corresponding RatingID, which is found in the Rating table.'''

    museum_hist_data_folder_path = environ["MUSEUM_HIST_DATA_FOLDER_PATH"]
    success = True
    try:
        # Load the csv file into a DataFrame
        kiosk_data = pd.read_csv(museum_hist_data_folder_path)

        # Filter the kiosk data to exclude emergency and assistance data
        filtered_data = kiosk_data[kiosk_data['val'] != -1].copy()

        # Load in the data from the Rating table
        cursor = connection.cursor()
        rating_query = "SELECT RatingID, Rating from Rating;"
        cursor.execute(rating_query)
        rating_data = cursor.fetchall()

        # Map the rating in the DataFrame to the corresponding RatingID
        filtered_data['val'] = filtered_data['val'].apply(
            lambda x: tuple_switch_one(x, rating_data))

        # Insert data into the Vote table
        insert_query = "INSERT INTO Vote(ExhibitionID, VoteTime, RatingID) VALUES (%s, %s, %s)"

        for index, row in filtered_data.iterrows():
            params = (row['site'], row['at'], row['val'])
            cursor.execute(insert_query, params)
            connection.commit()

        cursor.close()

    except Exception as e:
        success = False
        logging.error('Failed to migrate data to vote table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully migrated data to vote table')


def migrate_data_to_assistance_table(connection) -> None:
    '''Moves the relevant data in the kiosk data file to the assistance table
    in the museum database. The SQL query looks in the data for a value of -1 and type
    of 0.0 which indicates this record is for assistance, and thereafter moves these
    data entries across.'''

    museum_hist_data_folder_path = environ["MUSEUM_HIST_DATA_FOLDER_PATH"]
    success = True
    try:
        # Load the csv file into a DataFrame
        data = pd.read_csv(museum_hist_data_folder_path)

        # Filter the kiosk data to get only the assistance records
        filtered_data = data[(data['val'] == -1) & (data['type'] == 0.0)]

        cursor = connection.cursor()
        query = "INSERT INTO Assistance(ExhibitionID, AssistanceTime) VALUES (%s, %s)"

        # Insert the data into the Assistance table
        for index, row in filtered_data.iterrows():
            params = (row['site'], row['at'])
            cursor.execute(query, params)
            connection.commit()

        cursor.close()
    except Exception as e:
        success = False
        logging.error('Failed to migrate data to assistance table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully migrated data to assistance table')


def migrate_data_to_emergency_table(connection) -> None:
    '''Moves the relevant data in the kiosk data file to the emergency table
    in the museum database. The SQL query looks in the data for a value of -1 and type
    of 1.0 which indicates this record is for emergency, and thereafter moves these
    data entries across.'''

    museum_hist_data_folder_path = environ["MUSEUM_HIST_DATA_FOLDER_PATH"]
    success = True
    try:
        # Load the csv file into a DataFrame
        data = pd.read_csv(museum_hist_data_folder_path)

        # Filter the kiosk data to get only the emergency records
        filtered_data = data[(data['val'] == -1) & (data['type'] == 1.0)]

        cursor = connection.cursor()
        query = "INSERT INTO Emergency(ExhibitionID, EmergencyTime) VALUES (%s, %s)"

        # Insert the data into the Emergency table
        for index, row in filtered_data.iterrows():
            params = (row['site'], row['at'])
            cursor.execute(query, params)
            connection.commit()

        cursor.close()
    except Exception as e:
        success = False
        logging.error('Failed to migrate data to emergency table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully migrated data to emergency table')


if __name__ == "__main__":

    # Load environment variables from .env
    load_dotenv()

    # Configuration of logging messages
    log_file_name = environ['LOG_FILE_NAME']
    logging.basicConfig(filename=log_file_name, level=logging.INFO,
                    format = '%(asctime)s-%(name)s-%(levelname)s-%(message)s')

    # Connect to the database
    db_connection = connect_to_db()

    
    # For inserting static data into db...
    migrate_data_to_vote_table(db_connection)
    migrate_data_to_assistance_table(db_connection)
    migrate_data_to_emergency_table(db_connection)
    
   