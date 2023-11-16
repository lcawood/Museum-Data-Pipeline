'''Python script to populate the Exhibition, Department, and Rating
tables within the database. This script is kept separate for 
simplicity and so that if any additional fields are to be added, 
then this can be done easily here.'''

from datetime import datetime
import logging
from os import environ

from dotenv import load_dotenv
import pandas as pd

from pipeline import connect_to_db

# Helper Function
def tuple_switch_two(val_1: str, val_2: str, list_of_tuples: list) -> int:
    '''Same as tuple_switch_one function, although the subtle difference here
    is that the tuples are now of length 3 and we see if val_1 and val_2 match
    up to the second and third element, and if so, return the first element.'''

    for tpl in list_of_tuples:
        if val_1 == tpl[1] and val_2 == tpl[2]:
            return tpl[0]
    raise ValueError("Not found!")

# Helper Function
def date_converter(date: str) -> datetime:
    '''Converts date strings of form DD/MM/YY to datetime objects of 
     the form MM/DD/YY.'''
    date = datetime.strptime(date, "%d/%m/%y")
    date = date.strftime("%m/%d/%y")
    return date


def populate_rating_table(connection) -> None:
    '''In the Ratings table, inserts data entries for each of the rating options.'''

    success = True
    try:
        cursor = connection.cursor()
        query = '''INSERT INTO Rating (Rating, Meaning)
                VALUES (0, 'Terrible'), (1, 'Bad'), (2, 'Neutral'),
                (3, 'Good'), (4, 'Amazing');'''
        cursor.execute(query)
        connection.commit()
        cursor.close()
    except Exception as e:
        success = False
        logging.error('Failed to populate rating table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully populated rating table')


def populate_department_table(connection) -> None:
    '''In the Department table, inserts data entries for each museum department.'''

    success = True
    try:
        cursor = connection.cursor()
        query = '''INSERT INTO Department (Title, Floor)
            VALUES ('Entomology', 'Vault'), ('Geology', '1'), ('Paleontology', '1'),
            ('Zoology', '2'), ('Ecology', '3'), ('Zoology', '1');'''
        cursor.execute(query)
        connection.commit()
        cursor.close()
    except Exception as e:
        success = False
        logging.error('Failed to populate department table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully populated department table')


def migrate_data_to_exhibition_table(connection) -> None:
    '''Adds data into Exhibition table. All data for this is taken from the
    combined exhibition data file, which is the combined data from the 
    downloaded .json exhibition files. This is except for the DepartmentID
    column in the Exhibition table, this is attained from the Department Table
    by mapping the department Floor and Title.'''

    museum_exh_data_folder_path = environ["MUSEUM_EXH_DATA_FOLDER_PATH"]
    success = True
    try:
        # Load the csv file into a DataFrame
        exhibition_data = pd.read_csv(museum_exh_data_folder_path)

        # Make a copy to avoid changes to the original document
        exhibition_data = exhibition_data.copy()

        # Load in the data from the Department table
        cursor = connection.cursor()
        department_query = "SELECT * FROM Department"
        cursor.execute(department_query)
        department_data = cursor.fetchall()

        # Map the floor and department in the DataFrame to the corresponding DepartmentID
        exhibition_data['DepartmentID'] = exhibition_data.apply(
            lambda x: tuple_switch_two(x['DEPARTMENT'], x['FLOOR'], department_data), axis=1)

        # Insert data into the Exhibition table
        insert_query = '''INSERT INTO Exhibition(ExhibitionID, Title,
                            Information, StartDate, DepartmentID)
                            VALUES (%s, %s, %s, %s, %s)'''

        for index, row in exhibition_data.iterrows():
            params = (row['EXHIBITION_ID'], row['EXHIBITION_NAME'], row['DESCRIPTION'],
                    date_converter(row['START_DATE']), row['DepartmentID'])
            cursor.execute(insert_query, params)
            connection.commit()

        cursor.close()
    except Exception as e:
        success = False
        logging.error('Failed to migrate data to exhibition table: %s', str(e))
    finally:
        if success:
            logging.info('Successfully migrated data to exhibition table')


if __name__ == "__main__":

    # Load environment variables from .env
    load_dotenv()

    # Connect to the database
    db_connection = connect_to_db()

    # Configuration of logging messages
    log_file_name = environ['LOG_FILE_NAME']
    logging.basicConfig(filename=log_file_name, level=logging.INFO,
                    format = '%(asctime)s-%(name)s-%(levelname)s-%(message)s')


    # Adding the data to the museum database
    populate_rating_table(db_connection)
    populate_department_table(db_connection)
    migrate_data_to_exhibition_table(db_connection)