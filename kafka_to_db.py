'''A python script to collect live data from a Kafka cluster 
and input this data into a database.'''

from argparse import ArgumentParser
from datetime import datetime
from os import environ
import json
import logging

from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv

from pipeline.pipeline import connect_to_db, tuple_switch_one

# Helper function


def transform_kafka_data(data: dict) -> dict:
    '''Takes the kafka data as input as formats this ready to be 
    inserted into the database.'''

    # Convert the time to a format consistent with the database
    time = datetime.strptime(data['at'], '%Y-%m-%dT%H:%M:%S.%f%z')
    formatted_time = time.strftime('%m/%d/%y %H:%M:%S')
    transformed_data = {
        'at': formatted_time,
        'site': "EXH_0" + data['site'].rjust(1, '0'),
        'val': data['val']
    }
    # Add 'type' to separate data into vote, emergency, or assistance
    if 'type' in data:
        transformed_data['type'] = data['type']
    else:
        transformed_data['type'] = None
    return transformed_data

# Helper function


def move_to_db(data: dict, connection) -> None:
    '''Takes an input of the formatted kafka data and inserts this into 
    its appropriate table within the database.'''

    cursor = connection.cursor()

    # Data is a vote
    if data['val'] != -1:

        # Load in the data from the Rating table
        rating_query = "SELECT RatingID, Rating from Rating;"
        cursor.execute(rating_query)
        rating_data = cursor.fetchall()

        # Map the rating value in the data to the corresponding RatingID
        matching_rating_id = tuple_switch_one(data['val'], rating_data)

        # Insert into Vote table
        vote_query = "INSERT INTO Vote(ExhibitionID, VoteTime, RatingID) VALUES (%s, %s, %s)"
        vote_params = (data['site'], data['at'], matching_rating_id)
        cursor.execute(vote_query, vote_params)
        connection.commit()

    # Data is an assistance request
    elif data['val'] == -1 and data['type'] == 0:

        assistance_query = "INSERT INTO Assistance(ExhibitionID, AssistanceTime) VALUES (%s, %s)"
        assistance_params = (data['site'], data['at'])
        cursor.execute(assistance_query, assistance_params)
        connection.commit()

    # The data is an emergency request
    elif data['val'] == -1 and data['type'] == 1:

        emergency_query = "INSERT INTO Emergency(ExhibitionID, EmergencyTime) VALUES (%s, %s)"
        emergency_params = (data['site'], data['at'])
        cursor.execute(emergency_query, emergency_params)
        connection.commit()

# Main function


def process_kafka_data(consumer: Consumer, topic: str, connection) -> None:
    '''Consumes data from the Kafka cluster, formats the data, and inserts
    the data into the database. Any data that contains errors will not be 
    inserted, and instead is logged to a separate file along with the 
    corresponding error message.'''

    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            # Look for another message if none received within the second
            if msg is None:
                continue
            try:
                msg_string = msg.value().decode()
                msg_dict = json.loads(msg_string)

                # Check for missing fields
                if 'at' not in msg_dict.keys() or \
                    'site' not in msg_dict.keys() or \
                        'val' not in msg_dict.keys():
                    logging.info('Message: %s', msg_dict)
                    logging.info("Error: Missing 'at', 'site', or 'val'\n")
                    continue

                # Check for valid exhibition site
                if msg_dict['site'] not in [str(x) for x in range(6)] \
                        or len(msg_dict['site']) != 1:
                    logging.info('Message: %s', msg_dict)
                    logging.info('Error: Invalid exhibition number\n')
                    continue

                # Check for valid rating (or -1 for request)
                if msg_dict['val'] not in list(range(-1, 5)):
                    logging.info('Message: %s', msg_dict)
                    logging.info('Error: Value out of range\n')
                    continue

                # If request, then check for valid type
                if 'type' in msg_dict.keys():
                    if msg_dict['type'] not in [0, 1]:
                        logging.info('Message: %s', msg_dict)
                        logging.info('Error: Invalid type given\n')
                        continue

            # Catch errors within the Kafka client consuming messages
            except KafkaException:
                logging.error('Kafka error: %s', KafkaException)

            # Catch errors during interaction with Kafka cluster
            except KafkaError:
                logging.error('Kafka error: %s', KafkaError)

            # Format the valid data and insert into the database
            transformed_data = transform_kafka_data(msg_dict)
            move_to_db(transformed_data, connection)

            # Live data feed output to the console
            print(transformed_data)

    except KeyboardInterrupt:
        pass
    finally:
        # Stop listening for messages and close the connection
        consumer.close()
        connection.close()


if __name__ == "__main__":

    # Load environment variables from .env
    load_dotenv()

    kafka_config = {
        'bootstrap.servers': environ["KAFKA_SERVER"],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': environ["SASL_USERNAME"],
        'sasl.password': environ["SASL_PASSWORD"],
        'group.id': 'museum-data-consumer-group',
        'auto.offset.reset': 'latest'
    }

    # Connect to the database
    db_connection = connect_to_db()

    # Log error messages only if specified to do so
    parser = ArgumentParser(description='Kafka consumer')
    parser.add_argument('-l', '--log_file', type=str,
                        help='Enter the log file name')
    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(filename='error_log.txt', level=logging.INFO,
                            format='%(asctime)s-%(levelname)s-%(message)s')

    process_kafka_data(Consumer(kafka_config), 'lmnh', db_connection)
