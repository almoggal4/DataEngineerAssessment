import os
from MobileyeDatabase import MobileyeDatabase
from pathlib import Path
import json
import threading
import time

# The path of the current/created DB.
DB_PATH = r".\MobileyeDB"
# The directory we want to monitor for new object and vehicle files.
Monitor_Directory_Path = r".\MonitorFilesDirectory"
# Files prefix of the object and vehicle files.
OBJECTS_FILES_PREFIX = "object_detection"
VEHICLES_FILES_PREFIX = "vehicle_status"
# Is the process is still running
PROCESS_RUNNING = True


def get_new_files(directory, last_processed_time):
    """
    Get the list of new files that have been modified after the last processed time.
    :param directory: The directory to monitor for new files.
    :param last_processed_time: The timestamp of the last processed file.
    :return: A list of new files.
    """
    new_files = []
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        # Get the modification time of the file.
        current_file_processed_time = os.path.getmtime(file_path)
        # Check if the file is new (modified after the last processed time). If so, append it to the list of new files.
        if current_file_processed_time > last_processed_time:
            new_files.append(file_path)
    return new_files


def insert_values_to_db(db, files):
    """
    gets a list of files, check if the file is 'object' file or 'vehicle' file and if the file is new (the row does not
    exists yet). if so, inserting the values of the new file into his corresponding table in the DB with his vehicle id
    and his timestamp.
    :param db: the DB we will insert the new values from the new files.
    :param files: the new files that were detected.
    """
    # checking all the new file/s.
    for file in files:
        # getting the filename.
        filename = os.path.basename(file)
        print(filename)
        # if the file is an object detection file.
        if OBJECTS_FILES_PREFIX in filename:
            # extracting the json data from the object json file.
            with open(file) as new_file:
                new_json_data = json.load(new_file)
            # checking every event.
            for object_event in new_json_data['objects_detection_events']:
                # extracting the data of the event
                vehicle_id = object_event['vehicle_id']
                timestamp = object_event['detection_time']
                # I decided to just insert the events as one string unit into the DB, instead of splitting each event to
                # his own row. It's more space efficient because there are less duplicate values in the DB.
                detections = str(object_event['detections'])
                # if the row does not exists (ideally shouldn't happen but the code can run twice on the same files).
                if not db.is_detections_exists(vehicle_id, timestamp):
                    # inserting the extracted data to the corresponding DB table.
                    db.insert_object_detections(vehicle_id, timestamp, detections)
        # elif the file is a vehicle status file.
        elif VEHICLES_FILES_PREFIX in filename:
            # extracting the json data from the vehicle json file.
            with open(file) as new_file:
                new_json_data = json.load(new_file)
            # checking every status.
            for vehicle_status in new_json_data['vehicle_status']:
                # extracting the data of the status
                vehicle_id = vehicle_status['vehicle_id']
                timestamp = vehicle_status['report_time']
                status = vehicle_status['status']
                # if the row does not exists (ideally shouldn't happen but the code can run twice on the same files).
                if not db.is_status_exists(vehicle_id, timestamp):
                    # inserting the extracted data to the corresponding DB table.
                    db.insert_vehicle_status(vehicle_id, timestamp, status)


def monitor_new_files_process(monitor_directory, db):
    """
    Running a thread process that checks for the arrival of new files and another thread process to load them
    into their corresponding tables in the DB.
    :param monitor_directory: the directory we want to monitor for new files.
    :param db: the db we to use for inserting the new data of the files.
    """
    # using global variable and modifying it
    global PROCESS_RUNNING
    # Get the initial modification time of the monitor directory
    last_file_processed_time = time.time()
    # monitors the directory as long as the process is running
    while PROCESS_RUNNING:
        # Get the list of new files since the last processed time
        new_files = get_new_files(monitor_directory, last_file_processed_time)
        # if there are new files
        if new_files:
            # inserting the values of the new files to their corresponding db.
            # using threads for more efficiency (monitor thread will monitor and the worker thread will run the function +
            # the main thread is not blocked on the function + it's more organized)
            worker_thread = threading.Thread(target=insert_values_to_db, args=(db, new_files, ))
            worker_thread.start()
            # Update the last processed time to the latest modification time of the new file.
            for file in new_files:
                current_file_processed_time = os.path.getmtime(file)
                if current_file_processed_time > last_file_processed_time:
                    last_file_processed_time = current_file_processed_time
        # letting the monitor thread a break for one second for cpu efficiency - not continuously checking for new files.
        time.sleep(1)


# creating/connecting to the mobileye DB.
mobileye_db = MobileyeDatabase(DB_PATH)

# starting the process using threading for more efficiency (monitor thread will monitor and the worker thread will run the function +
#             # the main thread is not blocked on the function + it's more organized)
monitor_thread = threading.Thread(target=monitor_new_files_process, args=(Monitor_Directory_Path, mobileye_db, ))
monitor_thread.start()

# handling the stop of the process
input("Press Enter to stop the process...\n")
PROCESS_RUNNING = False
