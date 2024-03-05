import sqlite3
import ast

# The table names of the object and vehicle files.
OBJECTS_TABLE_NAME = "ObjectsEvents"
VEHICLES_TABLE_NAME = "VehiclesStatus"
DB_PATH = r".\MobileyeDB"

class MobileyeDatabase:
    def __init__(self, db_location, objects_table_name=OBJECTS_TABLE_NAME, vehicles_table_name=VEHICLES_TABLE_NAME):
        """
        Creates a DB for the Mobileye data.
        The DB will have 2 differnt tables: one for the objects events and one for the vehicle status.
        :param db_location:  The path for the DB creation/connection.
        :param objects_table_name: the object files table name (the default value is OBJECTS_TABLE_NAME)
        :param vehicles_table_name: the vehicle files table name (the default value is VEHICLES_TABLE_NAME)
        """
        # creating/connecting to the Mobileye DB (without threading check).
        self.conn = sqlite3.connect(db_location, check_same_thread=False)
        self.cur = self.conn.cursor()
        # saving the attributes of the class
        self.objects_table_name = objects_table_name
        self.vehicles_table_name = vehicles_table_name
        # creating a table for the objects events
        creating_object_table_query = f"CREATE TABLE IF NOT EXISTS {objects_table_name} (VehicleId text, Timestamp text, Detections text)"
        self.cur.execute(creating_object_table_query)
        self.conn.commit()
        # creating a table for the vehicle events
        creating_vehicles_table_query = f"CREATE TABLE IF NOT EXISTS {vehicles_table_name} (VehicleId text, Timestamp text, Status text)"
        self.cur.execute(creating_vehicles_table_query)
        self.conn.commit()

    def insert_object_detections(self, vehicle_id, timestamp, detections):
        """
        Inserting object event data to the DB based on the corresponding table name.
        :param vehicle_id: on which vehicle does the event occur.
        :param timestamp: the timestamp of the event.
        :param event: the event that occur.
        """
        inserting_object_detection_query = f"INSERT INTO {self.objects_table_name} VALUES (?, ?, ?)"
        self.cur.execute(inserting_object_detection_query, (vehicle_id, timestamp, detections, ))
        self.conn.commit()

    def insert_vehicle_status(self, vehicle_id, timestamp, status):
        """
        Inserting vehicle status data to the DB based on the corresponding table name.
        :param vehicle_id: on which vehicle does is this status.
        :param timestamp: the timestamp of the status.
        :param status: the status of the vehicle.
        """
        inserting_vehicle_status_query = f"INSERT INTO {self.vehicles_table_name} VALUES (?, ?, ?)"
        self.cur.execute(inserting_vehicle_status_query, (vehicle_id, timestamp, status))
        self.conn.commit()

    def is_detections_exists(self, vehicle_id, timestamp):
        """
        Check if object detections already exists in the DB.
        :param vehicle_id: the vehicle id of the object.
        :param timestamp: the timestamp of the object.
        :return: True if the value exists, False otherwise.
        """
        # check if there is a row with the vehicle id and the timestamp provided, in the corresponding table.
        check_detections_exists_query = f"SELECT Timestamp FROM {self.objects_table_name} WHERE Timestamp = ? AND VehicleId = ?"
        self.cur.execute(check_detections_exists_query, (timestamp, vehicle_id, ))
        return self.cur.fetchone() is not None

    def is_status_exists(self, vehicle_id, timestamp):
        """
        Check if object status already exists in the DB.
        :param vehicle_id: the vehicle id of the object.
        :param timestamp: the timestamp of the object.
        :return: True if the value exists, False otherwise.
        """
        # check if there is a row with the vehicle id and the timestamp provided, in the corresponding table.
        check_status_exists_query = f"SELECT Timestamp FROM {self.vehicles_table_name} WHERE Timestamp = ? AND VehicleId = ?"
        self.cur.execute(check_status_exists_query, (timestamp, vehicle_id, ))
        return self.cur.fetchone() is not None

    def vehicle_current_status(self, vehicle_id):
        """
        Get the latest status of a vehicle.
        :param vehicle_id: the vehicle.
        :return: the latest status of the vehicle.
        """
        # get all the status timestamps of the vehicle
        vehicle_timestamps_query = f"SELECT Status FROM {self.vehicles_table_name} WHERE VehicleId = ?"
        self.cur.execute(vehicle_timestamps_query, (vehicle_id, ))
        vehicle_timestamps = self.cur.fetchall()
        # getting the latest status of the vehicle - the latest row is the latest status
        # * [:-1] did not work for me...
        return vehicle_timestamps[len(vehicle_timestamps) - 1][0]

    def object_latest_detections(self, vehicle_id):
        """
        return string with the latest detections of a vehicle.
        :param vehicle_id: the vehicle id.
        :return: string with the latest detections of a vehicle
        """
        # get all the events timestamp of the vehicle
        object_timestamps_query = f"SELECT Detections FROM {self.objects_table_name} WHERE VehicleId = ?"
        self.cur.execute(object_timestamps_query, (vehicle_id, ))
        object_timestamps = self.cur.fetchall()
        # getting the latest detections of the vehicle
        detections = ast.literal_eval(object_timestamps[len(object_timestamps) - 1][0])
        # for every detection concatenating the output string
        output = "The following objects were detected:\n"
        for detection in detections:
            detection_values = list(detection.values())
            output += str(detection_values[1]) + " " + detection_values[0] + "\n"
        return output.rstrip('\n')

    def is_clean_record_car(self, vehicle_id):
        """
        checks if the car was not involved in an accident
        :param vehicle_id: the vehicle
        :return: True if the car was not involved in an accident, otherwise returns False.
        """
        # get all the accident status of the vehicle
        check_accident_query = f"SELECT Status FROM {self.vehicles_table_name} WHERE VehicleId = ? AND Status = ?"
        self.cur.execute(check_accident_query, (vehicle_id, "accident", ))
        vehicle_status = self.cur.fetchall()
        # if the list is not empty return false - if the car was in an accident return false
        return not vehicle_status

    def close_db(self):
        """
        Close the DB connection after using it
        :return:
        """
        self.conn.close()


# run the following commands only if this program is the main program (not imported)
if __name__ == "__main__":
    # for showing the useful functions I have written
    mobileye_db = MobileyeDatabase(DB_PATH)
    print(mobileye_db.vehicle_current_status("ebab5f787798416fb2b8afc1340d7a4e"))
    print(mobileye_db.object_latest_detections("ebab5f787798416fb2b8afc1340d7a4e"))
    print(mobileye_db.is_clean_record_car("ebae3f787798416fb2b8afc1340d7a6d"))
