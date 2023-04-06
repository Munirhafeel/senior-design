'''
Author: Munir Mohamed Hafeel
Date: 03-30-2023
'''

import re
import os
import keys #passwords
import paho.mqtt.client as mqtt
import logging  
from datetime import datetime
from pymongo import MongoClient
from statistics import mean, median, mode, variance, stdev

# logs
log_dir = 'cwd/logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_dir, 'sensor_data_extractor.log'), level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log(message, level=logging.INFO):
    print(message)
    logging.log(level, message)

class SensorDataExtractor:
    def __init__(self, db_uri):
        self.client = MongoClient(db_uri)
        self.db = self.client.ModelFarm2
        self.sensor_patterns = { 
            "temperature-humidity": {
                "pattern": r'Temperature: (\d+\.\d+), Humidity: (\d+\.\d+)',
                "metadata": {
                    "sensor": "DHT22",
                    "device": "Arduino-MKR-1010-A"
                },
                "data_keys": ["temperature", "humidity"],
                "records": []
            },
            "co2": {
                "pattern": r'CO2: (\d+)',
                "metadata": {
                    "sensor": "SCD40",
                    "device": "Arduino-MKR-1010-A"
                },
                "data_keys": ["co2"],
                "records": []
            },
            "light": {
                "pattern": r'Red: (\d+), Green: (\d+), Blue: (\d+), Clear: (\d+)',
                "metadata": {
                    "sensor": "SCD40",
                    "device": "Arduino-MKR-1010-A"
                },
                "data_keys": ["Red", "Green", "Blue", "Clear"],
                "records": []
            },
            "pH": {
                "pattern": r'pH: (\d+)',
                "metadata": {
                    "sensor": "Atlas-Scientific-Gravity-pH",
                    "device": "Arduino-MKR-1010-B"
                },
                "data_keys": ["pH"],
                "records": []
            },
            "conductivity": {
                "pattern": r'EC: (\d+)',
                "metadata": {
                    "sensor": "Atlas-Scientific-Conductivity",
                    "device": "Arduino-MKR-1010-B"
                },
                "data_keys": ["EC"],
                "records": []
            },
            "o2": {
                "pattern": r'O2: (\d+)',
                "metadata": {
                    "sensor": "Atlas-Scientific-Gravity-O2",
                    "device": "Arduino-MKR-1010-B"
                },
                "data_keys": ["o2"],
                "records": []
            }
        }

    def intercept_mqtt_data(self, mqtt_broker, mqtt_port, mqtt_topic):
        def on_connect(client, userdata, flags, rc):
            print(f"Connected with result code {str(rc)}")
            client.subscribe(mqtt_topic)

        def on_message(client, userdata, msg):
            try:
                line = msg.payload.decode()
                timestamp_pattern = r'(\d{4}-\d{2}-\d{2}, \d{2}:\d{2}:\d{2})'
                timestamp_match = re.search(timestamp_pattern, line)

                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    dt = datetime.strptime(timestamp, '%Y-%m-%d, %H:%M:%S')

                    for sensor, sensor_info in self.sensor_patterns.items():
                        match = re.search(sensor_info['pattern'], line)
                        if match:
                            record = {
                                "metadata": sensor_info['metadata'],
                                "timestamp": dt,
                                "data": {k: float(v) for k, v in zip(sensor_info['data_keys'], match.groups())}
                            }
                            self.insert_data([record], sensor, 1)

            except Exception as e:
                print(f"Error processing MQTT message: {e}")

        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        try:
            client.connect(mqtt_broker, mqtt_port, 60)
            client.loop_forever()
        except Exception as e:
            print(f"Error connecting to MQTT broker: {e}")

    def extract_data(self, file_path):
        with open(file_path, 'r') as file:
            for line in file:
                timestamp_pattern = r'(\d{4}-\d{2}-\d{2}, \d{2}:\d{2}:\d{2})'
                timestamp_match = re.search(timestamp_pattern, line)
                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    dt = datetime.strptime(timestamp,'%Y-%m-%d, %H:%M:%S')
                    for sensor, sensor_info in self.sensor_patterns.items():
                        match = re.search(sensor_info['pattern'],line)
                        if match:
                            record = {
                                "metadata": sensor_info['metadata'],
                                "timestamp": dt,
                                "data": {k: float(v) for k,v in zip(sensor_info['data_keys'], match.groups())}
                            }
                            sensor_info['records'].append(record)
        batch_size = 1000
        for sensor, sensor_info in self.sensor_patterns.items():
            self.insert_data(sensor_info['records'], sensor, batch_size)

    def insert_data(self, data, sensor_type, batch_size):
        date_groups = {}
        for record in data:
            dt = record['timestamp']
            date_str = dt.strftime('%Y-%m-%d')
            collection_name = f"{sensor_type}-{date_str}"
            if collection_name not in date_groups:
                date_groups[collection_name] = []

            date_groups[collection_name].append(record)

        for collection_name, records in date_groups.items():
            collection = self.db[collection_name]
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)

    def get_average_value(self, sensor_type, date_str, data_key):
        collection_name = f"{sensor_type}-{date_str}"
        collection = self.db[collection_name]
        pipeline = [
            {"$group": {"_id": None, "average": {"$avg": f"$data.{data_key}"}}}
        ]
        result = collection.aggregate(pipeline)
        average_value = list(result)[0]['average'] if result else None
        return average_value

    def get_highest_value(self, sensor_type, date_str, data_key):
        collection_name = f"{sensor_type}-{date_str}"
        collection = self.db[collection_name]
        pipeline = [
            {"$group": {"_id": None, "highest": {"$max": f"$data.{data_key}"}}}
        ]
        result = collection.aggregate(pipeline)
        highest_value = list(result)[0]['highest'] if result else None
        return highest_value

    def get_lowest_value(self, sensor_type, date_str, data_key):
        collection_name = f"{sensor_type}-{date_str}"
        collection = self.db[collection_name]
        pipeline = [
            {"$group": {"_id": None, "lowest": {"$min": f"$data.{data_key}"}}}
        ]
        result = collection.aggregate(pipeline)
        lowest_value = list(result)[0]['lowest'] if result else None
        return lowest_value

    def get_median(self, collection_name, data_key):
        collection = self.db[collection_name]
        data_points = list(collection.aggregate([
            {"$project": {data_key: f"$data.{data_key}"}},
            {"$sort": {data_key: 1}}
        ]))

        data_values = [dp[data_key] for dp in data_points]
        return median(data_values)

    def get_mode(self, collection_name, data_key):
        collection = self.db[collection_name]
        data_points = list(collection.aggregate([
            {"$project": {data_key: f"$data.{data_key}"}},
            {"$group": {"_id": f"${data_key}", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

        return data_points[0]['_id'] if data_points else None

    def get_standard_deviation(self, collection_name, data_key):
        collection = self.db[collection_name]
        data_points = list(collection.aggregate([
            {"$project": {data_key: f"$data.{data_key}"}}
        ]))

        data_values = [dp[data_key] for dp in data_points]
        return stdev(data_values)

    def get_percentiles(self, collection_name, data_key, percentiles=[25, 50, 75, 90]):
        collection = self.db[collection_name]
        data_points = list(collection.aggregate([
            {"$project": {data_key: f"$data.{data_key}"}},
            {"$sort": {data_key: 1}}
        ]))

        data_values = [dp[data_key] for dp in data_points]
        total_data_points = len(data_values)
        percentile_values = {}

        for percentile in percentiles:
            index = int(total_data_points * (percentile / 100))
            percentile_values[percentile] = data_values[index]

        return percentile_values

    def get_data_count(self, collection_name):
        collection = self.db[collection_name]
        return collection.count_documents({})

    def get_data_rate(self, collection_name):
        collection = self.db[collection_name]
        timestamps = list(collection.aggregate([
            {"$project": {"timestamp": 1}},
            {"$sort": {"timestamp": 1}}
        ]))

        if len(timestamps) < 2:
            return None

        start_time = timestamps[0]['timestamp']
        end_time = timestamps[-1]['timestamp']
        time_delta = (end_time - start_time).total_seconds()

        return len(timestamps) / time_delta if time_delta > 0 else None
    
    def filter_data(self, sensor_type, date_str, conditions):
        collection_name = f"{sensor_type}-{date_str}"
        collection = self.db[collection_name]
        filtered_data = collection.find(conditions)
        return list(filtered_data)
    
    def detect_anomalies(self, sensor_type, date_str, data_key, threshold=2):
        collection_name = f"{sensor_type}-{date_str}"
        mean_value = self.get_average_value(sensor_type, date_str, data_key)
        std_dev_value = self.get_standard_deviation(collection_name, data_key)

        lower_bound = mean_value - threshold * std_dev_value
        upper_bound = mean_value + threshold * std_dev_value

        collection = self.db[collection_name]
        anomaly_data = collection.find({"$or": [
            {f"data.{data_key}": {"$lt": lower_bound}},
            {f"data.{data_key}": {"$gt": upper_bound}},
        ]})

        return list(anomaly_data)

    def get_data_for_time(self, collection_name, time_str):
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        collection = self.db[collection_name]
        result = collection.find_one({"timestamp": dt})
        return result
    
    def get_latest_datapoint(self, collection_name):
        collection = self.db[collection_name]
        result = collection.find().sort("timestamp", -1).limit(1)
        return result[0] if collection.count_documents({}) > 0 else None

    
    def get_latest_collection(self, sensor_type):
        collection_names = self.db.list_collection_names()
        sensor_collections = sorted([name for name in collection_names if name.startswith(sensor_type)])

        if sensor_collections:
            return sensor_collections[-1]
        else:
            return None

    def magic(self, time_str=None):
        ideal = {
            "temperature_high": 27,
            "temperature_low": 20,
            "co2_high": 700,
            "co2_low": 350,
            "humidity_high": 70,
            "humidity_low": 40,
            "pH_high": 6.8,
            "pH_low": 5.3,
            "EC_high": 1800,
            "EC_low": 1200,
            "o2_high": 80,
            "o2_low": 35
        }
        if time_str == None:
            collections = {
                "temperature-humidity": self.get_latest_collection("temperature-humidity"),
                "co2": self.get_latest_collection("co2"),
                "pH": self.get_latest_collection("pH"),
                "EC": self.get_latest_collection("EC"),
                "o2": self.get_latest_collection("o2")
            }
            
            values = {}
            for sensor, collection_name in collections.items():
                if isinstance(collection_name, str):
                    latest_datapoint = self.get_latest_datapoint(collection_name)
                    if latest_datapoint:
                        for key, value in latest_datapoint["data"].items():
                            values[key] = value
                else:
                    print(f"Warning: Invalid collection name for sensor {sensor}: {collection_name}")

            total = 0
            for key, value in values.items():
                if ideal[f"{key}_low"] <= value <= ideal[f"{key}_high"]:
                    total += 1
                else:
                    total -= 1

            return total




    def close(self):
        self.client.close()

def run_mqtt():
    info = keys.run()
    db_uri = info.getauth("mongo")
    extractor = SensorDataExtractor(db_uri)
    mqtt_broker = "test.mosquitto.org"
    mqtt_port = 1883
    mqtt_topic = "sensor_data"

    try:
        log("Starting MQTT data interception")
        extractor.intercept_mqtt_data(mqtt_broker, mqtt_port, mqtt_topic)

    except Exception as e:
        log(f"Error running MQTT data interception: {e}", level=logging.ERROR)

    finally:
        log("Closing mqtt interception")
        extractor.close()

def run_extract():
    info = keys.run()
    db_uri = info.getauth("mongo")
    extractor = SensorDataExtractor(db_uri)

    path = info.getpath("logs")

    try:
        log("Starting data extraction")
        extractor.extract_data(path)

    except Exception as e:
        log(f"Error running data extraction: {e}", level=logging.ERROR)

    finally:
        log("Closing the extractor")
        extractor.close()

if __name__ == '__main__':
    run_extract()

'''if __name__ == '__main__':
    # run_mqtt()
    # run_extract()

    info = keys.run()
    db_uri = info.getauth("mongo")
    sensors = {
        "temperature-humidity": ["temperature", "humidity"],
        "light": ["red","green","blue","clear"],
        "co2": ["co2"]
        }
    date_str_list = [
        "2023-02-24",
        "2023-02-25",
        "2023-02-27",
        "2023-02-28",
        "2023-03-01",
        "2023-03-06",
        "2023-03-07",
        "2023-03-08",
        "2023-03-09",
        "2023-03-10",
        "2023-03-11",
        "2023-03-13",
        "2023-03-18"
    ]
    extractor = SensorDataExtractor(db_uri)
    magic_value = extractor.magic()

    for sensor_type, data_keys in sensors.items():
        for date_str in date_str_list:
            collection_name = f"{sensor_type}-{date_str}"
            for data_key in data_keys:
                print("Collection: {%s}, Data: {%s}", collection_name, data_key)
                average_value = extractor.get_average_value(sensor_type, date_str, data_key)
                highest_value = extractor.get_highest_value(sensor_type, date_str, data_key)
                lowest_value = extractor.get_lowest_value(sensor_type, date_str, data_key)
                median_value = extractor.get_median(collection_name, data_key)
                mode_value = extractor.get_mode(collection_name, data_key)
                std_dev_value = extractor.get_standard_deviation(collection_name, data_key)
                percentiles = extractor.get_percentiles(collection_name, data_key)
                data_count = extractor.get_data_count(collection_name)
                data_rate = extractor.get_data_rate(collection_name)
                print(f"Average value: {average_value}")
                print(f"Highest value: {highest_value}")
                print(f"Lowest value: {lowest_value}")
                print(f"Median value: {median_value}")
                print(f"Mode value: {mode_value}")
                print(f"Standard deviation: {std_dev_value}")
                print(f"Percentiles: {percentiles}")
                print(f"Data count: {data_count}")
                print(f"Data rate: {data_rate}")
        magic_value = extractor.magic()
    print("magic: ", magic_value)
    extractor.close()
'''