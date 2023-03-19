import re
from datetime import datetime
from pymongo import MongoClient
import keys


class SensorDataExtractor:
    def __init__(self, db_uri):
        self.client = MongoClient(db_uri)
        self.db = self.client.ModelFarm

    def extract_data(self, file_path):
        with open(file_path, 'r') as file:
            for line in file:
                timestamp_pattern = r'(\d{4}-\d{2}-\d{2}, \d{2}:\d{2}:\d{2})'
                timestamp_match = re.search(timestamp_pattern, line)

                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    dt = datetime.strptime(timestamp, '%Y-%m-%d, %H:%M:%S')

                    temp_humid_pattern = r'Temperature: (\d+\.\d+), Humidity: (\d+\.\d+)'
                    co2_pattern = r'CO2: (\d+)'
                    light_pattern = r'Red: (\d+), Green: (\d+), Blue: (\d+), Clear: (\d+)'

                    temp_humid_match = re.search(temp_humid_pattern, line)
                    co2_match = re.search(co2_pattern, line)
                    light_match = re.search(light_pattern, line)

                    # we partition data based on time 
                    # one collection per group of data, per day.
                    if temp_humid_match:
                        collection_name = 'temperature-humidity-' + dt.strftime('%Y-%m-%d')
                        collection = self.db[collection_name]

                        record = {
                            "metadata": {
                                "sensor": "DHT22",
                                "device": "Arduino-MKR-1010-A"
                            },
                            "timestamp": dt,
                            "data": {
                                "temperature": float(temp_humid_match.group(1)),
                                "humidity": float(temp_humid_match.group(2))
                            }
                        }

                        collection.insert_one(record)

                    elif co2_match:
                        collection_name = 'co2-' + dt.strftime('%Y-%m-%d')
                        collection = self.db[collection_name]

                        record = {
                            "metadata": {
                                "sensor": "SCD40",
                                "device": "Arduino-MKR-1010-A"
                            },
                            "timestamp": dt,
                            "data": {
                                "co2": float(co2_match.group(1))
                            }
                        }

                        collection.insert_one(record)

                    elif light_match:
                        collection_name = 'light-' + dt.strftime('%Y-%m-%d')
                        collection = self.db[collection_name]

                        record = {
                            "metadata": {
                                "sensor": "APDS9960",
                                "device": "Arduino-MKR-1010-A"
                            },
                            "timestamp": dt,
                            "data": {
                                "red": float(light_match.group(1)),
                                "green": float(light_match.group(2)),
                                "blue": float(light_match.group(3)),
                                "clear": float(light_match.group(4))
                            }
                        }

                        collection.insert_one(record)

    def close(self):
        self.client.close()


def run():
    info = keys.run()
    db_uri = info.getauth("mongo")
    extractor = SensorDataExtractor(db_uri)
    path = info.getpath("logs")
    extractor.extract_data(path)
    extractor.close()


if __name__ == '__main__':
    run()
