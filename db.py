import re
from datetime import datetime
from pymongo import MongoClient
import keys


class SensorDataExtractor:
    def __init__(self, db_uri):
        self.client = MongoClient(db_uri)
        self.db = self.client.ModelFarm

        # create empty lists to store data
        self.temperature_humidity_data = []
        self.co2_data = []
        self.light_data = []

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

                    if temp_humid_match:
                        # accumulate data for temperature/humidity
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
                        self.temperature_humidity_data.append(record)

                    elif co2_match:
                        # accumulate data for CO2
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
                        self.co2_data.append(record)

                    elif light_match:
                        # accumulate data for light
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
                        self.light_data.append(record)

        # insert the data in batches of 1000
        batch_size = 1000
        self.insert_data(self.temperature_humidity_data, 'temperature-humidity-', batch_size)
        self.insert_data(self.co2_data, 'co2-', batch_size)
        self.insert_data(self.light_data, 'light-', batch_size)

    def insert_data(self, data, collection_prefix, batch_size):
    # group the data by date and insert in batches
        date_groups = {}
        for record in data:
            dt = record['timestamp']
            date_str = dt.strftime('%Y-%m-%d')
            collection_name = collection_prefix + date_str

            if collection_name not in date_groups:
                date_groups[collection_name] = []

            date_groups[collection_name].append(record)

        for collection_name, records in date_groups.items():
            collection = self.db[collection_name]
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                collection.insert_many(batch)

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
