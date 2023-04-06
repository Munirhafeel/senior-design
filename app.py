from flask import Flask, render_template, request, redirect, url_for
import paho.mqtt.client as mqtt
import db
from bson.decimal128 import Decimal128


app = Flask(__name__)

devices = [
    {"id": "pump1", "name": "Pump 1"},
    {"id": "pump2", "name": "Pump 2"},
    {"id": "pump3", "name": "Pump 3"},
    {"id": "pump4", "name": "Pump 4"},
    {"id": "fan1", "name": "Fan 1"},
    {"id": "fan2", "name": "Fan 2"},
]

# MQTT Broker credentials
mqtt_server = "100.64.74.12"
mqtt_topic = "fantopic"

# Initialize SensorDataExtractor
info = db.keys.run()
db_uri = info.getauth("mongo")
extractor = db.SensorDataExtractor(db_uri)

def on_connect(client, userdata, flags, rc):
    client.subscribe("temp_humid")
    client.subscribe("APDS")
    client.subscribe("SCD40")
    client.subscribe("TDS")
    client.subscribe("pH")
    client.subscribe("DO")
    client.subscribe("EC")
    get_latest_values()

def on_message(client, userdata, msg):
    incoming_message = msg.payload.decode()
    extractor.insert_decoded_message(incoming_message)

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(mqtt_server, 1883)

# Start the MQTT loop in the background
mqtt_client.loop_start()

def get_latest_values():
  sensor_types = ["temperature-humidity", "co2", "light", "pH", "conductivity", "o2"]

  latest_data = {}
  for sensor_type in sensor_types:
      latest_collection = extractor.get_latest_collection(sensor_type)
      if latest_collection:
          latest_datapoint = extractor.get_latest_datapoint(latest_collection)
          if latest_datapoint:
              for key, value in latest_datapoint["data"].items():
                  # Convert the value to a float if it's stored as a Decimal128 object in MongoDB
                  if isinstance(value, Decimal128):
                      value = value.to_decimal().to_float_lossy()
                  latest_data[key] = value
                  print(key, value)




@app.route('/')
def index():
    return render_template('index.html', devices=devices)

@app.route('/device', methods=['POST'])
def fan():
    fan_state = request.form['device_state']
    print(fan_state)
    mqtt_client.publish(mqtt_topic, fan_state)
    return redirect(url_for('index'))

if __name__ == '__main__':
    try:
        app.run(debug=True)
    finally:
        # Stop the MQTT loop when the Flask app stops
        mqtt_client.loop_stop()
