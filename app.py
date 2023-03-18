from flask import Flask, render_template, request, redirect, url_for
import paho.mqtt.client as mqtt

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
mqtt_server = "test.mosquitto.org"
mqtt_topic = "fantopic"

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_server, 1883)

@app.route('/')
def index():
  return render_template('index.html', devices=devices)

@app.route('/device', methods=['POST'])
def fan():
  fan_state = request.form['device_state']
  mqtt_client.publish(mqtt_topic, fan_state)
  return redirect(url_for('index'))

if __name__ == '__main__':
  app.run(debug=True)
