from flask import Flask, render_template, request
import paho.mqtt.client as mqtt

app = Flask(__name__)

# MQTT Broker credentials
mqtt_server = "test.mosquitto.org"
mqtt_topic = "fantopic"

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_server, 1883)

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/device', methods=['POST'])
def fan():
  fan_state = request.form['device_state']
  mqtt_client.publish(mqtt_topic, fan_state)
  return render_template('index.html')

if __name__ == '__main__':
  app.run(debug=True)
