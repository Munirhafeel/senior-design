from gevent import monkey
monkey.patch_all()
from flask import Flask, render_template, request, redirect, url_for
import paho.mqtt.client as mqtt
import db
import threading
from gevent.pywsgi import WSGIServer



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


def populate_database():
    while True:
        db.run_extract()


def write_live():
    db.run_mqtt()


def run_write_live_continuously():
    while True:
        write_live()


@app.route('/')
def index():
    return render_template('index.html', devices=devices)


@app.route('/device', methods=['POST'])
def fan():
    fan_state = request.form['device_state']
    mqtt_client.publish(mqtt_topic, fan_state)
    return redirect(url_for('index'))


@app.route('/dashboard', methods=['POST'])
def dashboard():
    pass


def run_flask_app():
    http_server = WSGIServer(('', 8080), app)
    http_server.serve_forever()

if __name__ == '__main__':
    flask_app_thread = threading.Thread(target=run_flask_app)
    flask_app_thread.start()
    write_live_thread = threading.Thread(target=write_live)
    write_live_thread.start()

    populate_db_thread = threading.Thread(target=populate_database)
    populate_db_thread.start()

