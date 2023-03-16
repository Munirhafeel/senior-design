#include <WiFiNINA.h>
#include <PubSubClient.h>

// WiFi credentials
const char* ssid = "CanesGuest";

// MQTT Broker credentials
const char* mqtt_server = "test.mosquitto.org";
const char* mqtt_topic = "fantopic";

// Relay pin
int relayPin = 5;

// Initialize WiFi client and MQTT client
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

void setup() {
  // Initialize serial port
  Serial.begin(9600);

  // Connect to WiFi
  Serial.print("Connecting to ");
  Serial.println(ssid);
  WiFi.begin(ssid);
  while (WiFi.status() != WL_CONNECTED) {
    delay(1000);
    Serial.println("Connecting to WiFi...");
  }
  Serial.println("Connected to WiFi");

  // Connect to MQTT broker
  mqttClient.setServer(mqtt_server, 1883);
  while (!mqttClient.connected()) {
    Serial.println("Connecting to MQTT Broker...");
    if (mqttClient.connect("ArduinoMKR1010")) {
      Serial.println("Connected to MQTT Broker");
    } else {
      Serial.println("Failed to connect to MQTT Broker");
      delay(1000);
    }
  }

  // Set relay pin as output
  pinMode(relayPin, OUTPUT);

  // Subscribe to MQTT topic and register callback function
  mqttClient.setCallback(callback);
  mqttClient.subscribe(mqtt_topic);
}

void loop() {
  // Reconnect to WiFi if connection is lost
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("WiFi connection lost, reconnecting...");
    WiFi.begin(ssid);
    while (WiFi.status() != WL_CONNECTED) {
      delay(1000);
      Serial.println("Connecting to WiFi...");
    }
    Serial.println("Reconnected to WiFi");
  }

  // Reconnect to MQTT broker if connection is lost
  if (!mqttClient.connected()) {
    Serial.println("MQTT connection lost, reconnecting...");
    if (mqttClient.connect("ArduinoMKR1010")) {
      Serial.println("Reconnected to MQTT Broker");
      mqttClient.subscribe(mqtt_topic);
    } else {
      Serial.println("Failed to reconnect to MQTT Broker");
    }
  }

  // Process MQTT messages
  mqttClient.loop();
}

// MQTT message handler
void callback(char* topic, byte* payload, unsigned int length) {
  Serial.println("Message received: " + String(topic));
  Serial.println("Payload: " + String((char*)payload));

  // Check if message is "on" or "off"
  if ((strcmp((char*)payload, "onpic") == 0)||(strcmp((char*)payload, "onfic") == 0)) {
    digitalWrite(relayPin, HIGH);
    Serial.println("Fan turned on");
  } else if (strcmp((char*)payload, "offic") == 0) {
    digitalWrite(relayPin, LOW);
    Serial.println("Fan turned off");
  }
}


