#include <PubSubClient.h>

// WiFi credentials
const char* ssid = "CanesGuest";

// MQTT Broker credentials
const char* mqtt_server = "test.mosquitto.org";
const char* mqtt_topic = "fantopic";

// Relay pin


int relayPin1 = 0; //pump A
int relayPin2 = 1; //pump B
int relayPin3 = 2; //pump C
int relayPin4 = 3; //pump D
int relayPin5 = 4; //fan 1
int relayPin6 = 5; //fan 2


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
  pinMode(relayPin1, OUTPUT);
  pinMode(relayPin2, OUTPUT);
  pinMode(relayPin3, OUTPUT);
  pinMode(relayPin4, OUTPUT);
  pinMode(relayPin5, OUTPUT);
  pinMode(relayPin6, OUTPUT);

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


    Serial.print("Message arrived [");
    Serial.print(topic);
    Serial.print("] ");
    
    // Allocate memory for the payload_char with length+1 to accommodate the null terminator
    char payload_char[length + 1];
    
    // Copy the payload into payload_char and add the null terminator
    for (int i = 0; i < length; i++) {
        payload_char[i] = (char)payload[i];
        Serial.print((char)payload[i]);
    }
    payload_char[length] = '\0'; // Null terminator
    Serial.println();
    
    Serial.println("Message received: " + String(topic));
    Serial.println("Payload: " + String(payload_char));

    if (strcmp(payload_char,"pump1_on") == 0)
    {   
        digitalWrite(relayPin1, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"pump1_off") == 0)
    {   
        digitalWrite(relayPin1, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"pump2_on") == 0)
    {   
        digitalWrite(relayPin2, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"pump2_off") == 0)
    {   
        digitalWrite(relayPin2, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"pump3_on") == 0)
    {   
        digitalWrite(relayPin3, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"pump3_off") == 0)
    {   
        digitalWrite(relayPin3, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"pump4_on") == 0)
    {   
        digitalWrite(relayPin4, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"pump4_off") == 0)
    {   
        digitalWrite(relayPin4, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"fan1_on") == 0)
    {   
        digitalWrite(relayPin5, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"fan1_off") == 0)
    {   
        digitalWrite(relayPin5, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"fan2_on") == 0)
    {   
        digitalWrite(relayPin6, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"fan2_off") == 0)
    {   
        digitalWrite(relayPin6, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"sample7on") == 0)
    {   
        digitalWrite(relayPin7, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"sample7off") == 0)
    {   
        digitalWrite(relayPin7, LOW);
        Serial.println("Device 1 turned off");
    }
    else if (strcmp(payload_char,"sample8on") == 0)
    {   
        digitalWrite(relayPin8, HIGH);
        Serial.println("Device 1 turned on");
    }
    else if (strcmp(payload_char,"sample8off") == 0)
    {   
        digitalWrite(relayPin8, LOW);
        Serial.println("Device 1 turned off");
    }
    else
    {
        Serial.println("Payload not recognized");
    }

}


