#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <DHT.h>

// Configuración de la pantalla OLED
#define SCREEN_WIDTH 128
#define SCREEN_HEIGHT 64
#define OLED_RESET -1
Adafruit_SSD1306 display(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);

// Pin para LED de advertencia
#define LED_WARNING_PIN D7  // LED rojo para advertir cambios bruscos
#define LED_ALERT_PIN D6    // LED amarillo para alertas normales

// Pin para el sensor DHT11/DHT22
#define DHTPIN D4
#define DHTTYPE DHT11
DHT dht(DHTPIN, DHTTYPE);

// TODO: Configurar WiFi
const char* ssid = "TU_WIFI";
const char* password = "TU_PASSWORD";

// TODO: Configurar MQTT
const char* mqtt_server = "IP_BROKER_MQTT";
const int mqtt_port = 8082;
const char* mqtt_user = "tu_usuario";
const char* mqtt_password = "tu_password";

// TODO: Configurar tópicos
const char* topic_out = "pais/estado/ciudad/usuario/out";  // Para enviar datos
const char* topic_in = "pais/estado/ciudad/usuario/in";    // Para recibir comandos

WiFiClient espClient;
PubSubClient client(espClient);

unsigned long lastMsg = 0;
const long interval = 60000; // Enviar datos cada 60 segundos

void setup() {
    Serial.begin(115200);
    
    // Inicializar pines
    pinMode(LED_WARNING_PIN, OUTPUT);
    pinMode(LED_ALERT_PIN, OUTPUT);
    digitalWrite(LED_WARNING_PIN, LOW);
    digitalWrite(LED_ALERT_PIN, LOW);
    
    // Inicializar pantalla OLED
    if(!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
        Serial.println(F("SSD1306 allocation failed"));
        for(;;);
    }
    display.clearDisplay();
    display.setTextColor(WHITE);
    
    // Inicializar sensor DHT
    dht.begin();
    
    // Conectar WiFi
    setup_wifi();
    
    // Configurar MQTT
    client.setServer(mqtt_server, mqtt_port);
    client.setCallback(callback);
}

void setup_wifi() {
    delay(10);
    Serial.println();
    Serial.print("Conectando a ");
    Serial.println(ssid);
    
    WiFi.begin(ssid, password);
    
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    
    Serial.println("");
    Serial.println("WiFi conectado");
    Serial.println("IP address: ");
    Serial.println(WiFi.localIP());
}

void reconnect() {
    while (!client.connected()) {
        Serial.print("Intentando conexión MQTT...");
        
        if (client.connect("ESP8266Client", mqtt_user, mqtt_password)) {
            Serial.println("conectado");
            // Suscribirse al tópico para recibir comandos
            client.subscribe(topic_in);
            Serial.print("Suscrito a: ");
            Serial.println(topic_in);
        } else {
            Serial.print("falló, rc=");
            Serial.print(client.state());
            Serial.println(" intentar nuevamente en 5 segundos");
            delay(5000);
        }
    }
}

/*
 * Callback para procesar mensajes MQTT recibidos
 * Procesa dos tipos de mensajes:
 * 1. ALERT: Alerta cuando se exceden los límites (evento original)
 * 2. SUDDEN_CHANGE: Alerta cuando hay un cambio brusco (nuevo evento)
 */
void callback(char* topic, byte* payload, unsigned int length) {
    Serial.print("Mensaje recibido [");
    Serial.print(topic);
    Serial.print("] ");
    
    // Convertir payload a String
    String message = "";
    for (int i = 0; i < length; i++) {
        message += (char)payload[i];
    }
    Serial.println(message);
    
    // NUEVO: Procesar mensaje de CAMBIO BRUSCO
    if (message.startsWith("SUDDEN_CHANGE")) {
        processSuddenChange(message);
    }
    // Procesar mensaje de ALERTA (evento original)
    else if (message.startsWith("ALERT")) {
        processAlert(message);
    }
}

/*
 * NUEVA FUNCIÓN: Procesar cambio brusco
 * Formato del mensaje: SUDDEN_CHANGE <variable> <valor_ref> <valor_actual> <cambio%>
 * Ejemplo: "SUDDEN_CHANGE Temperatura 22.00 28.00 27.3%"
 */
void processSuddenChange(String message) {
    Serial.println("⚠️ CAMBIO BRUSCO DETECTADO");
    
    // Parsear el mensaje
    int firstSpace = message.indexOf(' ');
    int secondSpace = message.indexOf(' ', firstSpace + 1);
    int thirdSpace = message.indexOf(' ', secondSpace + 1);
    int fourthSpace = message.indexOf(' ', thirdSpace + 1);
    
    String variable = message.substring(firstSpace + 1, secondSpace);
    String valorRef = message.substring(secondSpace + 1, thirdSpace);
    String valorActual = message.substring(thirdSpace + 1, fourthSpace);
    String cambio = message.substring(fourthSpace + 1);
    
    // Encender LED de advertencia (rojo)
    digitalWrite(LED_WARNING_PIN, HIGH);
    
    // Mostrar en pantalla OLED
    display.clearDisplay();
    display.setTextSize(1);
    display.setCursor(0, 0);
    display.println("CAMBIO BRUSCO!");
    display.println("");
    display.setTextSize(1);
    display.print("Variable: ");
    display.println(variable);
    display.println("");
    display.print("Anterior: ");
    display.println(valorRef);
    display.print("Actual: ");
    display.println(valorActual);
    display.print("Cambio: ");
    display.println(cambio);
    display.display();
    
    // Parpadear LED 5 veces
    for(int i = 0; i < 5; i++) {
        digitalWrite(LED_WARNING_PIN, HIGH);
        delay(200);
        digitalWrite(LED_WARNING_PIN, LOW);
        delay(200);
    }
    
    // Mantener LED encendido por 30 segundos
    digitalWrite(LED_WARNING_PIN, HIGH);
    delay(30000);
    digitalWrite(LED_WARNING_PIN, LOW);
    
    // Volver a mostrar pantalla normal
    displayNormal();
}

/*
 * Procesar alerta de límites excedidos (evento original)
 * Formato del mensaje: ALERT <variable> <min> <max>
 * Ejemplo: "ALERT Temperatura 18 24"
 */
void processAlert(String message) {
    Serial.println("🚨 ALERTA: Límites excedidos");
    
    // Parsear el mensaje
    int firstSpace = message.indexOf(' ');
    int secondSpace = message.indexOf(' ', firstSpace + 1);
    int thirdSpace = message.indexOf(' ', secondSpace + 1);
    
    String variable = message.substring(firstSpace + 1, secondSpace);
    String minValue = message.substring(secondSpace + 1, thirdSpace);
    String maxValue = message.substring(thirdSpace + 1);
    
    // Encender LED de alerta (amarillo)
    digitalWrite(LED_ALERT_PIN, HIGH);
    
    // Mostrar en pantalla OLED
    display.clearDisplay();
    display.setTextSize(1);
    display.setCursor(0, 0);
    display.println("ALERTA!");
    display.println("");
    display.print("Variable: ");
    display.println(variable);
    display.println("");
    display.print("Limites: ");
    display.print(minValue);
    display.print(" - ");
    display.println(maxValue);
    display.display();
    
    // Mantener LED encendido por 20 segundos
    delay(20000);
    digitalWrite(LED_ALERT_PIN, LOW);
    
    // Volver a mostrar pantalla normal
    displayNormal();
}

/*
 * Mostrar pantalla normal con última medición
 */
void displayNormal() {
    float temperature = dht.readTemperature();
    float humidity = dht.readHumidity();
    
    display.clearDisplay();
    display.setTextSize(1);
    display.setCursor(0, 0);
    display.println("Sistema IoT REMA");
    display.println("");
    display.setTextSize(2);
    display.print("T: ");
    display.print(temperature, 1);
    display.println(" C");
    display.print("H: ");
    display.print(humidity, 1);
    display.println(" %");
    display.display();
}

/*
 * Enviar datos de sensores al servidor
 */
void sendSensorData() {
    float temperature = dht.readTemperature();
    float humidity = dht.readHumidity();
    
    // Verificar si las lecturas son válidas
    if (isnan(temperature) || isnan(humidity)) {
        Serial.println("Error leyendo el sensor DHT!");
        return;
    }
    
    // Crear mensaje JSON
    String payload = "{\"Temperatura\":";
    payload += temperature;
    payload += ",\"Humedad\":";
    payload += humidity;
    payload += "}";
    
    // Publicar en el tópico
    if (client.publish(topic_out, payload.c_str())) {
        Serial.println("Datos enviados: " + payload);
        displayNormal();
    } else {
        Serial.println("Error al enviar datos");
    }
}

void loop() {
    // Mantener conexión MQTT
    if (!client.connected()) {
        reconnect();
    }
    client.loop();
    
    // Enviar datos cada 60 segundos
    unsigned long now = millis();
    if (now - lastMsg > interval) {
        lastMsg = now;
        sendSensorData();
    }
}
