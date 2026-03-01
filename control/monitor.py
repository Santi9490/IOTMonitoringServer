from argparse import ArgumentError
import ssl
from django.db.models import Avg, Max, Min, Count
from datetime import timedelta, datetime
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings

# Initialized without connecting — setup_mqtt() handles the full setup
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def detect_sudden_changes():
    """
    Nuevo evento: Detecta cambios bruscos en las mediciones.
    
    Condición: Compara el promedio de las mediciones de los últimos 30 minutos
    con el promedio de las mediciones de las 2 horas anteriores.
    Si la diferencia es mayor al umbral configurado (por ejemplo, 20%), 
    se considera un cambio brusco.
    
    Acción: Envía un comando al dispositivo IoT para ejecutar una acción en el actuador
    (por ejemplo, encender un LED, mostrar mensaje en pantalla, etc.)
    """
    print("Detectando cambios bruscos en las mediciones...")
    
    # Umbral de cambio porcentual que se considera "brusco"
    # Se ajusta este valor en settings.py
    threshold_percentage = getattr(settings, 'SUDDEN_CHANGE_THRESHOLD', 20)
    
    # Obtener datos de los últimos 30 minutos (periodo reciente)
    recent_time = datetime.now() - timedelta(minutes=30)
    recent_data = Data.objects.filter(base_time__gte=recent_time)
    
    # Obtener datos de las 2 horas anteriores (periodo de referencia)
    reference_start = datetime.now() - timedelta(hours=2, minutes=30)
    reference_end = datetime.now() - timedelta(minutes=30)
    reference_data = Data.objects.filter(
        base_time__gte=reference_start,
        base_time__lt=reference_end
    )
    
    # Agrupar por estación y variable
    recent_aggregation = recent_data.annotate(recent_avg=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('recent_avg', 'station__id', 'measurement__id',
                'station__user__username',
                'measurement__name',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    
    # Crear diccionario para búsqueda rápida de datos recientes
    recent_dict = {}
    for item in recent_aggregation:
        key = (item['station__id'], item['measurement__id'])
        recent_dict[key] = item
    
    # Comparar con datos de referencia
    reference_aggregation = reference_data.annotate(reference_avg=Avg('avg_value')) \
        .values('reference_avg', 'station__id', 'measurement__id')
    
    sudden_changes = 0
    for ref_item in reference_aggregation:
        key = (ref_item['station__id'], ref_item['measurement__id'])
        
        if key in recent_dict:
            recent_item = recent_dict[key]
            recent_avg = recent_item['recent_avg']
            reference_avg = ref_item['reference_avg']
            
            # Evitar división por cero
            if reference_avg != 0:
                # Calcular cambio porcentual
                percentage_change = abs(
                    ((recent_avg - reference_avg) / reference_avg) * 100
                )
                
                # Si el cambio supera el umbral, enviar alerta
                if percentage_change > threshold_percentage:
                    variable = recent_item['measurement__name']
                    country = recent_item['station__location__country__name']
                    state = recent_item['station__location__state__name']
                    city = recent_item['station__location__city__name']
                    user = recent_item['station__user__username']
                    
                    # Formato del mensaje: SUDDEN_CHANGE <variable> <valor_referencia> <valor_actual> <cambio%>
                    message = "SUDDEN_CHANGE {} {:.2f} {:.2f} {:.1f}%".format(
                        variable, reference_avg, recent_avg, percentage_change
                    )
                    topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
                    
                    print(datetime.now(), "Cambio brusco detectado en {} para {}: {:.1f}%".format(
                        user, variable, percentage_change))
                    print(datetime.now(), "Enviando comando a {}".format(topic))
                    
                    client.publish(topic, message)
                    sudden_changes += 1
    
    print("{} cambios bruscos detectados".format(sudden_changes))

def analyze_data():
    print("Calculando alertas...")

    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alert = True

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
            client.publish(topic, message)
            alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")


def on_connect(client, userdata, flags, rc, properties=None):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))

def on_disconnect(client: mqtt.Client, userdata, flags, rc, properties=None):
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()

def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''
    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        # Use VERSION2 API and pass client_id separately from credentials
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB, settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos.
    - analyze_data: Verifica si las mediciones están fuera de los límites establecidos
    - detect_sudden_changes: Detecta cambios bruscos en las mediciones
    '''
    print("Iniciando cron...")
    
    # Ejecutar analyze_data cada 5 minutos (evento original)
    schedule.every(5).minutes.do(analyze_data)
    
    # Ejecutar detect_sudden_changes cada 10 minutos (nuevo evento)
    schedule.every(2).minutes.do(detect_sudden_changes)
    
    print("Servicio de control iniciado")
    print("- Análisis de límites: cada 5 minutos")
    print("- Detección de cambios bruscos: cada 2 minutos")
    
    while 1:
        schedule.run_pending()
        time.sleep(1)
