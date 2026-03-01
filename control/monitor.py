from argparse import ArgumentError
import ssl
from django.db.models import Avg, Max, Min, Count
from datetime import timedelta
from django.utils import timezone
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings

# Initialized without connecting — setup_mqtt() handles the full setup
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def detect_sudden_changes():
    """Detecta cambios bruscos comparando el promedio reciente vs referencia."""
    threshold = getattr(settings, 'SUDDEN_CHANGE_THRESHOLD', 20)
    now = timezone.now()
    two_min_ago = now - timedelta(minutes=2)
    five_min_ago = now - timedelta(minutes=5)

    # Fetch current and previous hour rows to handle window spanning hour boundary
    current_base = now.replace(minute=0, second=0, microsecond=0)
    prev_base = current_base - timedelta(hours=1)
    rows = list(
        Data.objects.filter(base_time__in=[prev_base, current_base])
            .select_related('station__user',
                            'station__location__city',
                            'station__location__state',
                            'station__location__country',
                            'measurement')
    )

    def compute_avg(start, end):
        """Average individual readings whose timestamp falls in [start, end)."""
        groups = {}
        for row in rows:
            key = (row.station_id, row.measurement_id)
            vals = [
                v for v, t in zip(row.values, row.times)
                if start <= row.base_time + timedelta(seconds=t) < end
            ]
            if vals:
                entry = groups.setdefault(key, {'vals': [], 'row': row})
                entry['vals'].extend(vals)
        return {
            key: {'avg': sum(d['vals']) / len(d['vals']), 'row': d['row']}
            for key, d in groups.items()
        }

    recent = compute_avg(two_min_ago, now)
    reference = compute_avg(five_min_ago, two_min_ago)

    # Debug
    print(f"Recent data count: {len(recent)}")
    print(f"Reference data count: {len(reference)}")
    print(f"Recent dict: {recent}")
    print(f"Reference dict: {reference}")

    sudden_changes = 0
    for key, ref in reference.items():
        rec = recent.get(key)
        if not rec or ref['avg'] == 0 or rec['avg'] is None:
            continue

        change = abs((rec['avg'] - ref['avg']) / ref['avg'] * 100)
        if change <= threshold:
            continue

        row = ref['row']
        topic = '{}/{}/{}/{}/in'.format(
            row.station.location.country.name,
            row.station.location.state.name,
            row.station.location.city.name,
            row.station.user.username,
        )
        message = "SUDDEN_CHANGE {} {:.2f} {:.2f} {:.1f}%".format(
            row.measurement.name, ref['avg'], rec['avg'], change
        )
        print(f"Cambio brusco detectado: {row.measurement.name} {ref['avg']:.2f} -> {rec['avg']:.2f} ({change:.1f}%)")
        client.publish(topic, message)
        sudden_changes += 1

    print(f"{sudden_changes} cambios bruscos detectados")

def analyze_data():
    print("Calculando alertas...")

    data = Data.objects.filter(
        base_time__gte=timezone.now() - timedelta(hours=1))
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
            print(timezone.now(), "Sending alert to {} {}".format(topic, variable))
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
