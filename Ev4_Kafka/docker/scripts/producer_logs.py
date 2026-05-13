import time
from kafka import KafkaProducer
import sys

print("--- INICIANDO PRODUCER PARA AA4 ---")
try:
    # 'kafka' es el nombre del servicio en tu docker-compose
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(0, 10, 1)
    )
    topic = 'topic_logs'
    
    # Ruta donde Docker mapea tus datos
    with open('/tmp/data/logs_acceso.txt', 'r') as f:
        for line in f:
            mensaje = line.strip()
            print(f"Enviando a Kafka: {mensaje}")
            producer.send(topic, mensaje.encode('utf-8'))
            time.sleep(2) # Pausa de 2 seg para simular tiempo real
            
except Exception as e:
    print(f"ERROR: {e}")