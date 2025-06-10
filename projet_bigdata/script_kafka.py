from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
from data_creation import generate_cdr  # Assure-toi que ce module est disponible

faker = Faker()

# Nouvelle configuration Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

producteur_kafka = Producer(**kafka_config)

# Fonction de rappel pour la livraison
def rapport_livraison(erreur, message):
    if erreur is not None:
        print(f"❌ Erreur lors de l'envoi: {erreur}")
    else:
        print(f"✅ Message livré à {message.topic()} [partition {message.partition()}]")

# Nom du topic
nom_topic = 'topic'

try:
    while True:
        # Envoi de 3 messages par itération
        for _ in range(3):
            enregistrement_cdr = generate_cdr()
            cle_message = str(faker.random_int(min=1, max=1000))
            valeur_message = json.dumps(enregistrement_cdr)

            producteur_kafka.produce(
                nom_topic,
                key=cle_message,
                value=valeur_message,
                callback=rapport_livraison
            )

            producteur_kafka.poll(0)

        time.sleep(1.75)  # Pause entre chaque lot de messages
except KeyboardInterrupt:
    print("🛑 Interruption du producteur Kafka")
finally:
    producteur_kafka.flush()
