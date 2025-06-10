import csv
from faker import Faker
import random

fake = Faker()

regions = [
    "Tanger-Tétouan-Al Hoceïma",
    "Oriental",
    "Fès-Meknès",
    "Rabat-Salé-Kénitra",
    "Béni Mellal-Khénifra",
    "Casablanca-Settat",
    "Marrakech-Safi",
    "Drâa-Tafilalet",
    "Souss-Massa",
    "Guelmim-Oued Noun",
    "Laâyoune-Sakia El Hamra",
    "Dakhla-Oued Ed-Dahab"
]

rate_plans = ["Starter", "Premium", "Infinity"]

def generate_moroccan_phone():
    prefix = random.choice(["2126", "2127"])
    suffix = "".join(str(random.randint(0, 9)) for _ in range(8))
    return prefix + suffix

# Génération des données
data = []
for _ in range(2500):
    customer_id = generate_moroccan_phone()
    activation_date = fake.date_between(start_date='-2y', end_date='today')
    subscription_type = "postpaid"
    status = "active"
    region = random.choice(regions)
    rate_plan_id = random.choice(rate_plans)
    data.append([
        customer_id,
        activation_date,
        subscription_type,
        status,
        region,
        rate_plan_id
    ])

# Écriture dans un fichier CSV
with open("customer_subscriptions.csv", mode="w", newline='', encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["customer_id", "activation_date", "subscription_type", "status", "region", "rate_plan_id"])
    writer.writerows(data)

print("✅ Données générées et enregistrées dans 'customer_subscriptions.csv'")
