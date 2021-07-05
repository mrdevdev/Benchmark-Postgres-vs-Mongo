from faker import Faker
from faker_vehicle import VehicleProvider
from pymongo import MongoClient
import psycopg2
import json


def setUsername(name):
    nameSplitted = name.split()
    return nameSplitted[0][0] + nameSplitted[1]


def setMail(username, mail):
    mailSplitted = mail.split('@')
    return username + '@' + mailSplitted[1]


fake = Faker()
fake.add_provider(VehicleProvider)

dataset = []

for x in range(10):  # 100k en 5 minutos
    company = {}
    company['name'] = fake.company()
    company['suffix'] = fake.company_suffix()
    company['address'] = fake.address()
    company['companyCar'] = fake.vehicle_object()

    person = fake.simple_profile()
    person['company'] = company
    person.pop('birthdate')
    person['username'] = setUsername(person['name'])
    person['mail'] = setMail(person['username'], person['mail'])

    dataset.append(json.dumps(person))

print(dataset[0])

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="PostgreSQL_99")

cur = conn.cursor()
# TODO: crear Timer
for x in range(len(dataset)):
    cur.execute("INSERT INTO usersJsonB (userData) VALUES ('{0}'::jsonb);".format(dataset[x]))
conn.commit()
# TODO: parar Timer

# TODO: reiniciar Timer
for x in range(len(dataset)):
    cur.execute("INSERT INTO usersJson (userData) VALUES ('{0}');".format(dataset[x]))
conn.commit()
# TODO: parar Timer


cur.close()

query1 = "EXPLAIN ANALYZE SELECT jsonb_extract_path(userData, 'company', 'companyCar', 'Model') as car_model, count(*) as cars_by_quantity FROM usersJsonB GROUP BY jsonb_extract_path(userData, 'company', 'companyCar', 'Model')"
query2 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_set(userData, '{company, companyCar, Year}', '2000') FROM usersJsonB as u2 WHERE u1.userId = u2.userId);"
query3 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_insert(userData, '{company, companyCar, licensePlate}', '000-123') FROM usersJsonB as u2 WHERE u1.userId = u2.userId);"

# TODO: cambiar el WHERE final de la query 4 y 5 con un WHERE sobre los años, ya que puede ser mas fácil
query4 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_set(userData, '{company, companyCar, Year}', '2000') FROM usersJsonB as u2 WHERE u1.userId = u2.userId) WHERE userData #>> '{company, companyCar, Model}' LIKE 'G%';"
query5 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_insert(userData, '{company, companyCar, licensePlate}', '000-123') FROM usersJsonB as u2 WHERE u1.userId = u2.userId) WHERE userData #>> '{company, companyCar, Model}' LIKE 'G%';"


d = {'a': 'HOLA', 'b': 'CHAU'}
mongo_client = MongoClient("mongodb://localhost:27017/")
database_mongo = mongo_client["databaseName"]
Collection = database_mongo["collectionName"]
Collection.insert_one(d)
