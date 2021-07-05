from faker import Faker
from faker_vehicle import VehicleProvider
import json
from mongo import *
from postgres import *
import time


def set_username(name):
    name_split = name.split()
    return name_split[0][0] + name_split[1]


def set_mail(username, mail):
    mail_split = mail.split('@')
    return username + '@' + mail_split[1]


def main():
    fake = Faker()
    fake.add_provider(VehicleProvider)

    dataset_string = []  # Postgres needs strings to insert data
    dataset_dict = []    # Mongo needs dicts to insert data

    print("Generando dataset...")
    timer_start = time.time()
    for x in range(100000):  # 100k en 100 segundos
        company = {}
        company['name'] = fake.company()
        company['suffix'] = fake.company_suffix()
        company['address'] = fake.address()
        company['companyCar'] = fake.vehicle_object()

        person = fake.simple_profile()
        person['company'] = company
        person.pop('birthdate')
        person['username'] = set_username(person['name'])
        person['mail'] = set_mail(person['username'], person['mail'])

        dataset_string.append(json.dumps(person))
        person['_id'] = x
        dataset_dict.append(person)

    timer_end = time.time()
    print("\tTiempo en generar el dataset: {0} segundos".format(timer_end - timer_start))

    # SETUP
    print("Creando tablas...")
    setup_postgres()
    setup_mongo()

    # QUERY 6
    print("QUERY #6")
    postgres_query_6(dataset_string)
    mongo_query_6(dataset_dict)

    # QUERY 1
    print("QUERY #1")
    postgres_query_1()
    mongo_query_1()

    # QUERY 2
    print("QUERY #2")
    postgres_query_2()
    mongo_query_2()

    # QUERY 3
    print("QUERY #3")
    postgres_query_3()
    mongo_query_3()

    # QUERY 4
    print("QUERY #4")
    postgres_query_4()
    mongo_query_4()

    # QUERY 5
    print("QUERY #5")
    postgres_query_5()
    mongo_query_5()


    query2 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_set(userData, '{company, companyCar, Year}', '2000') FROM usersJsonB as u2 WHERE u1.userId = u2.userId);"
    query3 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_insert(userData, '{company, companyCar, licensePlate}', '000-123') FROM usersJsonB as u2 WHERE u1.userId = u2.userId);"

    # TODO: cambiar el WHERE final de la query 4 y 5 con un WHERE sobre los años, ya que puede ser mas fácil
    query4 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_set(userData, '{company, companyCar, Year}', '2000') FROM usersJsonB as u2 WHERE u1.userId = u2.userId) WHERE userData #>> '{company, companyCar, Model}' LIKE 'G%';"
    query5 = "EXPLAIN ANALYZE UPDATE usersJsonB as u1 SET userData = (SELECT jsonb_insert(userData, '{company, companyCar, licensePlate}', '000-123') FROM usersJsonB as u2 WHERE u1.userId = u2.userId) WHERE userData #>> '{company, companyCar, Model}' LIKE 'G%';"


if __name__ == '__main__':
    main()
