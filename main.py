from faker import Faker
from faker_vehicle import VehicleProvider
import json
from mongo import *
from postgres import *
import time
import argparse


def set_username(name):
	name_split = name.split()
	return name_split[0][0] + name_split[1]


def set_mail(username, mail):
	mail_split = mail.split('@')
	return username + '@' + mail_split[1]


def main():

	CANTIDAD_DE_REGISTROS = 100000
	PG_USERNAME = "postgres"
	PG_PASSWORD = ""
	PG_HOST = ""
	PG_DB = "postgres"

	# PARSE ARGUMENTS
	parser = argparse.ArgumentParser()
	parser.add_argument('-pg_username', dest='postgres_username', required=False, default=PG_USERNAME)
	parser.add_argument('-pg_password', dest='postgres_password', required=False, default=PG_PASSWORD)
	args = parser.parse_args()
	postgres_username = str(args.postgres_username)
	postgres_password = str(args.postgres_password)

	fake = Faker()
	fake.add_provider(VehicleProvider)

	dataset_string = []  # Postgres needs strings to insert data
	dataset_dict = []  # Mongo needs dicts to insert data

	res = None
	try:
		connt = psycopg2.connect(host=PG_HOST, database=PG_DB, user=postgres_username, password=postgres_password)
		curt = connt.cursor()
		curt.execute("SELECT * FROM usersJsonB  LIMIT 1")
		res = curt.fetchone()
		print(res)
		connt.close()
	except:
		pass

	if res is not None:
		CANTIDAD_DE_REGISTROS = 100

	print("Generando dataset...")
	timer_start = time.time()
	for x in range(CANTIDAD_DE_REGISTROS):  # 100k en 100 segundos
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
	if res is None:
		print("Creando tablas...")
		setup_postgres(postgres_username, postgres_password, PG_HOST, PG_DB)
		# setup_mongo()

		# QUERY 6
		print("QUERY insert")
		postgres_query_6(dataset_string, postgres_username, postgres_password, PG_HOST, PG_DB)
		# mongo_query_6(dataset_dict)

	# QUERY 1
	print("QUERY #1")
	postgres_query_1(postgres_username, postgres_password, PG_HOST, PG_DB)
	# mongo_query_1()

	# QUERY 2
	print("QUERY #2")
	postgres_query_2(postgres_username, postgres_password, PG_HOST, PG_DB)
	# mongo_query_2()

	# QUERY 3
	print("QUERY #3")
	postgres_query_3(postgres_username, postgres_password, PG_HOST, PG_DB)
	# mongo_query_3()

	# QUERY 4
	print("QUERY #4")
	postgres_query_4(postgres_username, postgres_password, PG_HOST, PG_DB)
	# mongo_query_4()

	# QUERY 5
	print("QUERY #5")
	postgres_query_5(postgres_username, postgres_password, PG_HOST, PG_DB)
	# mongo_query_5()


if __name__ == '__main__':
	main()
