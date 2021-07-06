import psycopg2
import time

create_table_1 = "DROP TABLE IF EXISTS usersJsonB; CREATE TABLE usersJsonB(" \
                    "userId SERIAL PRIMARY KEY," \
                    "userData JSONB NOT NULL" \
                 ");"

create_table_2 = "DROP TABLE IF EXISTS usersJson; CREATE TABLE usersJson(" \
                    "userId SERIAL PRIMARY KEY," \
                    "userData JSON NOT NULL" \
                 ");"

query1_string = "SELECT jsonb_extract_path(userData, 'company', 'companyCar', 'Model') as car_model, " \
                        "count(*) as cars_by_quantity " \
                "FROM usersJsonB " \
                "GROUP BY jsonb_extract_path(userData, 'company', 'companyCar', 'Model')"

query2_string = "UPDATE usersJsonB as u1 " \
                "SET userData = (" \
                        "SELECT jsonb_set(userData, '{company, companyCar, Category}', '\"SUV\"') " \
                        "FROM usersJsonB as u2 " \
                        "WHERE u1.userId = u2.userId" \
                ");"

query3_string = "UPDATE usersJsonB as u1 " \
                "SET userData = (" \
                        "SELECT jsonb_insert(userData, '{company, companyCar, licensePlate}', '\"000-123\"') " \
                        "FROM usersJsonB as u2 " \
                        "WHERE u1.userId = u2.userId" \
                ");"

query4_string = "UPDATE usersJsonB as u1 " \
                "SET userData = (" \
                        "SELECT jsonb_set(userData, '{company, companyCar, Make}', '\"Ford\"') " \
                        "FROM usersJsonB as u2 " \
                        "WHERE u1.userId = u2.userId) " \
                "WHERE (userData #>> '{company, companyCar, Year}')::int > 2005;"

query5_string = "UPDATE usersJsonB as u1 " \
                "SET userData = (" \
                        "SELECT jsonb_insert(userData, '{company, companyCar, color}', '\"yellow\"') " \
                        "FROM usersJsonB as u2 " \
                        "WHERE u1.userId = u2.userId) " \
                "WHERE (userData #>> '{company, companyCar, Year}')::int < 2005;"


def postgres_query_6(dataset, username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    query = "INSERT INTO usersJson (userData) VALUES ('{0}')".format(dataset[0])
    timer_start = time.time()
    for x in range(1, len(dataset)):
        query = query + ",('{0}')".format(dataset[x])
    cur.execute(query + ";")
    conn.commit()
    timer_end = time.time()
    print("\tQuery 6 - Postgres - with JSON: {0} segundos".format(timer_end - timer_start))
    cur.close()

    cur = conn.cursor()
    query = "INSERT INTO usersJson (userData) VALUES ('{0}'::jsonb)".format(dataset[0])
    timer_start = time.time()
    for x in range(1, len(dataset)):
        query = query + ",('{0}'::jsonb)".format(dataset[x])
    cur.execute(query + ";")
    conn.commit()
    timer_end = time.time()
    print("\tQuery 6 - Postgres - with JSONB: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_1(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query1_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 1 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_2(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query2_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 2 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_3(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query3_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 3 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_4(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query4_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 4 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_5(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query5_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 5 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def setup_postgres(username, password):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user=username,
        password=password)
    cur = conn.cursor()
    cur.execute(create_table_1)
    conn.commit()
    cur.execute(create_table_2)
    conn.commit()
    cur.close()
    print("\tTablas en Postgres creadas!")
