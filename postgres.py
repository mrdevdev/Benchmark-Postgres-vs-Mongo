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


def postgres_query_6(dataset):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="PostgreSQL_99")

    cur = conn.cursor()
    timer_start = time.time()
    for x in range(len(dataset)):
        cur.execute("INSERT INTO usersJson (userData) VALUES ('{0}');".format(dataset[x]))
    conn.commit()
    timer_end = time.time()
    print("\tQuery 6 - Postgres - with JSON: {0} segundos".format(timer_end - timer_start))
    cur.close()

    cur = conn.cursor()
    timer_start = time.time()
    for x in range(len(dataset)):
        cur.execute("INSERT INTO usersJsonB (userData) VALUES ('{0}'::jsonb);".format(dataset[x]))
    conn.commit()
    timer_end = time.time()
    print("\tQuery 6 - Postgres - with JSONB: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_1():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="PostgreSQL_99")

    cur = conn.cursor()
    timer_start = time.time()
    cur.execute(query1_string)
    conn.commit()
    timer_end = time.time()
    print("\tQuery 1 - Postgres: {0} segundos".format(timer_end - timer_start))
    cur.close()


def postgres_query_2():
    pass


def postgres_query_3():
    pass


def postgres_query_4():
    pass


def postgres_query_5():
    pass


def setup_postgres():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="PostgreSQL_99")
    cur = conn.cursor()
    cur.execute(create_table_1)
    conn.commit()
    cur.execute(create_table_2)
    conn.commit()
    cur.close()
    print("\tTablas en Postgres creadas!")
