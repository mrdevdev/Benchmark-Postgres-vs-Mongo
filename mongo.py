from pymongo import MongoClient
import time


def setup_mongo():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    collection.drop()
    print("\tColección en Mongo creada!")


def mongo_query_6(dataset):
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]

    timer_start = time.time()
    collection.insert_many(dataset)
    timer_end = time.time()
    print("\tQuery 6 - Mongo: {0} segundos".format(timer_end - timer_start))


def mongo_query_1():
    pass


def mongo_query_2():
    pass


def mongo_query_3():
    pass


def mongo_query_4():
    pass


def mongo_query_5():
    pass
