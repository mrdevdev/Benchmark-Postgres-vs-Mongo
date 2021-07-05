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
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    
    timer_start = time.time()
    collection.aggregate([{
        "$group": {
            "_id": "$company.companyCar.Model", 
            "cars_by_quantity": {"$sum": 1}
        }
    }, {
        "$sort": {"cars_by_quantity": -1}
    }])
    timer_end = time.time()
    print("\tQuery 1 - Mongo: {0} segundos".format(timer_end - timer_start))


def mongo_query_2():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    
    timer_start = time.time()
    collection.update_many(
    {},
    {
        "$set": {"company.companyCar.Category": "SUV"}
    })
    timer_end = time.time()
    print("\tQuery 2 - Mongo: {0} segundos".format(timer_end - timer_start))


def mongo_query_3():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    
    timer_start = time.time()
    collection.update_many(
    {},
    {
        "$set" : {"company.companyCar.licensePlate": "000-123"}
    })
    timer_end = time.time()
    print("\tQuery 3 - Mongo: {0} segundos".format(timer_end - timer_start))


def mongo_query_4():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    
    timer_start = time.time()
    collection.update_many(
    {"company.companyCar.Year": {"$gt" : 2005}},
    {
        "$set": {"company.companyCar.Make": "Ford"}
    })
    timer_end = time.time()
    print("\tQuery 4 - Mongo: {0} segundos".format(timer_end - timer_start))


def mongo_query_5():
    mongo_client = MongoClient("mongodb://localhost:27017/")
    database_mongo = mongo_client["final_bases2"]
    collection = database_mongo["users"]
    
    timer_start = time.time()
    collection.update_many(
    {"company.companyCar.Year": {"$lt" : 2005}},
    {
        "$set": {"company.companyCar.color": "Yellow"}
    })
    timer_end = time.time()
    print("\tQuery 5 - Mongo: {0} segundos".format(timer_end - timer_start))

