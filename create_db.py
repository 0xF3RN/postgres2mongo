from pymongo import MongoClient

client = MongoClient("localhost", 27017)

def db_checker():
    if "autorent" in client.list_database_names():
        return True
    db = client["autorent1"]
    db.create_collection("automobile")
    db.create_collection("claim")
    db.create_collection("client")
    db.create_collection("employee")
    db.create_collection("invoice")
    db.create_collection("partner")
    db.create_collection("rent")
    db.create_collection("service")
    db.create_collection("type_of_work")
    return False