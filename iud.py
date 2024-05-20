from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["autorent"]
collection = db["claim"]

def insert():
    document = {
    "id": 3,
    "claim_date": "2023-11-11",
    "description": "не заправил!"
    }
    result = collection.insert_one(document)
    print(f"Inserted document ID: {result.inserted_id}")
    print(collection.find_one({'_id':result.inserted_id}))

def update():
    result = collection.update_one(
    {"id": 3},
    {"$set": {"description": "а нет заправил"}}
    )
    print(f"Updated {result.modified_count} document(s).")
    print(collection.find_one({"id":3}))

def delete():
    result = collection.delete_one({"id": 3})
    print(f"Deleted {result.deleted_count} document(s).")
    print(collection.find_one({"id":3}))

insert()
update()
delete()

