from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["autorent"]

rent_collection = db["rent"]
cursor = rent_collection.aggregate([
    {
        "$lookup": {
            "from": "client",
            "localField": "client_id",
            "foreignField": "id",
            "as": "client"
        }
    },
    {
        "$lookup": {
            "from": "automobile",
            "localField": "automobile_id",
            "foreignField": "id",
            "as": "automobile"
        }
    },
    {
        "$unwind": "$client"
    },
    {
        "$unwind": "$automobile"
    },
    {
        "$project": {
            "_id": 0,
            "client_surname": "$client.client_surname",
            "client_firstname": "$client.client_firstname",
            "client_secondname": "$client.client_secondname",
            "model": "$automobile.model",
            "reg_number": "$automobile.reg_number",
            "price": "$automobile.price",
            "rent_start": 1,
            "rent_end": 1
        }
    }
])

print("Вывод клиентов и машин. которые они арендовали")
for document in cursor:
    print(document)