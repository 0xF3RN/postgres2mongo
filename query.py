from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client["autorent"]


def cost_for_clients():
    collection = db["rent"]
    cursor = collection.aggregate([
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
            }
        }
    ])
    for doc in cursor:
        print(doc)


def toyota_cars():
    collection = db["automobile"]
    cursor = collection.find({"model": {"$regex": "Toyota", "$options": "i"}}, {"model":1, "_id":0})
    for car in cursor:
        print(car)

def client_claims():
    collection = db["rent"]
    cursor = collection.aggregate([
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
                "from": "claim",
                "localField": "claim_id",
                "foreignField": "id",
                "as": "claim"
            }
        },
        {
            "$unwind": "$client"
        },
        {
            "$unwind": "$claim"
        },
        {
            "$project": {
                "_id": 0,
                "client_surname": "$client.client_surname",
                "client_firstname": "$client.client_firstname",
                "client_secondname": "$client.client_secondname",
                "claim": "$claim.description"
            }
        }
    ])
    for doc in cursor:
        print(doc)

def partner_ximki():
    collection = db["partner"]
    cursor = collection.find({"org_name": {"$regex": "Химки", "$options": "i"}},  {"org_name": 1, "org_address": 1, "_id": 0})
    for doc in cursor:
        print(doc)

def over10k():
    collection = db["automobile"]
    cursor = collection.find({"mileage": {"$gt": 10000}}, {"model": 1, "mileage": 1, "_id": 0})
    for car in cursor:
        print(car)



def main():
    print("Запрос для вывода дат, стоимости аренды и клиента")
    cost_for_clients()
    print("Запрос для вывода всех машин марки Tayota")
    toyota_cars()
    print("Запрос для вывода клиентов и жалоб на них")
    client_claims()
    print("Запрос для вывода партнеров из города Химки")
    partner_ximki()
    print("Вывод машин с  пробегом более 10000км")
    over10k()


main()