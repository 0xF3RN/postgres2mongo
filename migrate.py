import psycopg2
import json
import datetime
from pymongo import MongoClient
from create_db import db_checker

# it just works
def convert_date(obj):
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    raise TypeError("Type not serializable")

# mongo insert func
def mongo_insert(data:list, collection:str):
    collection = db[collection]
    try:
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents into collection '{collection}'")
    except Exception as e:
        print(f"An error occurred: {e}")

# postgres conn
conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="12345"
)

# mongo conn
client = MongoClient("localhost", 27017)
db = client["autorent"]

# automobile -> done не надо так делать, лучше через item[].isoformat()
def automobile():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM automobile")
    dict_data = []
    for item in cur.fetchall():
        formatted_item = {
            "id": item[0],
            "model": item[1],
            "reg_number": item[2],
            "prod_date": item[3].isoformat(),
            "mileage": item[4],
            "air_cond": item[5],
            "eng_cap": item[6],
            "lagg_cap": item[7],
            "maint_date": item[8].isoformat(),  # Convert to string format
            "price": item[9]  
        }
        dict_data.append(formatted_item)
    automobile = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(automobile)
    #mongo_insert(dict_data, "automobile")

# claim -> done
def claim():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM claim")
    keys = ["id", "claim_date", "description"]
    dict_data =  [dict(zip(keys, [item[0], item[1].isoformat(), item[2]])) for item in cur.fetchall()]
    claim = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(claim)
    #mongo_insert(dict_data,"claim")

# client -> done
def rent_client():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM client")
    keys = ["id", "client_surname", "client_firestname", "client_secondname", "client_birthday", "client_passport",
              "client_license", "client_email", "client_phone"]
    dict_data = [dict(zip(keys, [item[0], item[1], item[2], item[3], item[4].isoformat(), item[5]])) for item in cur.fetchall()]
    client = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(client)
    #mongo_insert(dict_data, "client")

# employee -> done
def employee():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM employee")
    keys = ["id", "employee_surname", "employee_firestname", "employee_secondname", "employee_email", "employee_phone"]
    dict_data = [dict(zip(keys, item)) for item in cur.fetchall()]
    employee = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(employee)
    #mongo_insert(dict_data,"employee")

# invoice -> done
def invoice():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM invoice")
    keys = ["id", "invoice_date", "rent_cost"]
    dict_data = [dict(zip(keys, [item[0],item[1].isoformat(),item[2]])) for item in cur.fetchall()]
    invoice = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(invoice)
    #mongo_insert(dict_data, "invoice")

# partner -> done
def partner():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM partner")
    keys = ["id", "org_name", "org_address", "org_bank", "org_tax"]
    dict_data = [dict(zip(keys, item)) for item in cur.fetchall()]
    patner = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(patner)
    #mongo_insert(dict_data, "partner")

# rent -> done
def rent():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM rent")
    keys = ["id", "automobile_id", "client_id", "employee_id", "invoice_id", "claim_id", "rent_start",
            "rent_end", "start_point", "end_point", "mileage"]
    dict_data = [dict(zip(keys, [item[0], item[1], item[2], item[3], item[4],item[5], item[6].isoformat(), item[7].isoformat(),
                                 item[8], item[9], item[10]])) for item in cur.fetchall()]
    rent = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(rent)
    #mongo_insert(dict_data, "rent")

# service -> done
def service():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM service")
    keys = ["id", "work_date", "address", "automobile_id", "work_id", "partner_id"]
    dict_data = [dict(zip(keys, [item[0], item[1].isoformat(), item[2], item[3], item[4], item[5]])) for item in cur.fetchall()]
    service = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(service)
    #mongo_insert(dict_data, "service")

# type_of_work -> 
def work():
    cur = conn.cursor()
    cur.execute("SET lc_monetary TO 'ru_RU.UTF-8';SELECT * FROM type_of_work")
    keys = ["id", "work", "description"]
    dict_data = [dict(zip(keys, item)) for item in cur.fetchall()]
    work = json.dumps(dict_data, default=convert_date, ensure_ascii=False, indent=4)
    print(work)
    #mongo_insert(dict_data, "type_of_work")


def main():

    if not db_checker():
        automobile() 
        claim() 
        rent_client() 
        employee()  
        invoice() 
        partner() 
        rent() 
        service() 
        work()
    else:
        print("Data exists")


if __name__ == "__main__":
    main()