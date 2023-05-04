from collections import defaultdict
import os

from flask import Flask, jsonify, request
import pymongo

DATABASE_NAME = "orders_db"

app = Flask(__name__)

def get_db(db_name=DATABASE_NAME):
    host = os.environ.get("MONGODB_URI", "localhost")
    if host == "localhost":
        client = pymongo.MongoClient(host='localhost',
                                    port=27017,
                                    username='root',
                                    password='pass',
                                    authSource='admin')
    else:
        client = pymongo.MongoClient(host=host)
    db = client[db_name]
    return db

# Sanity check
@app.route('/ignition')
def ping_server():
    return "The api is working fine."

# DB connection check endpoint
@app.route('/orders')
def get_all_orders():
    try:
        db = get_db()
        cursor = db.orders_tb.find()
        data = list(cursor)
        res = []
        for i in data:
            i.pop("_id")
            res.append(i)
    except Exception as e:
        print("An error occured: ", e)
        db = None
    finally:
        if type(db) == pymongo.MongoClient:
            db.close()
        return jsonify({"all_orders": res})

# Fetch order(s) by order_id
@app.route('/order_id')
def get_orders():
    try:
        db = get_db()
        order_id = int(request.args.get('order_id'))
        cursor = db.orders_tb.find({"order_id": order_id})
        result = []
        for element in cursor:
            element.pop("_id")
            result.append(element)
        
        return jsonify({"result": result})
    except:
        db = None
    finally:
        if type(db) == pymongo.MongoClient:
            db.close()

@app.route('/avg_products')
def get_avg_products():
    try:
        db = get_db()
        cursor = db.orders_tb.find()
        num = count = 0

        for element in cursor:
            count += 1
            num += len(element["products"])
        print("Debugging avg endpoint: ", num, count)
        return jsonify({"average_products": num / count})
    except Exception as e:
        print("An error occured: ", e)
        db = None
    finally:
        if type(db) == pymongo.MongoClient:
            db.close()


@app.route('/create_materialized_view')
def create_materialized_view():
    db = get_db()
    pipeline = [
        # Unwind the products array
        {"$unwind": "$products"},
        
        # Group by product id and name to calculate average quantity
        {"$group": {
            "_id": {"id": "$products.id"},
            "avg_quantity": {"$avg": "$products.quantity"}
        }},
        
        # Project the desired fields in the output
        {"$project": {
            "_id": 0,
            "id": "$_id.id",
            "name": "$products.name",
            "measurement": "$products.measurement",
            "avg_quantity": "avg_quantity"
        }},
        # Materialized views merging options
        {"$merge": {
            "into": "orders_tb",
            "on": "_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }}
    ]
    
    db.orders_tb.aggregate(pipeline, allowDiskUse=True, collation={"locale": "en_US", "strength": 2}).\
        allowDiskUse(True).\
        out("my_materialized_view")
        
    return jsonify({"message": "Materialized view created/updated successfully."})

@app.route('/avg_quantity')
def get_avg_quantity():
    try:
        db = get_db()
        pipeline = [
            # Unwind the products array
            {"$unwind": "$products"},
            
            # Group by product id and name to calculate average quantity
            {"$group": {
                "_id": {"id": "$products.id", "name": "$products.name", "measurement": "$products.measurement"},
                "avg_quantity": {"$avg": "$products.quantity"}
            }},
            
            # Project the desired fields in the output
            {"$project": {
                "_id": 0,
                "id": "$_id.id",
                "name": "$_id.name",
                "measurement": "$_id.measurement",
                "avg_quantity": 1
            }}
        ]

        # Execute the pipeline and store the results in a list
        results = list(db.orders_tb.aggregate(pipeline))

        return jsonify({"avg_qty_per_product": results})
    except:
        db = None
    finally:
        if type(db) == pymongo.MongoClient:
            db.close()

if __name__=='__main__':
    app.run(host="0.0.0.0", port=5000)
