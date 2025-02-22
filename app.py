from collections import defaultdict

from flask import Flask, jsonify, request
import pymongo
from pymongo import MongoClient

DATABASE_NAME = "orders_db"

app = Flask(__name__)

def get_db(db_name=DATABASE_NAME):
    client = MongoClient(host='localhost',
                         port=27017, 
                         username='root', 
                         password='pass',
                         authSource="admin")
    db = client[db_name]
    return db

@app.route('/ignition')
def ping_server():
    return "The api is working fine."

@app.route('/orders')
def get_all_orders():
    db = ""
    try:
        db = get_db()
        cursor = db.orders_tb.find()
        data = list(cursor)
        res = []
        print("Debugging all orders data: ", data)
        for i in data:
            i.pop("_id")
            res.append(i)
        return jsonify({"all_orders": res})
    except:
        pass
    finally:
        if type(db) == MongoClient:
            db.close()


@app.route('/order_id')
def get_orders():
    db=""
    try:
        db = get_db()
        order_id = int(request.args.get('order_id'))
        cursor = db.orders_tb.find({"order_id": order_id})
        result = []
        for element in cursor:
            element.pop("_id")
            result.append(element)
        
        print("Debugging result: ", result)
        return jsonify({"result": result})
    except:
        pass
    finally:
        if type(db)==MongoClient:
            db.close()

@app.route('/avg_products')
def get_avg_products():
    try:
        db = get_db()
        cursor = db.orders_tb.find()
        num = count = 0

        for element in cursor:
            count += 1
            num += len(element.products)
        print("Debugging avg endpoint: ", num, count)
        return jsonify({"average_products": num / count})
    except:
        pass
    finally:
        if type(db) == MongoClient:
            db.close()
    

@app.route('/avg_quantity')
def get_avg_quantity(product_id):
    try:
        db = get_db()
        cursor = db.orders_tb.find()
        num = count = 0
        nums = defaultdict(lambda: [0,0,0])

        for element in cursor:
            for product in element.products:
                nums[product.id][0] += product.quantity
                nums[product.id][1] += 1
                nums[product.id][2] = nums[product.id][0] / nums[product.id][1]
        print("Debugging avg qty endpoint: ", nums)

        return jsonify({"avg_qty_per_product": nums})
    except:
        pass
    finally:
        if type(db) == MongoClient:
            db.close()

if __name__=='__main__':
    app.run(host="0.0.0.0", port=5000)
