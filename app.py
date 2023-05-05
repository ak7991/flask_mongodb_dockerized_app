from collections import defaultdict

from flask import Flask, jsonify
import pymongo
from pymongo import MongoClient

app = Flask(__name__)

def get_db():
    client = MongoClient(host='test_mongodb',
                         port=27017, 
                         username='root', 
                         password='pass',
                        authSource="admin")
    db = client["animal_db"]
    return db

@app.route('/ignition')
def ping_server():
    return "The api is working fine."

@app.route('/animals')
def get_stored_animals():
    db=""
    try:
        db = get_db()
        _animals = db.animal_tb.find()
        animals = [{"id": animal["id"], "name": animal["name"], "type": animal["type"]} for animal in _animals]
        return jsonify({"animals": animals})
    except:
        pass
    finally:
        if type(db)==MongoClient:
            db.close()

@app.route('/order_id')
def get_orders(order_id):
    db=""
    try:
        db = get_db()
        cursor = db.orders_tb.find({"id": order_id})
        result = []
        for element in cursor:
            temp = {
                "order_id": element.order_id,
                "product_count": element.product_count,
                "products": element.products
                }
            result.append(temp)
        
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

        return jsonify({"avg_qty_per_product": nums})
    except:
        pass
    finally:
        if type(db) == MongoClient:
            db.close()

if __name__=='__main__':
    app.run(host="0.0.0.0", port=5000)
