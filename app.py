from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo import ReadPreference

app = Flask(__name__)
MongoDB_url = "mongodb+srv://ch85_db_user:Christian@cs498-hw3cluster.fh6mtv0.mongodb.net/ev_db?retryWrites=true&w=majority&appName=cs498-hw3cluster"
client = MongoClient(MongoDB_url)
databaseLoaded = client["ev_db"]
collection = databaseLoaded["vehicles"]

def start_server():
    app.run(host="0.0.0.0", port=5000, debug=True)
@app.post("/insert-fast")
def insert_fast():
    data = request.get_json()
    #from documentation - > w=1 meanign that only the primary node acknowledges
    #this is faster but risk of data loss if primary crashes
    fast_collection = collection.with_options(
        write_concern=WriteConcern(w=1)
    )
    result = fast_collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)})

#wait until a majorityy of the nodes acknoeldge the write -> will be slower but also very durable:)
@app.post("/insert-safe")
def insert_safe():
    data = request.get_json()

    safe_collection = collection.with_options(
        write_concern=WriteConcern(w="majority")
    )
    result = safe_collection.insert_one(data)
    return jsonify({"inserted_id": str(result.inserted_id)})
# From instructions remember JSON object with format {"count": <total_count>}, while <total_count> is count of Tesla vehicles.
@app.get("/count-tesla-primary")
def count_tesla_primary():
    primary_collection = collection.with_options(
        read_preference=ReadPreference.PRIMARY
    )
    total_count = primary_collection.count_documents({"Make": "TESLA"})
    return jsonify({"count": total_count})

@app.get("/count-bmw-secondary")
def count_bmw_secondary():
    secondary_collection = collection.with_options(
        read_preference=ReadPreference.SECONDARY_PREFERRED
    )
    total_count = secondary_collection.count_documents({"Make": "BMW"})
    return jsonify({"count": total_count})

if __name__ == "__main__":
    start_server()
