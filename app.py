from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)
DATA_FILE = "ais_messages.json"

@app.route("/data", methods=["GET"])
def get_data():
    if not os.path.exists(DATA_FILE):
        return jsonify([])
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = []
    return jsonify(data)

@app.route("/update", methods=["POST"])
def update_data():
    new_data = request.get_json()
    if not new_data:
        return "No data provided", 400

    # Append new data to the existing JSON file
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = []
    else:
        data = []

    data.append(new_data)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    return "Data updated", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
