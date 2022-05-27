#!flask/bin/python
from flask import Flask, jsonify, abort, request
import database


app = Flask(__name__)


@app.route('/todo/api/v1.0/cabinets', methods=['GET'])
def get_all_temps():
    return jsonify(database.get_all_cab_temp())

@app.route('/temp/cabinets' ,methods=['GET'])
def get_temps_main():
    return jsonify(database.get_all_cab_temp_by_date(start_date = '2022-05-24 16:24:43'))

if __name__ == '__main__':
    app.run(debug=True)

