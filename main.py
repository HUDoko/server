#!flask/bin/python
from flask import Flask, jsonify, abort, request
import database
import dataloader
import datetime


app = Flask(__name__)


@app.route('/todo/api/v1.0/cabinets', methods=['GET'])
def get_all_temps():
    return jsonify(database.get_all_cab_temp())


@app.route('/temp/cabinets', methods=['GET'])
def get_temps_main():
    start_date = datetime.now() - datetime.timedelta(hours=2)
    return jsonify(database.get_all_cab_temp_by_date(start_date=start_date))


if __name__ == '__main__':
    dataloader.load_sensor_info()
    app.run(debug=True)


