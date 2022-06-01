#!flask/bin/python
from flask import Flask, jsonify, abort, request
import database
import dataloader
import datetime
import threading


app = Flask(__name__)


@app.route('/todo/api/v1.0/cabinets', methods=['GET'])
def get_all_temps():
    return jsonify(database.get_all_cab_temp())


@app.route('/temp/cabinets', methods=['GET'])
def get_temps_main():
    start_date = datetime.datetime.now() - datetime.timedelta(hours=2)
    return jsonify(database.get_all_cab_temp_by_date(start_date=start_date))


if __name__ == '__main__':
    threading.Thread(target=lambda: app.run(debug=True, use_reloader=False)).start()
    dataloader.period_load()



