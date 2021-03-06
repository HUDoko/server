#!flask/bin/python
from flask import Flask, jsonify, abort, request
import database
import dataloader
import datetime
import threading


app = Flask(__name__)


@app.route('/temp/cabinets/<string:room>&<string:start_date>&<string:end_date>', methods=['GET'])
def get_cab_temps_se(room, start_date, end_date):
    if len(room) == 0 or len(start_date) == 0 or len(end_date) == 0:
        abort(404)
    return jsonify(database.get_cabinet_temp(room, start_date, end_date))


@app.route('/temp/cabinets/<string:room>&<string:start_date>', methods=['GET'])
def get_cab_temps_s(room, start_date):
    if len(room) == 0 or len(start_date) == 0:
        abort(404)
    end_date = datetime.datetime.now()
    return jsonify(database.get_cabinet_temp(room, start_date, end_date))


@app.route('/temp/cabinets', methods=['GET'])
def get_temps_main():
    end_date = datetime.datetime.strptime("2021-03-20", "%Y-%m-%d")
    start_date = datetime.datetime.strptime("2021-03-19", "%Y-%m-%d")
    #start_date = datetime.datetime.now() - datetime.timedelta(hours=2)
    return jsonify(database.get_all_cab_temp_by_date(start_date=start_date , end_date= end_date))


@app.route('/temp/cabinet/<string:room>&<string:start_date>&<string:end_date>', methods=['GET'])
def get_cab_temps_se2(room, start_date, end_date):
    if len(room) == 0 or len(start_date) == 0 or len(end_date) == 0:
        abort(404)
    return jsonify(database.get_one_cab_temp_by_date(room, start_date, end_date))


@app.route('/temp/cabinet/<string:room>&<string:start_date>', methods=['GET'])
def get_cab_temps_s2(room, start_date):
    if len(room) == 0 or len(start_date) == 0:
        abort(404)
    end_date = datetime.datetime.now()
    return jsonify(database.get_one_cab_temp_by_date(room, start_date, end_date))


if __name__ == '__main__':
    threading.Thread(target=lambda: app.run(debug=True, use_reloader=False)).start()
    dataloader.period_load()



    #dataloader.load_from_csv()



