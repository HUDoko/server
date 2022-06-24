import numpy as np

import database
import DateConvertor as dc
import SensorInfo as si
import time
import pandas as pd
import weather_load as wi


def load_from_csv():
    filepath = "big_table.csv"
    sensor_info = {
        "cabinet_210_bat": "210_bat_temp",
        "cabinet_210_wall": "210_wall_temp",
        "cabinet_316_bat": "316_bat_temp",
        "cabinet_316_wall": "316_wall_temp",
        "cabinet_412a_bat": "412a_bat_temp",
        "cabinet_412a_wall": "412a_wall_temp",
        "cabinet_420_bat": "420_bat_temp",
        "cabinet_420_wall": "420_wall_temp",
        "cabinet_219_bat": "219_bat_temp",
        "cabinet_219_wall": "219_wall_temp"
    }
    weather_table = pd.read_csv(filepath, parse_dates=["date_time"], sep=';', index_col="date_time")
    weather_table = weather_table[['210_bat_temp', '210_wall_temp', '316_bat_temp',
                                   '316_wall_temp', '412a_bat_temp', '412a_wall_temp',
                                   '420_bat_temp', '420_wall_temp', '219_bat_temp', '219_wall_temp']]
    conn = database.get_connection()
    cursor = conn.cursor()

    Drop = "DROP TABLE IF EXISTS cabinets;"
    Create = "CREATE TABLE IF NOT EXISTS cabinets (CAB_ID INTEGER UNIQUE, full_name text, type text, cab_name text, link text);"
    cursor.execute(Drop)
    cursor.execute(Create)
    conn.commit()

    Drop = "DROP TABLE IF EXISTS temperature;"
    cursor.execute(Drop)
    Create = "CREATE TABLE IF NOT EXISTS temperature (date_time DATETIME , temp real, CAB_ID INTEGER);"
    cursor.execute(Create)


    for sensor in sensor_info.keys():
        is_wall = "bat"
        if sensor.find('wall') != -1:
            is_wall = 'wall'
        sensor_num = sensor.split('_')[1]
        CAB_ID = get_next_cab_id()
        Insert = "INSERT INTO cabinets VALUES ({}, '{}', '{}', '{}', '{}');".format(CAB_ID, sensor, is_wall, sensor_num,
                                                                                    sensor_info[sensor])
        print(Insert)
        cursor.execute(Insert)
        conn.commit()
        cabinet = weather_table[sensor_info[sensor]]
        #dc.df_add_datetime(cabinet)
        #cabinet = cabinet.drop(labels=['date', 'time'], axis=1)
        # print(cab_210)
        # Drop = "DROP TABLE IF EXISTS {};".format(sensor)
        # cursor.execute(Drop)
        # Create = "CREATE TABLE IF NOT EXISTS {} (date_time DATETIME , temp real);".format(sensor)
        # cursor.execute(Create)
        # print(Create)


        for index in cabinet.index:
            if not np.isnan(cabinet[index]):
                Insert = "INSERT INTO temperature  VALUES ('{}', {}, {});".format(index, cabinet[index] , CAB_ID)
                cursor.execute(Insert)

        conn.commit()
    conn.close()



def period_load(pause=120):
    while True:
        load_sensor_info()
        time.sleep(pause)


def load_sensor_info():
    print("start_Load")
    if database.is_table_exist('cabinets') and  database.is_table_exist('temperature'):
        select = """SELECT CAB_ID,
              full_name,
              type,
              cab_name,
              link
         FROM cabinets;
        """
        cabinets = database.select_execute(select)
        print(cabinets)
        conn = database.get_connection()
        cursor = conn.cursor()

        for cabinet in cabinets:
            print(cabinet[0])
            select_last_date = f"SELECT date_time FROM temperature WHERE CAB_ID = {cabinet[0]} ORDER BY date_time desc LIMIT 1 "
            last_date = database.select_execute(select_last_date)
            new_data = si.get_sensor_info_from_url(cabinet[4])
            dc.df_add_datetime(new_data)
            if not new_data.empty:
                new_data = new_data.drop(labels=['date', 'time'], axis=1)
                new_data = new_data[new_data['date_time'] > last_date[0][0]]
                # print(new_data)
                for index, row in new_data.iterrows():
                    insert = "INSERT INTO temperature  VALUES ('{}', {}, {});".format(row['date_time'], row['temp'],
                                                                                      cabinet[0])
                    print(insert)
                    cursor.execute(insert)
                conn.commit()

    else:
        print("DB not found")
        fill_empty_database()
    print("end_Load")


def get_next_cab_id():
    select = 'SELECT CAB_ID FROM cabinets ORDER BY CAB_ID desc  LIMIT 1'
    ID = database.select_execute(select)
    print(ID)
    if len(ID) == 0:
        return 0
    return ID[0][0] + 1


def fill_empty_database():
    sensor_info = {
        "cabinet_210_bat": "http://sensors.mwlabs.ru/view/ABE679970903",
        "cabinet_210_wall": "http://sensors.mwlabs.ru/view/43E079971203",
        "cabinet_316_bat": "http://sensors.mwlabs.ru/view/0A7779970903",
        "cabinet_316_wall": "http://sensors.mwlabs.ru/view/94CC79971203",
        "cabinet_412a_bat": "http://sensors.mwlabs.ru/view/7D0679971403",
        "cabinet_412a_wall": "http://sensors.mwlabs.ru/view/A2BF79971203",
        "cabinet_420_bat": "http://sensors.mwlabs.ru/view/53C779971203",
        "cabinet_420_wall": "http://sensors.mwlabs.ru/view/6AE379971203",
        "cabinet_219_bat": "http://sensors.mwlabs.ru/view/BDF579970903",
        "cabinet_219_wall": "http://sensors.mwlabs.ru/view/F51A79970903"
    }

    conn = database.get_connection()
    cursor = conn.cursor()

    Drop = "DROP TABLE IF EXISTS cabinets;"
    Create = "CREATE TABLE IF NOT EXISTS cabinets (CAB_ID INTEGER UNIQUE, full_name text, type text, cab_name text, link text);"
    cursor.execute(Drop)
    cursor.execute(Create)
    conn.commit()

    Drop = "DROP TABLE IF EXISTS temperature;"
    cursor.execute(Drop)
    Create = "CREATE TABLE IF NOT EXISTS temperature (date_time DATETIME , temp real, CAB_ID INTEGER);"

    for sensor in sensor_info.keys():
        is_wall = "bat"
        if sensor.find('wall') != -1:
            is_wall = 'wall'
        sensor_num = sensor.split('_')[1]
        CAB_ID = get_next_cab_id()
        Insert = "INSERT INTO cabinets VALUES ({}, '{}', '{}', '{}', '{}');".format(CAB_ID, sensor, is_wall, sensor_num,
                                                                                    sensor_info[sensor])
        print(Insert)
        cursor.execute(Insert)
        conn.commit()
        cabinet = si.get_sensor_info_from_url(sensor_info[sensor])
        dc.df_add_datetime(cabinet)
        cabinet = cabinet.drop(labels=['date', 'time'], axis=1)
        # print(cab_210)
        # Drop = "DROP TABLE IF EXISTS {};".format(sensor)
        # cursor.execute(Drop)
        # Create = "CREATE TABLE IF NOT EXISTS {} (date_time DATETIME , temp real);".format(sensor)
        # cursor.execute(Create)
        # print(Create)


        cursor.execute(Create)

        for index, row in cabinet.iterrows():
            Insert = "INSERT INTO temperature  VALUES ('{}', {}, {});".format(row['date_time'], row['temp'], CAB_ID)
            cursor.execute(Insert)

        conn.commit()
    conn.close()


def get_weather():
    filepath="big_table.csv"
    weather_table = pd.read_csv(filepath, parse_dates=["date_time"], sep=';', index_col="date_time")
    weather_table = weather_table[['wind_dir', 'wind_speed', 'visibility',
                                   'atm_phenomena', 'T', 'Td', 'f', 'Te', 'QNH', 'Po', 'COA', 'CULA',
                                   'CLB', 'CT', 'altitude', 'azimuth']]

    conn = database.get_connection()
    cursor = conn.cursor()
    Drop = "DROP TABLE IF EXISTS weather;"
    Create = "CREATE TABLE IF NOT EXISTS weather(" \
             "wind_dir INTEGER, " \
             "wind_speed INTEGER," \
             "visibility INTEGER , " \
             "atm_phenomena TEXT , " \
             "T INTEGER , " \
             "Td INTEGER , " \
             "f INTEGER, " \
             "Te INTEGER, " \
             "QNH INTEGER, " \
             "Po INTEGER, " \
             "COA INTEGER, " \
             "CULA INTEGER, " \
             "CLB INTEGER, " \
             "CT INTEGER, " \
             "altitude REAL, " \
             "azimuth REAL " \
             ");"
    cursor.execute(Drop)
    cursor.execute(Create)
    conn.commit()

    for index, row in weather_table.iterrows():
        str = ""
        last = weather_table.columns[-1]
        for column in weather_table.columns:
            if column == last:
                str += f"'{row[column]}'"
            else:
                str+= f"'{row[column]}'" + ", "
        Insert = f"""INSERT INTO weather VALUES(
        {str}
        );
        """
        print(Insert)
        cursor.execute(Insert)
    conn.commit()
    conn.close()

