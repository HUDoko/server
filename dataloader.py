import database
import DateConvertor as dc
import SensorInfo as si


def load_sensor_info():
    print("start_Load")
    if database.is_table_exist('cabinets'):
        select = """SELECT CAB_ID,
              full_name,
              type,
              cab_name,
              link
         FROM cabinets;
        """
        cabinets = database.select_execute(select)

        for cabinet in cabinets:
            select_last_date = f"SELECT date_time FROM temperature WHERE CAB_ID = {cabinet[0]} ORDER BY date_time desc LIMIT 1 "
            last_date = database.select_execute(select_last_date)
            new_data = si.get_sensor_info_from_url(cabinet[4])
            dc.df_add_datetime(new_data)
            new_data = new_data.drop(labels=['date', 'time'], axis=1)
            new_data = new_data[new_data['date_time'] > last_date[0][0]]
            for index, row in new_data.iterrows():
                insert = "INSERT INTO temperature  VALUES ('{}', {}, {});".format(row['date_time'], row['temp'], cabinet[0])
                print(insert)
                database.insert_execute(insert)
    else:
        print("DB not found")
        fill_empty_database()


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

    for sensor in sensor_info.keys():
        is_wall = "bat"
        if sensor.find('wall') != -1:
            is_wall = 'wall'
        sensor_num = sensor.split('_')[1]
        CAB_ID = get_next_cab_id()
        Insert = "INSERT INTO cabinets VALUES ({}, '{}', '{}', '{}', '{}');".format(CAB_ID, sensor, is_wall, sensor_num, sensor_info[sensor])
        print(Insert)
        cursor.execute(Insert)
        conn.commit()
        cabinet = si.get_sensor_info_from_url(sensor_info[sensor])
        dc.df_add_datetime(cabinet)
        cabinet = cabinet.drop(labels=['date', 'time'], axis=1)
        # print(cab_210)
        #Drop = "DROP TABLE IF EXISTS {};".format(sensor)
        #cursor.execute(Drop)
        #Create = "CREATE TABLE IF NOT EXISTS {} (date_time DATETIME , temp real);".format(sensor)
        #cursor.execute(Create)
        #print(Create)

        Drop = "DROP TABLE IF EXISTS {};".format(sensor)
        cursor.execute(Drop)
        Create = "CREATE TABLE IF NOT EXISTS temperature (date_time DATETIME , temp real, CAB_ID INTEGER);"
        cursor.execute(Create)


        for index, row in cabinet.iterrows():
            Insert = "INSERT INTO temperature  VALUES ('{}', {}, {});".format(row['date_time'], row['temp'], CAB_ID)
            cursor.execute(Insert)


        conn.commit()
    conn.close()
    

