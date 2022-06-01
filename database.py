import sqlite3 as sql


def get_connection():
    db_file = "weather.db"
    return sql.connect(db_file)

# получение последних 10(по умолчанию) измерений по всем кабинетам
def get_all_cab_temp(count=10):
    conn = get_connection()
    cursor = conn.cursor()
    cabinets = cursor.execute("SELECT DISTINCT cab_name FROM cabinets_tables;").fetchall()
    tables = cursor.execute("SELECT table_name, type, cab_name FROM cabinets_tables;").fetchall()
    result = {}
    for cab in cabinets:
        result["{}".format(cab[0])] = {
            'dates': [],
            'temp':
                {
                    'wall': [],
                    'bat': []
                },
        }
    for table in tables:
        records = cursor.execute('SELECT date_time, "temp" FROM {} ORDER BY date_time desc  LIMIT {}'.format(table[0], count)).fetchall()
        if len(result[table[2]]['dates']) == 0:
            for rec in records:
                result[table[2]]['dates'].append(rec[0])
                result[table[2]]['temp'][table[1]].append(rec[1])
        else:
            # если есть данные, то сливаем аккуратно воедино массивы
            insert_index = 0
            result[table[2]]['temp'][table[1]] = [None] * count
            for rec in records:
                while insert_index < len(result[table[2]]['dates']) and result[table[2]]['dates'][insert_index] < rec[0]:
                    insert_index += 1
                result[table[2]]['dates'].insert(insert_index, rec[0])
                result[table[2]]['temp'][not_wb(table[1])].insert(insert_index, None)
                result[table[2]]['temp'][table[1]].insert(insert_index, rec[1])
            result[table[2]]['temp'][table[1]] = avg_fill(result[table[2]]['temp'][table[1]])
            result[table[2]]['temp'][not_wb(table[1])] = avg_fill( result[table[2]]['temp'][not_wb(table[1])])

    conn.close()
    return result

#получить json для всех кабинетов за период времени
def get_all_cab_temp_by_date(start_date = None, end_date = None):
    conn = get_connection()
    cursor = conn.cursor()
    select = """
    SELECT date_time,
       "temp",      
       type,
       cab_name
    FROM temperature    
    LEFT JOIN  
    cabinets on temperature.CAB_ID == cabinets.CAB_ID   
    """

    # Формирование Where для SQL-запроса
    where = ""
    if not (start_date is None):
        where = f' WHERE date_time > "{start_date}" '
        if not (end_date is None):
            where += f' AND date_time < "{end_date}"'
    else:
        if not (end_date is None):
            where += f' WHERE date_time < "{end_date}"'
    select += where

    order = "ORDER BY date_time;"
    select += order
    print(select)
    sql_res = cursor.execute(select).fetchall()
    result = {}
    for row in sql_res:
        if not result.keys().__contains__(row[3]):
            result[row[3]] = {
            'dates': [],
            'temp':
                {
                    'wall': [],
                    'bat': []
                },
            }
        result[row[3]]['dates'].append(row[0])
        result[row[3]]['temp'][row[2]].append(row[1])
        result[row[3]]['temp'][not_wb(row[2])].append(None)

    for key in result.keys():
        result[key]['temp']['wall'] = avg_fill(result[key]['temp']['wall'])
        result[key]['temp']['bat'] = avg_fill(result[key]['temp']['bat'])

    return result



def not_wb(str):
    if str == "wall":
        return "bat"
    if str == "bat":
        return "wall"
    raise "not_wb is only using for wall and bat values"


def avg_fill(array):
    prev_i = None
    next_i = None
    none_count = 0
    i = 0
    while i < len(array):
        if array[i] is not None:
            if none_count != 0:
                next_i = i
                if prev_i is None:
                    for j in range(0, next_i):
                        array[j] = array[next_i]
                else:
                    for j in range(prev_i+1, next_i):
                        array[j] = (array[prev_i] * (next_i-j) + array[next_i] * (j - prev_i)) / (next_i - prev_i)
            prev_i = i
            none_count = 0
        else:
            none_count += 1
            if i == len(array)-1:
                for j in range(prev_i + 1, len(array)):
                    array[j] = array[prev_i]
        i += 1
    return array


def select_execute(select):
    conn = get_connection()
    cursor = conn.cursor()
    return cursor.execute(select).fetchall()


def insert_execute(insert):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(insert)
    conn.commit()


def is_table_exist(table_name):
    conn = get_connection()
    cursor = conn.cursor()
    select = f"SELECT * FROM {table_name} LIMIT 1"
    try:
        cursor.execute(select)
        return True
    except:
       return False

