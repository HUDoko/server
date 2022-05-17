import sqlite3 as sql




# получение последних 10(по умолчанию) измерений по всем кабинетам
def get_all_cab_temp(count=10):
    db_file = "C:\\Users\\tautp\\PycharmProjects\\FillDataBase\\weather.db"
    conn = sql.connect(db_file)
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
