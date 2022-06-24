from keras.models import model_from_json
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta


# загрузка модели
def load_model(model_name):
    json_file = open(model_name + '.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()

    loaded_model = model_from_json(loaded_model_json)

    # load weights into new model
    loaded_model.load_weights(model_name + ".h5")
    print("Model succesfuly loaded")
    return loaded_model


# сохранение модели
def save_model(model, model_name):
  model_json = model.to_json()
  with open(model_name + ".json", "w") as json_file:
    json_file.write(model_json)
    model.save_weights(model_name + ".h5")
  print("Model " + "'" + model_name + "'" + " succesfuly saved ")
  return model_name


def look_range(x, bat, range_size = 3):
  if x["412a_wall_temp_anomaly"] == 0:
    return 0
  start_date = x.name - timedelta(hours = range_size)
  end_date = x.name +  timedelta(hours = range_size)
  for date in pd.date_range(start_date,end_date,freq= '1H'):
    #print(butt.index.isin([date]).any())
    if x["412a_wall_temp_anomaly"] == 1 and bat.index.isin([date]).any():
      if bat.loc[date]['anomaly'] == 1:
        return 1
  return 0


def bat_anomaly(batery_array):
    battery_column = 'batery'
    bat = pd.DataFrame(batery_array, colunns = [battery_column])
    bat["anomaly"] = 0
    bat = bat.dropna()
    bat['diff'] = bat[battery_column] - bat[battery_column].shift(3)
    bat_border = np.std(bat['diff']) * 2
    bat['anomaly'] = bat.apply(lambda x: 1 if x['diff'] < -bat_border else 0, axis=1)
    #plot_anomalies(bat, battery_column)
    return bat['anomaly']


# прогнозирование
def model_predict(model, x_test):
  return model.predict(x_test)


# посчитать разницы между предсказнными и оригинальными значениями
def get_differences(model, predicted, test):
  diff=[]
  for i in range(len(test)):
      pr = predicted[i][0]
      diff.append(abs(test[i]- pr))
  return diff


def any_anomaly(claster_num, seria):
    model = model_1
    if claster_num == 2:
        model = model_2
    if claster_num == 3:
        model = model_3
    predicted = model_predict(model, seria)
    diff = get_differences(model, predicted, seria)
    #




#не забыть потом приделать загрузку из БД
model_1 = load_model("cluster_1")
model_2 = load_model('cluster_2')
model_3 = load_model('cluster_3')


