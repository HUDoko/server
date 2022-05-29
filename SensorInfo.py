import requests
import pandas as pd
import DateConvertor as dc


# new file???
def url_is_valid(url):
    try:
        requests.get(url)
        return True
    except Exception as error:
        print('Cannot set sensor info from the url: ' + url + " :")
        print(repr(error))
        return False


def get_sensor_info_from_url(url, sep=" "):
    if url_is_valid(url):
        return pd.read_csv(url, sep=sep, header=None, names=["date", "time", "temp"])
    else:
        print('get_sensor_info_from_url returned empty DataFrame')
        return pd.DataFrame()


# get all sensor data
def get_sensor_info(sensor_info, first_date_time=None, sep=" ", res_period="1H"):
    sensor_info_tmp = dict()
    # data_count needs to check that all data was added
    data_count = 0
    # get DataFrames from the URL
    for sensor in sensor_info.keys():
        sensor_info_tmp[sensor] = get_sensor_info_from_url(sensor_info[sensor], sep)
        data_count += sensor_info_tmp[sensor].shape[0]
        # rename temp column for every sensor
        sensor_info_tmp[sensor] = sensor_info_tmp[sensor].rename(columns={"temp": sensor + "_temp"})
    # create summary df
    sensor_info_val = sensor_info_tmp.values()
    # convert values of time column to time
    df = pd.concat(sensor_info_val, sort=True)
    dc.df_add_datetime(df)
    # delete loaded data
    if first_date_time is not None:
        df = df[df['date_time'] >= first_date_time]
    # set index and resample
    df = df.set_index('date_time')
    df = df.resample(res_period).mean()
    return df.round(2)