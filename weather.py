import json
from datetime import datetime
from io import BytesIO

import matplotlib as mpl
import numpy as np
import pandas as pd
import requests
from matplotlib import pyplot as plt


def get_weather_df(username, password, port, url, zipcode):
    line = 'https://'+username+':'+password+'@twcservice.mybluemix.net:'+port+'/api/weather/v1/location/'+zipcode+'%3A4%3AUS/forecast/hourly/48hour.json?units=m&language=en-US'
    raw = requests.get(line)
    weather = json.loads(raw.text)
    df = pd.DataFrame.from_dict(weather['forecasts'][0],orient='index').transpose()
    for forecast in weather['forecasts'][1:]:
      df = pd.concat([df, pd.DataFrame.from_dict(forecast,orient='index').transpose()])

    time = np.array(df['fcst_valid_local'])
    for row in range(len(time)):
      time[row] = datetime.strptime(time[row], '%Y-%m-%dT%H:%M:%S%z')

    df = df.set_index(time)
    return df


def get_weather_plots(df):
    plt.ioff()
    df['rain'] = df['pop'].as_matrix()

    tmean = pd.rolling_mean(df['temp'], window=4, center=True)
    rhmean = pd.rolling_mean(df['rh'], window=4, center=True)
    cldsmean = pd.rolling_mean(df['clds'], window=4, center=True)
    wspdmean = pd.rolling_mean(df['wspd'], window=4, center=True)

    mpl.style.use('bmh')

    fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(8, 10))

    df['temp'].plot(ax=axes[0], color='dodgerblue',sharex=True)
    tmean.plot(ax=axes[0], kind='line',color='darkorchid', sharex=True)
    axes[0].set_ylabel('temperature [$^o$C]')

    df['rain'].plot(ax=axes[1], color='dodgerblue',sharex=True)
    axes[1].set_ylabel('chance of rain [%]')

    df['rh'].plot(ax=axes[2], color='dodgerblue',sharex=True)
    rhmean.plot(ax=axes[2], kind='line',color='darkorchid', sharex=True)
    axes[2].set_ylabel('humidity [%]')

    df['clds'].plot(ax=axes[3], color='dodgerblue',sharex=True)
    cldsmean.plot(ax=axes[3], kind='line',color='darkorchid', sharex=True)
    axes[3].set_ylabel('clouds [%]')

    df['wspd'].plot(ax=axes[4], color='dodgerblue',sharex=False)
    wspdmean.plot(ax=axes[4], kind='line',color='darkorchid', sharex=True)
    axes[4].set_ylabel('wind [m s$^{-1}$]')

    weatherdat = BytesIO()
    fig.savefig(weatherdat, format='png')
    weatherdat.seek(0)
    return weatherdat