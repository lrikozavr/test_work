import requests as re
import datetime as dt
import time
from types import SimpleNamespace
import pandas as pd
import os
import json
import threading

API_KEY = "96d20ebeb1689c1df6448ddf5b9d0e0e"
GET_WEATHER_PATH = "http://api.openweathermap.org"
GET_HISTORY_WEATHER_PATH = "https://history.openweathermap.org"

HISTORY_WEATHER_API_SUFFIX = "data/2.5/history"

WEATHER_API_SUFFIX = "data/2.5/weather"
GEO_API_DIRECT_SUFFIX = "geo/1.0/direct"
GEO_API_REVERSE_SUFFIX = "geo/1.0/reverse"

class WeatherReport():
    
    def __init__(self, city_name: str | None = None, lat: float | None = None, lon: float | None = None):
        self.site = SimpleNamespace()
        self.site.city_name = city_name
        self.site.lat = lat
        self.site.lon = lon
        
    def getCurrentUserSiteByIp(self):
        url = 'http://ipinfo.io/json'
        result = re.get(url).json()
        self.site.city_name = result["city"]
        self.site.lat = result["loc"].split(",")[0]
        self.site.lon = result["loc"].split(",")[1]
        self.site.postalcode = result["postal"]
        self.site.timezone = result["timezone"]
    
    def changeSite(self, city_name: str | None = None, lat: float | None = None, lon: float | None = None):
        if(not city_name == None):
            self.site.city_name = city_name
        if(not lat == None):
            self.site.lat = lat
        if(not lon == None):
            self.site.lon = lon

    def __check_server_responce__(self, res):
        if int(res['cod']) >= 400:
            try:
                message = res['message']
            except:
                message = "No server message"

            if(int(res['cod']) < 500):
                raise Exception(f"Client Error: {message}")
            if(int(res['cod']) > 500):
                raise Exception(f"Server Error: {message}")

    def getWeatherNow(self):
        try:
            result = re.get(url=f"{GET_WEATHER_PATH}/{WEATHER_API_SUFFIX}?q={self.site.city_name}&appid={API_KEY}").json()
            self.__check_server_responce__(result)
            return result
        except Exception as exc:
            if(self.site.lat == None and self.site.lon == None):
                raise
            if(not 'city not found' == str(exc).split(": ")[1]):
                raise Exception(exc)

        result = re.get(url=f"{GET_WEATHER_PATH}/{WEATHER_API_SUFFIX}?lat={self.site.lat}&lon={self.site.lon}&appid={API_KEY}").json()        
        self.__check_server_responce__(result)                        
        return result

    def getWeatherHistory(self, start, end = None):
        if(end == None):
            end = int(time.mktime(dt.datetime.now(dt.timezone.utc).timetuple()))
        if hasattr(self.site,'city_name'):
            result = re.get(url=f"{GET_HISTORY_WEATHER_PATH}/{HISTORY_WEATHER_API_SUFFIX}?q={self.site.city_name}&type=hour&start={start}&end={end}&appid={API_KEY}").json()
        elif hasattr(self.site,'lat') and hasattr(self.site,'lat'):
            result = re.get(url=f"{GET_WEATHER_PATH}/{WEATHER_API_SUFFIX}?lat={self.site.lat}&lon={self.site.lon}&type=hour&start={start}&end={end}&appid={API_KEY}").json()        

        self.__check_server_responce__(result)
        
        return result

    def getGeoSite(self, mod: str):
        if(mod == 'city'):
            result = re.get(url=f"{GET_WEATHER_PATH}/{GEO_API_DIRECT_SUFFIX}?q={self.site.city_name}&limit=1&appid={API_KEY}").json()
            #print(result)
            try:
                self.__check_server_responce__(result)
            except TypeError:
                self.site.lat = result[0]["lat"]
                self.site.lon = result[0]["lon"]
        #    raise
        if(mod == 'site'):
            result = re.get(url=f"{GET_WEATHER_PATH}/{GEO_API_REVERSE_SUFFIX}?lat={self.site.lat}&lon={self.site.lon}&limit=1&appid={API_KEY}").json()
            try:
                self.__check_server_responce__(result)
            except TypeError:
                self.site.city_name = result[0]["name"]

#------------------------------------------------------------------------------------------

WEATHER_DATABASE_COLUMNS = ['base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod',
       'request_time', 'coord.lon', 'coord.lat', 'weather.id',
       'weather.main', 'weather.description', 'weather.icon', 'main.temp',
       'main.feels_like', 'main.temp_min', 'main.temp_max',
       'main.pressure', 'main.humidity', 'main.sea_level',
       'main.grnd_level', 'wind.speed', 'wind.deg', 'wind.gust',
       'clouds.all', 'sys.country', 'sys.sunrise', 'sys.sunset']
WEATHER_DATABASE_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/weather_database.csv"
WEATHER_THREADS_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/threads"

def CollectWeatherReport(city_name: str | None = None):
    a = WeatherReport(city_name=city_name)

    #a.getCurrentUserSiteByIp()

    while 1:
        try:
            res = a.getWeatherNow()
        except Exception as exc:
            if(str(exc).split(': ')[1] == 'Nothing to geocode'):
                raise Exception(f"Empty city_name value")
            else:
                raise

        now = dt.datetime.now(dt.timezone.utc)
        res['request_time'] = int(now.timestamp())

        res['weather'] = res['weather'][0]
        data = pd.DataFrame(pd.json_normalize(res,max_level=1), columns=WEATHER_DATABASE_COLUMNS)
        if(os.path.isfile(WEATHER_DATABASE_PATH)):
            header_flag = False
        else:
            header_flag = True
        data.to_csv(WEATHER_DATABASE_PATH,header=header_flag,mode="a",index=False)

        now = dt.datetime.now(dt.timezone.utc)
        if(now.minute == 59):
            additional_hour = 2    
        else:
            additional_hour = 1
        
        if(now.hour+additional_hour >= 24):
            delay = dt.datetime(now.year,now.month,now.day+1,0,0,0,tzinfo=dt.timezone.utc).timestamp() - now.timestamp()            
        else:
            delay = dt.datetime(now.year,now.month,now.day,now.hour+additional_hour,0,0,tzinfo=dt.timezone.utc).timestamp() - now.timestamp()
        
        time.sleep(delay)
        
        #basik stop
        with open(WEATHER_THREADS_PATH,"r+") as thr:
            content = json.load(thr)            
            if(city_name in content['toend'] and city_name in content['start']):
                content['start'].remove(city_name)
                content['toend'].remove(city_name)
                thr.seek(0)
                thr.truncate(0)
                json.dump(content,thr)
                break

#------------------------------------------------------------------------------------------

class ThreadWithExcecution(threading.Thread):

    def __init__(self, target, args, daemon = False):
        threading.Thread.__init__(self)
        self.target = target
        self.args = args
        self.daemon = daemon

    def run(self):
        try:
            self.target(*self.args)
        except Exception as exc:
            self.exc = exc

    def join(self):
        threading.Thread.join(self)

        if hasattr(self,"exc"):
            raise Exception(self.exc)

def startWeatherThread(city_name: str = "Kyiv", deamon_flag: bool = False):

    with open(WEATHER_THREADS_PATH,"r+") as thr:
        content = json.load(thr)
        if(city_name in content['start']):
            raise Exception(f"Thread for city <{city_name}> already exist")
        else:
            collect = ThreadWithExcecution(target = CollectWeatherReport, args=(city_name,), daemon=deamon_flag)
            collect.start()
            time.sleep(0.15)
            if(collect.is_alive() == False):
                collect.join()
    
        content['start'].append(city_name)
        thr.seek(0)
        thr.truncate(0)
        json.dump(content,thr)

def endWeatherThread(city_name: str = "Kyiv"):

    with open(WEATHER_THREADS_PATH,"r+") as thr:
        content = json.load(thr)
        if(city_name in content['start']):
            if(not city_name in content['end']):
                content['toend'].append(city_name)
            else:
                raise Exception(f"Note of end Thread of city <{city_name}> already exist")
        else:
            raise Exception(f"Thread for city <{city_name}> not started yet")
        
        thr.seek(0)
        thr.truncate(0)
        json.dump(content,thr)

#------------------------------------------------------------------------------------------

def getWeatherFromDatabase(city_name: str, date: dt.datetime, timedelta: dt.timedelta):
    start = int(date.timestamp())
    end = int((date + timedelta).timestamp())

    data = pd.read_csv(WEATHER_DATABASE_PATH,header=0)
    data_temp = data[data['name'] == city_name]
    
    return data_temp[(data_temp['dt'] >= start) & (data_temp['dt'] <= end)].reset_index(drop=True)

def getJSONfromDataFrame(data: pd.DataFrame, flag: str = "index"):
    #
    def create_recursive_dict(name,value,i):
        if(i == len(name)):
            return value
        return dict({f'{name[i]}': create_recursive_dict(name,value,i+1)})
    #
    def change_recursive_dict(d,name,value,i,j):
        if(i == j-1):
            d[name[i]] = value
            return d
        d[name[i]] = change_recursive_dict(d[name[i]],name,value,i+1,j)
        return d

    if(flag == "index"):
        result = dict()
        for index in range(data.shape[0]):
            d = dict()

            for col in data.columns.values:
                if(not col.find('.') == -1):
                    n = col.split('.')
                    levels = [n[i] for i in range(len(n))]
                    #
                    if(levels[0] in d.keys()):
                        d[levels[0]] = change_recursive_dict(d[levels[0]],levels[1:],data.loc[index,col],0,len(n)-1)
                    else:
                        d[levels[0]] = create_recursive_dict(levels[1:],data.loc[index,col],0)
                else:
                    d[col] = data.loc[index,col]

            result[index] = d

    elif(flag == "value"):
            d = dict()

            for col in data.columns.values:
                if(not col.find('.') == -1):
                    n = col.split('.')
                    levels = [n[i] for i in range(len(n))]
                    #
                    if(levels[0] in d.keys()):
                        d[levels[0]] = change_recursive_dict(d[levels[0]],levels[1:],data[col].values,0,len(n)-1)
                    else:
                        d[levels[0]] = create_recursive_dict(levels[1:],data[col].values,0)
                else:
                    d[col] = data[col].values        

            result = d

    return result
