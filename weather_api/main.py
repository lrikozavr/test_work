# Using flask to make an api 
# import necessary libraries and functions 
from flask import Flask, jsonify, request, Response
from werkzeug.exceptions import HTTPException

import json
import numpy as np
import datetime as dt
from weather import getWeatherFromDatabase, getJSONfromDataFrame, startWeatherThread, endWeatherThread
from weather import WEATHER_THREADS_PATH

# creating a Flask app 
app = Flask(__name__) 

#token = lambda col,row: [''.join(np.trunc(np.random.random(col)*9.999).astype(int).astype(str).tolist()) for i in range(row)]
#token(32,5)
xtoken = ['49793382777636623217232494406695',
        '15344871970428912899495228710644',
        '15950954456471276192448529958197',
        '51939968850156653880393761735549',
        '73188428144815618702716054089301']

json.dump({"start": [], "toend": []}, open(WEATHER_THREADS_PATH,'w'))

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

return_execution = lambda code,name,description: json.dumps({
                                                                "code": code,
                                                                "name": name,
                                                                "description": description,
                                                            })

@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response

@app.route('/weather/track/city=<string:city_name>&status=<string:status>&apitoken=<string:apitoken>', methods = ['POST'])
def track(city_name,status,apitoken):
    if(apitoken in xtoken):
        if(status in ['start','stop']):
            if(status == 'start'):
                try:
                    startWeatherThread(city_name=city_name,deamon_flag=False)
                    return return_execution(200,"OK","Done"), 200
                except Exception as exc:
                    return return_execution(406,"Not Acceptable",str(exc)), 406
            else:
                try:
                    endWeatherThread(city_name=city_name)
                    return return_execution(200,"OK","Done"), 200
                except Exception as exc:
                    return return_execution(406,"Not Acceptable",str(exc)), 406
        else:
            return return_execution(406,"Not Acceptable",f"Incorrect status value. Your <{status}>, but option is ['start','stop']"), 406
    else:
        return return_execution(401,"Unauthorized","Incorrect api token"), 401

# on the terminal type: curl http://127.0.0.1:5000
@app.route('/weather/city=<string:city_name>&date=<string:date>&format=<string:flag>&apitoken=<string:apitoken>', methods = ['GET']) 
def disp(city_name,date,flag,apitoken): 
    if(apitoken in xtoken):
        try:
            req_date = dt.datetime.fromisoformat(date)
        except ValueError as ver:
            return return_execution(406,"Not Acceptable",str(ver)), 406
        if(not flag in ['index','value']):
            return return_execution(406,"Not Acceptable","Unexpected format. Choose one of ['index','value']"), 406
        
        req_delta = dt.timedelta(days=1)
    
        data = getWeatherFromDatabase(city_name,req_date,req_delta)
        if(data.shape[0] == 0):
            return return_execution(406,"Not Acceptable",f"Database dont have city that call: <{city_name}>"), 406
        
        result = getJSONfromDataFrame(data,flag)
        
        return json.dumps(result, cls=NpEncoder)
    else:
        return return_execution(401,"Unauthorized","Incorrect api token"), 401

  
# driver function 
if __name__ == '__main__': 
  
    app.run(debug = True) 