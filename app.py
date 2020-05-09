import numpy as np 
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, func
from flask import Flask, jsonify 
from sqlalchemy.orm import Session
import datetime as dt
import pandas as pd
import json

app = Flask(__name__)


# Database set up 

engine = create_engine('sqlite:///Resources/hawaii.sqlite')

Base = automap_base()
Base.prepare(engine,reflect=True)


@app.route("/")
def home():
    return(
        "<br>Available Routes:</br>"
        "<br> <button> <a href=/api/v1.0/precipitation target=_blank> Precipitation Data </a> </button>"
        "<br> <button> <a href=/api/v1.0/stations target=_blank> Station Data </a> </button>"
        "<br> <button> <a href=/api/v1.0/tobs target=_blank> Temperature Observation Data </a> <button>"
        "<br><br>/api/v1.0/start_date"
        "<br>/api/v1.0/start_date/end_date"
        )

@app.route("/api/v1.0/precipitation")
def precip():
    session = Session(engine)

    measurements = Base.classes.measurement
    stations = Base.classes.station

    maxdate = session.query(func.max(measurements.date))[0][0]

    year = int(maxdate[:4])
    month = int(maxdate[5:7])
    day = int(maxdate[8:10])

    query_date = dt.date(year,month,day) - dt.timedelta(days=365)
    
    prcp_at_station = "USC00519281"

    # Design a query to retrieve the last 12 months of precipitation data
    last12months = session.query(measurements.date,measurements.prcp).\
        filter(measurements.date > query_date).\
        filter(measurements.station == prcp_at_station).all()

    # # Save the query results as a Pandas DataFrame,sort the df by date and set the index to the date column
    df = pd.DataFrame(last12months).sort_values('date').set_index('date')
    df = df.rename(columns={'prcp':'precipitation'})

    df = df.to_json(orient="table")

    return df

@app.route("/api/v1.0/stations")
def station():

    session = Session(engine)

    measurements = Base.classes.measurement
    stations = Base.classes.station

    station_name_code = session.query(stations.station,stations.name).group_by(stations.name).all()
    
    station_df = pd.DataFrame(station_name_code)

    station_json = station_df.to_json(orient='table')
    
    return station_json
    
@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    measurements = Base.classes.measurement
    stations = Base.classes.station

    maxdate = session.query(func.max(measurements.date))[0][0]

    year = int(maxdate[:4])
    month = int(maxdate[5:7])
    day = int(maxdate[8:10])

    query_date = dt.date(year,month,day) - dt.timedelta(days=365)

    stationmost = session.query(measurements.station,func.count(measurements.station)).\
        group_by(measurements.station).\
        order_by(func.count(measurements.station).desc()).first()

    temp = session.query(measurements.date,measurements.tobs).\
        filter(measurements.date > query_date).\
        filter(measurements.station==stationmost[0]).all()
    
    temp_df = pd.DataFrame(temp)

    temp_df = temp_df.rename(columns={"tobs":"Temperature Observation"})

    temp_df = temp_df.to_json(orient='table')

    return temp_df


if __name__ == '__main__':
    app.run(debug=True)