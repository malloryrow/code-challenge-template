import sqlite3
from flask import Flask, request, jsonify, g
import statistics
import os
from flasgger import Swagger, swag_from

app = Flask(__name__)
RAW_DATABASE = '../raw_wx_data.db'
STATS_DATABASE = '../stats_wx_data.db'

# Configuring Swagger
template = {
    "swagger": "2.0",
    "info": {
      "title": "Corteva Code Challenge Weather API",
      "description": "This API was developed by Mallory Row (mallory.row@gmail.com) for the Corteva Code Challenge.",
      "version": "1.0"
    }
}
app.config['SWAGGER'] = {
    'title': 'Corteva Code Challenge Weather API',
    'uiversion': 2,
    'template': './resources/flasgger/swagger_ui.html'
}
swagger = Swagger(app, template=template)

#### Connect and read to databases
def get_db(DATABASE):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

def init_raw_db():
    if not os.path.exists(RAW_DATABASE):
        with app.app_context():
            raw_db = get_db(RAW_DATABASE)

def init_stats_db():
    if not os.path.exists(STATS_DATABASE):
        with app.app_context():
            stats_db = get_db(STATS_DATABASE)

#### Routes
@app.route('/api/weather/', methods=['GET'])
def weather():
    """
    This displays daily weather data.
    The stations are in Nebraska, Iowa, Illinois, Indiana, or Ohio.
    There are many entries but the default limit is to 10.
    ---
    tags:
      - Weather
    responses:
      200:
        description: Daily weather data
        examples:
          application/json:
            status: success
            data:
              Date: YYYYMMDD format
              MaxT: maximum temperature (Celsius)
              MinT: minimum temperature (Celsius)
              Precip: Total precipitation (millimeters)
              StationID: station identification
    """
    raw_db = get_db(RAW_DATABASE)

    if request.method == 'GET':
        raw_query = 'SELECT * FROM WX_Data_Raw WHERE 1=1'
        raw_params = []
        # Query Station ID
        station_id = request.args.get('StationID')
        if station_id:
            raw_query += ' AND StationID = ?'
            raw_params.append(station_id)
        # Query Date
        date = request.args.get('Date')
        if date:
            raw_query += ' AND Date = ?'
            raw_params.append(date)
        # Pages
        limit = request.args.get('limit', default=10, type=int)
        offset = request.args.get('offset', default=0, type=int)
        raw_query += ' LIMIT ? OFFSET ?'
        raw_params.extend([limit, offset])
        # Filter and display
        raw_cur = raw_db.execute(raw_query, raw_params)
        raw_rows = raw_cur.fetchall()
        return jsonify([dict(row) for row in raw_rows]), 200

@app.route('/api/weather/stats/', methods=['GET'])
def stats():
    """
    This displays annual weather data.
    The stations are in Nebraska, Iowa, Illinois, Indiana, or Ohio.
    There are many entries but the default limit is to 10.
    ---
    tags:
      - Weather
    responses:
      200:
        description: Annual weather data
        examples:
          application/json:
            status: success
            data:
              AvgMaxT: yearly average maximum temperature (Celsius)
              AvgMinT: yearly average minimum temperature (Celsius)
              TotalPrecip: Total yearly precipitation (millimeters)
              Year: YYYY format
              StationID: station identification
    """
    stats_db = get_db(STATS_DATABASE)

    if request.method == 'GET':
        stats_query = 'SELECT * FROM WX_Data_Stats WHERE 1=1'
        stats_params = []
        # Query Station ID
        station_id = request.args.get('StationID')
        if station_id:
            stats_query += ' AND StationID = ?'
            stats_params.append(station_id)
        # Query Year
        year = request.args.get('Year')
        if year:
            stats_query += ' AND Year = ?'
            stats_params.append(year)
        # Pages
        limit = request.args.get('limit', default=10, type=int)
        offset = request.args.get('offset', default=0, type=int)
        stats_query += ' LIMIT ? OFFSET ?'
        stats_params.extend([limit, offset])
        # Filter and display
        stats_cur = stats_db.execute(stats_query, stats_params)
        stats_rows = stats_cur.fetchall()
        return jsonify([dict(row) for row in stats_rows]), 200

if __name__ == '__main__':
    init_raw_db()
    init_stats_db()
    app.run(debug=True)
