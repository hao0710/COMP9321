'''
COMP9321 2019 Term 1 Assignment Two Code Template
Student Name:
Student ID:
'''
import uuid
import os
from sqlite3 import OperationalError
from sqlalchemy import create_engine
import pandas as pd
from flask import Flask
from flask import request
from flask_restplus import Resource, Api
from flask_restplus import fields
from flask_restplus import reqparse
import requests
import json
import sqlite3
from datetime import datetime
from flask import g
from flask import jsonify


def clean(frame):
    dropcolumn = ["countryiso3code", "unit", "obs_status", "decimal"]
    frame.drop(dropcolumn, 1, inplace=True)
    indicator_id = frame["indicator"][0]["id"]
    indicator_value = frame["indicator"][0]["value"]
    frame.drop("indicator", 1, inplace=True)
    n = 0
    for i in frame["country"]:
        frame.loc[n, "country"] = i["value"]
        n += 1
    entries = frame.to_json(orient="records")
    cleaned_data = {
        "collection_id": str(uuid.uuid1()),
        "indicator": str(indicator_id),
        "indicator_value": str(indicator_value),
        "creation_time": str(datetime.now()),
        "entries": json.loads(entries)
    }
    return cleaned_data


def create_db(dbfile):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute(
        "CREATE TABLE collection (collection_id text,indicator text,indicator_value text,creation_time text,entries text)")
    conn.commit()
    conn.close()


def insert_db(dbfile):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    mydict = clean(dbfile)
    c.execute("INSERT INTO collection VALUES (?,?,?,?,?)",
              [mydict["collection_id"], mydict["indicator"], mydict["indicator_value"], mydict['creation_time'],
               json.dumps(mydict['entries'])])
    conn.commit()
    conn.close()


app = Flask(__name__)
api = Api(app,
          title="Collection Dataset",  # Documentation Title
          description="My Assignment 2, please click on the default to use bottom icons to test all my results .")  # Documentation Description
collection_model = api.model('collection', {'indicator_id': fields.String})
parser = reqparse.RequestParser()
parser.add_argument('q')


@api.route('/collection')
class collection(Resource):
    # post a collection
    @api.response(201, 'Collection Created Successfully')
    @api.response(200, 'Collection exist')
    @api.response(404, 'Collection does not exists')
    @api.doc(description="Add a new Collection")
    @api.expect(collection_model, validate=True)
    # question 1
    def post(self):
        payload = request.json
        if 'indicator_id' not in payload:
            api.abort(404, "missing indicator")
        indicator_id = payload["indicator_id"]
        url = "http://api.worldbank.org/v2/countries/all/indicators/{}?date=2013:2018&format=json&per_page=2000".format(
            indicator_id)
        obj = requests.get(url).json()
        if len(obj) == 1:
            return {"message": f"the input indicator id {indicator_id} doesn't exist in the data source!"}, 404
        original_data = obj[1]
        dbfile = pd.DataFrame(original_data)
        # if the data base not exists, create a new one. If exists, just insert the data
        if not os.path.isfile('./data.db'):
            create_db(dbfile)
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        # select the indicators in the database to see whether the indicator already exists in the database and store as i_id
        c.execute("SELECT indicator FROM collection")
        i_id = c.fetchall()
        if indicator_id not in json.dumps(i_id):
            insert_db(dbfile)

            sql_c_id = "SELECT collection_id FROM collection WHERE indicator =?"
            c.execute(sql_c_id, (indicator_id,))
            c_id = c.fetchall()
            c_id = [i[0] for i in c_id]

            sql_creation_time = "SELECT creation_time FROM collection WHERE indicator =?"
            c.execute(sql_creation_time, (indicator_id,))
            create_time = c.fetchone()
            conn.close()
            for i in c_id:
                return {
                           "location": "/collection/{}".format(i),
                           "collection_id": c_id[0],
                           "creation_time": create_time[0],
                           "indicator": indicator_id,
                       }, 201
        if indicator_id in json.dumps(i_id):  ## id exists in the database
            #insert_db(dbfile)
            c = conn.cursor()
            c.execute("SELECT collection_id FROM collection")
            c_id = c.fetchall()
            conn.close()
            c_id = [i[0] for i in c_id]
            return {
                       "location": "collection/{}".format(c_id)
                   }, 200

    # #question 3
    # @api.route('/collection')
    # class list_collection(Resource):
    @api.response(200, 'collection retrived successfully')
    @api.response(404, 'the database is empty')
    @api.doc(description='Geting a list of collection exists in the databse')
    def get(self):
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute('SELECT * from collection')
        flag = c.fetchall()
        conn.close()
        if not flag:
            return {"message": "collection is empty"}, 404
        result = []
        # c.execute("CREATE TABLE collection (collection_id text,indicator text,indicator_value text,creation_time text,entries text)")
        for i in flag:
            tmp = {
                "location": "/collection/{}".format(i[0]),
                "collection_id": i[0],
                "creation_time": i[3],
                "indicator": i[1],
            }
            result.append(tmp)
        return result, 200


@api.route('/collection/<string:collection_id>')
class delete_or_get_one_collection(Resource):
    @api.response(200, 'Collection is removed from databse')
    @api.response(404, 'Collection does not exists in database')
    @api.doc(description='Delete a collection by its collection_id')
    # question 2
    def delete(self, collection_id):
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute("SELECT collection_id FROM collection")
        flag = c.fetchall()
        if not flag:
            return {"message": "the collection does not exists in the database"}, 404
        flag = [i[0] for i in flag]
        for k in flag:
            if collection_id == k:
                delete_sql_c_id = "DELETE FROM collection WHERE collection_id=?"
                c.execute(delete_sql_c_id, (collection_id,))
                conn.commit()
                conn.close()
                return {"message": "Collection = {} is removed from the database!".format(collection_id)}, 200
            else:
                return {"message": "Collection = {} is not exists in the database".format(collection_id)}, 404

    @api.response(404, 'the collection does not exists in the database')
    @api.response(200, 'get one collection based on the collection_id')
    @api.doc(description='Get a collection based on collection_id')
    def get(self, collection_id):
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute("SELECT collection_id FROM collection")
        flag = c.fetchall()
        if not flag:
            return {"message": "the collection does not exists in the database"}, 404
        flag = [i[0] for i in flag]
        for k in flag:
            if collection_id == k:
                sql_indicator = "SELECT indicator FROM collection WHERE collection_id=?"
                c.execute(sql_indicator, (collection_id,))
                indicator = c.fetchone()

                sql_indicator_value = "SELECT indicator_value FROM collection WHERE collection_id=?"
                c.execute(sql_indicator_value, (collection_id,))
                indicator_value = c.fetchone()

                sql_entries = "SELECT entries FROM collection WHERE collection_id=?"
                c.execute(sql_entries, (collection_id,))
                entries = c.fetchall()

                sql_creation_time = "SELECT creation_time FROM collection WHERE collection_id=?"
                c.execute(sql_creation_time, (collection_id,))
                creation_time = c.fetchone()
                conn.close()
                entries = [json.loads(i) for i in entries[0]]
                return {
                           "collection_id": collection_id,
                           "indicator": indicator[0],
                           "indicator_value": indicator_value[0],
                           "creation_time": creation_time[0],
                           "entries": entries
                       }, 200
            else:
                return {"message": "Collection = {} is not exists in the database".format(collection_id)}, 404


@api.route('/collection/<string:collection_id>/<string:year>/<string:country>')
class q5(Resource):
    @api.response(200, 'Collection Retrived')
    @api.response(404, 'Collection does not exists in database')
    @api.response(400, "wrong input for year or country,check again")
    @api.doc(description='Retrieve economic indicator value for given country and a year')
    def get(self, collection_id, year, country):
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute("SELECT collection_id FROM collection")
        flag = c.fetchall()
        if not flag:
            return {"message": "the collection does not exists in the database"}, 404
        flag = [i[0] for i in flag]
        if int(year) not in [2013, 2014, 2015, 2016, 2017, 2018]:
            return {"message": "Wrong input"}, 400
        for k in flag:
            if collection_id == k:
                sql_indicator = "SELECT indicator FROM collection WHERE collection_id=?"
                c.execute(sql_indicator, (collection_id,))
                indicator = c.fetchone()

                sql_entries = "SELECT entries FROM collection WHERE collection_id=?"
                c.execute(sql_entries, (collection_id,))
                entries = c.fetchall()
                conn.close()
                entries = [json.loads(i) for i in entries[0]]
                for i in entries:
                    for j in i:
                        if j['country'] == country and j['date'] == year:
                            nation = j['country']
                            gdp = j['value']
                            time = j['date']
                            return {
                                       "collection_id": collection_id,
                                       "indicator": indicator[0],
                                       "country": f"{nation}",
                                       "year": f"{time}",
                                       "value": f"{gdp}"
                                   }, 200
                        # else:
                        #     return {"message" :" Wrong input for country or year"},400

            else:
                return {"message": "Collection = {} is not exists in the database".format(collection_id)}, 404


@api.route('/collection/<string:collection_id>/<string:year>')
class q6(Resource):
    @api.response(200, 'Retrieve Collection Successfully')
    @api.response(404, 'Collection not existed in the Dataset')
    @api.response(400, 'Wrong input')
    @api.doc(description="Retrieve a Top/Bottom Collection by year")
    @api.expect(parser)
    def get(self, collection_id, year):
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute("SELECT collection_id FROM collection")
        flag = c.fetchall()
        if not flag:
            return {"message": "Collection does not exists in the database"}, 404
        args = parser.parse_args()
        q = args.get('q')
        if q:  # we have q operator
            if q.startswith('top') or q.startswith('bottom'):
                if q.startswith('top'):
                    top = 1
                else:
                    top = 0
                N_tmp = []
                for i in list(q):
                    if i.isdigit():
                        N_tmp.append(i)
                if len(N_tmp) > 0:  # we have an request for output limited records
                    # print("I am here")
                    N = ''.join(N_tmp)
                    N = int(N)
                    if N < 1 or N > 100 or int(year) not in [2013, 2014, 2015, 2016, 2017, 2018]:
                        return {"message": "Wrong input"}, 400
                    conn = sqlite3.connect('data.db')
                    flag = [i[0] for i in flag]
                    tmp = []
                    result = []
                    for k in flag:
                        if collection_id == k:
                            sql_indicator = "SELECT indicator FROM collection WHERE collection_id=?"
                            c.execute(sql_indicator, (collection_id,))
                            indicator = c.fetchone()

                            sql_indicator_value = "SELECT indicator_value FROM collection WHERE collection_id=?"
                            c.execute(sql_indicator_value, (collection_id,))
                            indicator_value = c.fetchone()

                            sql_entries = "SELECT entries FROM collection WHERE collection_id=?"
                            c.execute(sql_entries, (collection_id,))
                            entries = c.fetchall()
                            conn.close()
                            entries = [json.loads(i) for i in entries[0]]
                            for i in entries:
                                for j in i:
                                    # if year not in j['date']:
                                    #     return {"message": "Wrong input"},400
                                    tmp.append(j)
                            frame = pd.DataFrame.from_dict(tmp, orient='columns')
                            frame = frame.loc[frame['date'] == year]
                            # entries=frame.to_dict('records')

                            if top == 1:
                                frame = frame.sort_values(by='value', ascending=False).head(N).to_json(orient='records')
                            else:
                                frame = frame.sort_values(by='value', ascending=False).tail(N).to_json(orient='records')
                            frame = json.loads(frame)
                            return {
                                       "indicator": indicator[0],
                                       "indicator_value": indicator_value[0],
                                       "entries": frame
                                   }, 200

                        else:

                            return {"message": "Collection not existed in the Dataset"}, 404
                else:
                    return {"message": "wrong input"}, 400
            else:
                return {"message": "wrong input"}, 400
        else:  # we don't have q. print entire frame
            conn = sqlite3.connect('data.db')
            c = conn.cursor()
            c.execute("SELECT collection_id FROM collection")
            if int(year) not in [2013, 2014, 2015, 2016, 2017, 2018]:
                return {"message": "Wrong input"}, 400
            flag = c.fetchall()
            flag = [i[0] for i in flag]
            tmp = []
            result = []
            for k in flag:
                if collection_id == k:
                    sql_indicator = "SELECT indicator FROM collection WHERE collection_id=?"
                    c.execute(sql_indicator, (collection_id,))
                    indicator = c.fetchone()
                    sql_indicator_value = "SELECT indicator_value FROM collection WHERE collection_id=?"
                    c.execute(sql_indicator_value, (collection_id,))
                    indicator_value = c.fetchone()
                    sql_entries = "SELECT entries FROM collection WHERE collection_id=?"
                    c.execute(sql_entries, (collection_id,))
                    entries = c.fetchall()
                    conn.close()
                    entries = [json.loads(i) for i in entries[0]]
                    for i in entries:
                        for j in i:
                            # if year not in j['date']:
                            #     return {"message": "Wrong input"},400
                            tmp.append(j)
                    frame = pd.DataFrame.from_dict(tmp, orient='columns')
                    frame = frame.loc[frame['date'] == year]
                    # entries=frame.to_dict('records')
                    frame = frame.to_json(orient='records')
                    frame = json.loads(frame)
                    return {
                               "indicator": indicator[0],
                               "indicator_value": indicator_value[0],
                               "entries": frame
                           }, 200
                else:
                    return {"message": "Collection not existed in the Dataset"}, 404


if __name__ == '__main__':
    app.run(debug=True)
