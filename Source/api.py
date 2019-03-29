import sqlite3
import json
import requests, re
from flask import Flask, make_response, jsonify, request
from flask_restplus import Resource, Api, fields, reqparse
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

def create_db(db_file):
    '''
    uase this function to create a db, don't change the name of this function.
    db_file: Your database's name.
    '''
    try:
        conn = sqlite3.connect(db_file)
        db.create_all()
    except Error as e:
        print(e)
    finally:
        conn.close()

'''
Put your API code below. No certain requriement about the function name as long as it works.
'''

valid_indicators = ["TX.VAL.AGRI.ZS.UN", "TM.VAL.AGRI.ZS.UN", "SP.RUR.TOTL.ZS", "SP.RUR.TOTL.ZG", "SP.RUR.TOTL", "SL.AGR.EMPL.ZS", "SL.AGR.EMPL.MA.ZS", "SL.AGR.EMPL.FE.ZS", "SI.POV.RUHC", "SI.POV.RUGP", "NV.AGR.TOTL.ZS", "NV.AGR.TOTL.CD", "ER.H2O.FWAG.ZS", "EN.POP.EL5M.RU.ZS", "EN.ATM.NOXE.AG.ZS", "EN.ATM.NOXE.AG.KT.CE", "EN.ATM.METH.AG.ZS", "EN.ATM.METH.AG.KT.CE", "EG.ELC.ACCS.RU.ZS", "AG.YLD.CREL.KG", "AG.SRF.TOTL.K2", "AG.PRD.LVSK.XD", "AG.PRD.FOOD.XD", "AG.PRD.CROP.XD", "AG.PRD.CREL.MT", "AG.LND.TRAC.ZS", "AG.LND.TOTL.RU.K2", "AG.LND.TOTL.K2", "AG.LND.PRCP.MM", "AG.LND.IRIG.AG.ZS", "AG.LND.FRST.ZS", "AG.LND.FRST.K2", "AG.LND.EL5M.RU.ZS", "AG.LND.EL5M.RU.K2", "AG.LND.CROP.ZS", "AG.LND.CREL.HA", "AG.LND.ARBL.ZS", "AG.LND.ARBL.HA.PC", "AG.LND.ARBL.HA", "AG.LND.AGRI.ZS", "AG.LND.AGRI.K2", "AG.CON.FERT.ZS", "AG.CON.FERT.PT.ZS", "AG.AGR.TRAC.NO", "NY.GDP.MKTP.CD"]


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
db = SQLAlchemy(app)

api = Api(app, title='Data Service for World Bank Economic Indicators', description='COMP9321 Assignment 2 - Prajwal Rao (z5176504)')

post_parser = reqparse.RequestParser()
post_parser.add_argument('indicator_id',  type=str, help='''Parameters to be given: an indicator
(http://api.worldbank.org/v2/indicators)''')

@api.route("/collections/")
class Collections(Resource):

    @api.response(200, 'OK')
    def get(self):
        """
        Retrieve the list of available collections
        """
        msg = get_collections()
        if msg:
            return make_response(jsonify(msg), 200)
        else:
            return make_response("No collections found. Try importing a new one!", 200)

    @api.expect(post_parser)
    @api.response(201, 'Created')
    @api.response(200, 'OK')
    @api.response(400, 'Invalid input')
    @api.response(500, 'API call error')
    def post(self):
        """
        Import a collection from the data service
        """

        args = post_parser.parse_args()
        indicator_id = args["indicator_id"]

        if indicator_id not in valid_indicators:
            return make_response("Invalid Indicator", 400) #use abort(400) ?

        collection_exists = CollectionsDB.query.filter_by(indicator_id = indicator_id).first()

        if collection_exists is None:  # Doesn't Exist, request new data
            response = callAPI(indicator_id)
            if response:
                parsed_response = json_parser(response, indicator_id)
                new_collection_id = insert_data(parsed_response)
                return make_response(jsonify(get_collection_by_id(new_collection_id)), 201)
            else:
                return make_response("No response recieved from API", 500) # internal server error

        else: # Already exists, retrieve data
            collection_id = collection_exists.collection_id
            return make_response(jsonify(get_collection_by_id(collection_id)), 200)

@api.route("/collections/<int:id>")
class Collection(Resource):
    @api.response(200, 'OK')
    @api.response(400, 'Invalid input')
    def get(self, id):
        """
        Retrieve a collection
        """
        msg = get_collection_by_id(id)
        if msg:
            return make_response(jsonify(msg), 200)
        else:
            return make_response("Collection does not exist!", 400)

    @api.response(200, 'OK')
    @api.response(400, 'Invalid input')
    def delete(self, id):
        """
        Deleting a collection with the data service
        """
        msg = delete_collection_by_id(id)
        if msg:
            return make_response(jsonify(msg), 200)
        else:
            return make_response("Collection does not exist!", 400)

@api.route("/collections/<int:id>/<int:year>/<string:country>")
class Collection(Resource):
    @api.response(200, 'OK')
    @api.response(400, 'Invalid input')
    def get(self, id, year, country):
        """
        Retrieve economic indicator value for given country and a year
        """
        msg = get_economic_indicator(id, country, year)
        if msg:
            return make_response(jsonify(msg), 200)
        else:
            return make_response("Does not exist!", 400)

@api.route("/collections/<int:id>/<int:year>")
class Collection(Resource):
    @api.response(200, 'OK')
    @api.response(400, 'Invalid input')
    def get(self, id, year):
        """
        Retrieve top/bottom economic indicator values for a given year
        """
        query = request.args.get('q')
        msg = get_economic_indicators(id, year, query)
        if msg:
            return make_response(jsonify(msg), 200)
        else:
            return make_response("Does not exist!", 400)

##################################################################
#DB

class CollectionsDB(db.Model):
    __tablename__ = 'CollectionsDB'
    collection_id = db.Column(db.Integer, primary_key = True)
    indicator_id = db.Column(db.String(100), nullable = False)
    indicator_val = db.Column(db.String(200))
    creation_time = db.Column(db.String(200), nullable = False)
    entries = db.relationship('EntriesDB', backref = 'CollectionsDB', lazy = True)

    def __repr__(self):
        return '{}, {}, {}, {}'.format(self.collection_id, self.indicator_id, self.indicator_val, self.creation_time)

class EntriesDB(db.Model):
    __tablename__ = 'EntriesDB'
    entry_id = db.Column(db.Integer, primary_key = True)
    collection_id = db.Column(db.Integer, db.ForeignKey('CollectionsDB.collection_id'), nullable = False)
    country = db.Column(db.String(100))
    date = db.Column(db.String(10))
    value = db.Column(db.String(50))
    collection = db.relationship("CollectionsDB", backref = "EntriesDB")

    def __repr__(self):
        return '{}, {}, {}, {}, {}'.format(self.entry_id, self.collection_id, self.country, self.date, self.value)

def insert_data(parsed_op):

    db_c = CollectionsDB.query.order_by(CollectionsDB.collection_id.desc()).first() # get last added collection id
    if db_c:
        collection_id = db_c.collection_id + 1
    else:
        collection_id = 1
    indicator_id = parsed_op["indicator"]
    indicator_val = parsed_op["indicator_value"]
    creation_time = parsed_op["creation_time"]

    new_collection = CollectionsDB(collection_id = collection_id, indicator_id = indicator_id, indicator_val = indicator_val, creation_time = creation_time)
    db.session.add(new_collection)
    db.session.commit()

    for e in parsed_op["entries"]:
        new_entry = EntriesDB(collection_id = collection_id, country = e["country"], date = e["date"], value = e["value"])
        db.session.add(new_entry)
        db.session.commit()

    return collection_id

def get_collections():
    db_c = CollectionsDB.query.all()

    if db_c == []:
        return None

    op = []

    for c in db_c:
        f = {
                "location" : "/<collections>/" + str(c.collection_id),
                "collection_id" : c.collection_id,
                "creation_time" : c.creation_time,
                "indicator": c.indicator_id
            }
        op.append(f)
    return op

def get_collection_by_id(id):

    c = CollectionsDB.query.filter_by(collection_id = int(id)).first()

    if c is None:
        return None

    formatted_op = {
        "collection_id" : c.collection_id,
        "indicator": c.indicator_id,
        "indicator_value": c.indicator_val,
        "creation_time" : c.creation_time
        }

    entries = []
    db_entries = EntriesDB.query.filter_by(collection_id = int(id)).all()
    for e in db_entries:
        entry = {
            "country": e.country,
            "date": e.date,
            "value": e.value
        }
        entries.append(entry)

    formatted_op["entries"] = entries

    return formatted_op

def delete_collection_by_id(id):

    db_c = CollectionsDB.query.filter_by(collection_id = int(id)).all()
    if db_c == []:
        return None

    db_e = EntriesDB.query.filter_by(collection_id = int(id)).all()
    for e in db_e:
        db.session.delete(e)

    for c in db_c:
        db.session.delete(c)

    db.session.commit()

    formatted_op = {
        "message" :"Collection = "+ str(id) +" is removed from the database!"
    }
    return formatted_op

def get_economic_indicator(id, country, year):

    e = EntriesDB.query.filter_by(collection_id = int(id), date = str(year), country = country).first()
    if e is None:
        return None

    c = CollectionsDB.query.filter_by(collection_id = int(id)).first()

    formatted_op = {
        "collection_id": str(e.collection_id),
        "indicator" : c.indicator_id,
        "country": e.country,
        "year": e.date,
        "value": e.value
    }

    return formatted_op

def get_economic_indicators(id, year, query = None):
    '''
    {
       "indicator": "NY.GDP.MKTP.CD",
       "indicator_value": "GDP (current US$)",
       "entries" : [
                      {
                         "country": "Arab World",
                         "date": "2016",
                         "value": 2513935702899.65
                      },
                      ...
                   ]
    }
    '''
    if query is None:
        db_e = EntriesDB.query.filter_by(collection_id = int(id), date = str(year)).filter(EntriesDB.value != "None").all()
        if db_e == []:
            return None

        c = CollectionsDB.query.filter_by(collection_id = int(id)).first()

        formatted_op = {
            "indicator": c.indicator_id,
            "indicator_value": c.indicator_val
        }

        entries = []

        for e in db_e:
            entry = {
                "country": e.country,
                "date": e.date,
                "value": e.value
            }
            entries.append(entry)
        formatted_op["entries"] = entries

        return formatted_op

    else:
        print(query)
        query = query.lower()
        match = re.search(r'([a-z]+)(\d+)', query)
        if match:
            type = match.group(1)
            range = int(match.group(2))

            if type == "top":
                print(type, range)

                db_e = EntriesDB.query.filter_by(collection_id = int(id), date = str(year)).order_by(EntriesDB.value.desc()).limit(range).all()
                if db_e == []:
                    return None

            elif type == "bottom":
                print(type, range)
                db_e = EntriesDB.query.filter_by(collection_id = int(id), date = str(year)).order_by(EntriesDB.value).limit(range).all()
                if db_e == []:
                    return None

            else:
                return None

            c = CollectionsDB.query.filter_by(collection_id = int(id)).first()

            formatted_op = {
                "indicator": c.indicator_id,
                "indicator_value": c.indicator_val
            }

            entries = []

            for e in db_e:
                entry = {
                    "country": e.country,
                    "date": e.date,
                    "value": e.value
                }
                entries.append(entry)
            formatted_op["entries"] = entries

            return formatted_op

##################################################################

def callAPI(indicator):
    response = requests.get("http://api.worldbank.org/v2/countries/all/indicators/" + indicator + "?date=2013:2018&format=json&per_page=100")
    if response.status_code != 200:
        print( 'Error {}'.format(response.status_code))
        return None
    return response.json()

def json_parser(dump, id):
    header = dump[0]
    contents = dump[1]

    indicator = contents[0]["indicator"]["id"]
    indicator_value = contents[0]["indicator"]["value"]

    collection_id = id

    formatted_op = {
        "collection_id" : collection_id,
        "indicator": indicator,
        "indicator_value": indicator_value,
        "creation_time" : datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        }

    entries = []

    for content in contents:
        country = content["country"]["value"]
        date = content["date"]
        value = str(content["value"])
        entry = {
            "country": country,
            "date": date,
            "value": value
        }
        entries.append(entry)

    formatted_op["entries"] = entries

    return formatted_op

if __name__ == '__main__':
    create_db("data.db") #db.create_all()
    app.run(debug = True)
