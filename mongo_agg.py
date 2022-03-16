from pymongo import MongoClient

import json

import argparse
import datetime
import sys


def main(args):
    parser = argparse.ArgumentParser(description=
"""Documentation for this program"""
   )

    parser.add_argument('datafile', metavar='fileName',
                   help='bla bla bla')

    parser.add_argument('--info',
                        action='store_true',  # boolean
                        help='TBD')

    parser.add_argument('--stringOption', 
                        metavar='zzz',
                        help='more help')

    rargs = parser.parse_args()

    client = MongoClient("mongodb://localhost:37017")
    db = client.testX

    db.foo.drop()

    with open(rargs.datafile, "r") as fd:
        for line in fd:
            obj = json.loads(line)
            obj['common']['td'] = datetime.datetime.strptime(obj['common']['td'], "%Y-%m-%dT%H:%M:%S.000Z")
            db.foo.insert_one(obj)


            
    pipeline = [
        {'$match': {'common.td':{'$gt':datetime.datetime(2021,6,1)}}},
        {'$group': {'_id': '$data.port', 'cnt':{'$sum':1}, 'total_eff':{'$sum':'$data.risk.eff'}}},
        {'$addFields': {'dad': {'$add':['$total_eff',77]}}},
        {'$addFields': {'mom': {'$add':['$dad',-5]}}}
        
    ]
    for doc in db.foo.aggregate(pipeline):
        print(doc)
            
            
if __name__ == "__main__":
    main(sys.argv)
