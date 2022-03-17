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

    parser.add_argument('--pw', 
                        metavar='password',
                        help='password for login')

    rargs = parser.parse_args()

    connstr = "mongodb://admax:%s@test0-shard-00-00-rrpjf.mongodb.net:27017,test0-shard-00-01-rrpjf.mongodb.net:27017,test0-shard-00-02-rrpjf.mongodb.net:27017/test?ssl=true&replicaSet=test0-shard-0&authSource=admin&retryWrites=true&w=majority" % rargs.pw

    #connstr = "mongodb://localhost:37017"

    client = MongoClient(connstr)
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
