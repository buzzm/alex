import json

import argparse
import sys
import datetime

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

    print("info is", rargs.info)

    
    aggs = {}
    with open(rargs.datafile, "r") as fd:
        for line in fd:
            obj = json.loads(line)

            td = datetime.datetime.strptime(obj['common']['td'], "%Y-%m-%dT%H:%M:%S.000Z")
            if td > datetime.datetime(2021,6,1):
                k = obj['data']['port']
                if k not in aggs:
                    aggs[k] = {'cnt':0, 'total_eff':0}

                one_agg = aggs[k]

                one_agg['cnt'] += 1
                one_agg['total_eff'] += obj['data']['risk']['eff']

    print(aggs)
    
            
if __name__ == "__main__":
    main(sys.argv)
