import asyncio
import argparse


from dbArchiver import ENV_DB_URL
import os
from pathlib import Path
#from os import makedirs, mkdir,symlink,remove
import yaml
import json
from datetime import datetime
import codecs
import glob
from enum import Enum
from autobahn.asyncio.component import Component, run
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from storage.schema import Event,WampData

ENV_DB_URL="DB_URL"

eng = create_engine(os.environ.get(ENV_DB_URL), pool_pre_ping=True, echo_pool=True)

Session = sessionmaker(bind=eng)
print("Plain called and init done")

def read_manifest(eventKey):
    with eng.connect() as con:
        dbSession = Session(bind=con)
        x = dbSession.query(Event).filter_by(EventKey=eventKey).first()    
        if (x != None):
            return x.Data['manifests']
        else:
            return {}

def read_events():
    with eng.connect() as con:
        dbSession = Session(bind=con)
        res = dbSession.query(Event).order_by(Event.RecordDate.desc()).all()   
        return [item.toDict() for item in res]
            
        
def read_wamp_data(eventId=None,tsBegin=None, num=10):
    with eng.connect() as con:        
        res = con.execute(text("""
        select data from wampdata 
        where event_id=:eventId and (data->'timestamp')::decimal > :tsBegin 
        order by (data->'timestamp')::decimal asc 
        limit :num
        """).bindparams(eventId=eventId, tsBegin=tsBegin, num=num))
        ret = [row[0] for row in res]
        return ret

def compose_replay_infos(eventId=None):
    with eng.connect() as con:        
        stmt = """
        select id,(w.data->'payload'->'session'->0)::float as st  from wampdata w
            where w.event_id=:eventId 
            and jsonb_array_length(w.data->'payload'->'cars') > 0
            order by st {orderArg}
            limit 1
        """
        res = con.execute(text(stmt.format(orderArg='asc')).bindparams(eventId=eventId))
        low = next(iter(res))
        res = con.execute(text(stmt.format(orderArg='desc')).bindparams(eventId=eventId))
        high = next(iter(res))
        print(low[1])
        
        dbSession = Session(bind=con)
        res = dbSession.query(Event).filter_by(Id=eventId).first()    
        
        ret = {'event': res.toDict(), 'minSessionTime': low[1], 'maxSessionTime': high[1]}

        return ret

        

if __name__ == '__main__':
    print(f"{os.environ.get(ENV_DB_URL)}")
    # print(f'{read_manifest("1")}')
    # print(f'{read_events()}')
    # print(f'{read_wamp_data(eventId=15, tsBegin=1615734697.6675763, num=2)}')
    print(f'{compose_replay_infos(15)}')

