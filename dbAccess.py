import asyncio
import argparse
from model.message import Message, MessageType
import logging


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
from storage.schema import Event, EventExtraData, TrackData,WampData

ENV_DB_URL="DB_URL"

eng = create_engine(os.environ.get(ENV_DB_URL), pool_pre_ping=True, echo_pool=True)

Session = sessionmaker(bind=eng)
print("Plain called and init done")

log = logging.getLogger("dbAccess")

def read_event_info(eventId):
    with eng.connect() as con:
        dbSession = Session(bind=con)
        res = dbSession.query(Event).filter_by(Id=eventId).first()    
        return res.toDict()


def read_manifest(eventKey):
    with eng.connect() as con:
        dbSession = Session(bind=con)
        x = dbSession.query(Event).filter_by(EventKey=eventKey).first()    
        if (x != None):
            return x.Data['manifests']
        else:
            return {}

def get_track_info(trackId):
    with eng.connect() as con:
        dbSession = Session(bind=con)
        res = dbSession.query(TrackData).filter_by(Id=trackId).first()    
        if res != None:
            return res.Data
            


def read_events():
    with eng.connect() as con:
        dbSession = Session(bind=con)
        res = dbSession.query(Event).order_by(Event.RecordDate.desc()).all()   
        return [item.toDict() for item in res]
            
        
def read_wamp_data(eventId=None,tsBegin=None, num=10):
    with eng.connect() as con:        
        res = con.execute(text("""
        select data from wampdata 
        where event_id=:eventId and (data->'timestamp')::numeric > :tsBegin 
        order by (data->'timestamp')::numeric asc 
        limit :num
        """).bindparams(eventId=eventId, tsBegin=tsBegin, num=num))
        ret = [row[0] for row in res]
        return ret

def read_wamp_data_diff(eventId=None,tsBegin=None, num=10):
    def compute_car_changes(ref, cur):
        changes = []
        # carsRef = ref[0]['payload']['cars']
        # carsCur = cur[0]['payload']['cars']
        for i in range(len(ref)):
            for j in range(len(ref[i])):                    
                if ref[i][j] != cur [i][j]:
                    changes.append([i,j,cur[i][j]])
        return changes

    def compute_session_changes(ref, cur):
        changes = []
        
        for i in range(len(ref)):        
            if ref[i] != cur [i]:
                changes.append([i,cur[i]])
        return changes
            

    with eng.connect() as con:        
        res = con.execute(text("""
        select data from wampdata 
        where event_id=:eventId and (data->'timestamp')::numeric > :tsBegin 
        order by (data->'timestamp')::numeric asc 
        limit :num
        """).bindparams(eventId=eventId, tsBegin=tsBegin, num=num))
        work = [row[0] for row in res]
        ret = [work[0]]
        ref = work[0]
        for cur in work[1:]:
            entry = {
                'cars': compute_car_changes(ref['payload']['cars'], cur['payload']['cars']),
                'session': compute_session_changes(ref['payload']['session'], cur['payload']['session']),
                }
            ret.append({'type':MessageType.STATE_DELTA.value, 'payload':entry, 'timestamp':cur['timestamp']})
            
        return ret


def compose_replay_infos(eventId=None):
    with eng.connect() as con:        

        stmtBoundary = """
            select id,(w.data->'payload'->'session'->0)::float as st, (w.data->'timestamp')::float   from wampdata w
                where w.event_id=:eventId 
                and jsonb_array_length(w.data->'payload'->'cars') > 0
                order by (w.data->'timestamp')::numeric {orderArg}
                limit 1
        """

        # this will detect race start and checkered flag if available. We only need race start
        stmt = """
            select  (w.data->'timestamp')::float as stamp, (w.data->'payload'->'session'->0)::float as st
            from wampdata w where 
            w.event_id=:eventId
            and w.data->'payload'->'messages'->0->>0 = 'Timing' 
            and w.data->'payload'->'messages'->0->>1 = 'RaceControl' 
            order by stamp
        """
        res = con.execute(text(stmt).bindparams(eventId=eventId))
        low = next(iter(res),None)
        if (low != None):
            minSt = low[1]
            minTs = low[0]
            
        else:
           
            res = con.execute(text(stmtBoundary.format(orderArg='asc')).bindparams(eventId=eventId))
            low = next(iter(res))
            minSt = low[1]
            minTs = low[2]

        res = con.execute(text(stmtBoundary.format(orderArg='desc')).bindparams(eventId=eventId))
        high = next(iter(res))
            
        
        ret = {'minSessionTime': minSt, 'maxSessionTime': high[1], 'minTimestamp': minTs}

        return ret


def compute_diffs(eventId=None):
    with eng.connect() as con:        
        stmt = """
        select id,(w.data->'payload'->'session'->0)::float as st, (w.data->'timestamp')::float   from wampdata w
            where w.event_id=:eventId 
            and jsonb_array_length(w.data->'payload'->'cars') > 0
            order by st {orderArg}
            limit 10
        """
        stmt = """
        select w.data from wampdata w where w.event_id=:eventId and jsonb_array_length(w.data->'payload'->'cars') > 0 order by id {orderArg} 
        --limit 10
        """
        res = con.execute(text(stmt.format(orderArg='asc')).bindparams(eventId=eventId))
        it = iter(res)
        ref = next(it)
        cur = next(it,None)
        changes = []
        while cur != None:
            carsRef = ref[0]['payload']['cars']
            carsCur = cur[0]['payload']['cars']
            for i in range(len(carsRef)):
                for j in range(len(carsRef[i])):                    
                    if carsRef[i][j] != carsCur [i][j]:
                        changes.append([i,j,carsCur[i][j]])
            ref = cur
            
            cur = next(it,None)            
        ret = changes
        return ret

def store_event_extra_data(eventKey=None, extraData={}):
    with eng.connect() as con:        
        dbSession = Session(bind=con)
        res = dbSession.query(Event).filter_by(EventKey=eventKey).first()    
        if res != None:
            # print(f"{res.Id}")
            ed = EventExtraData(EventId=res.Id, Data=extraData)
            dbSession.add(ed)
            dbSession.commit()
        else:            
            log.error(f"No event with eventKey {eventKey}")

def process_event_extra_data(eventKey=None, extraData={}):
    store_event_extra_data(eventKey,extraData)
    with eng.connect() as con:        
        dbSession = Session(bind=con)
        trackId = extraData['track']['trackId']
        res = dbSession.query(TrackData).filter_by(Id=trackId).first()    
        if res == None:
            dbSession.add(TrackData(Id=trackId, Data=extraData['track']))
            dbSession.commit()
        
        

if __name__ == '__main__':
    print(f"{os.environ.get(ENV_DB_URL)}")
    # print(f'{read_manifest("1")}')
    # print(f'{read_events()}')
    # print(f'{read_wamp_data(eventId=15, tsBegin=1615734697.6675763, num=2)}')
    #print(f'{compose_replay_infos(20)}')
    #print(f'{read_wamp_data_diff(eventId=15, tsBegin=1615734697.6675763, num=5)}')
    # with codecs.open("test.json", "w", encoding='utf-8') as f:
    #     res = compute_diffs(20)
    #     f.writelines(json.dumps(res))
    store_event_extra_data("neox", {'a':'b', 'n':1})

