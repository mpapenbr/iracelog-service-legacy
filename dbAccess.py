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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage.schema import Event,WampData

ENV_DB_URL="DB_URL"

eng = create_engine(os.environ.get(ENV_DB_URL))

Session = sessionmaker(bind=eng)
print("Plain called and init done")

def read_manifest(id):
    with eng.connect() as con:
        dbSession = Session(bind=con)
        x = dbSession.query(Event).filter_by(EventKey=id).first()    
        if (x != None):
            return x.Data['manifests']
        else:
            return {}

def read_events():
    with eng.connect() as con:
        dbSession = Session(bind=con)
        res = dbSession.query(Event).order_by(Event.RecordDate.desc()).all()   
        return [item.toDict() for item in res]
            
        


if __name__ == '__main__':
    print(f'{read_manifest("1")}')
    print(f'{read_events()}')

