import asyncio
import argparse
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

class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", topic="racelog.state", logdir="logs/json"):        
        self.websocket = websocket
        self.realm = realm
        self.topic = topic
        self.logdir = logdir
    
    def merge(self, **entries):
        self.__dict__.update(entries)    


def remove_file_silent(fn):
    try:        
        os.remove(fn)
    except OSError:
        pass # don't care if link does not exist
        
def runDirect(crossbar_websocket=None, realm="racelog", id=None, topic=None, mgr_topic=None):
    comp = Component(transports=crossbar_websocket, realm=realm)

    @comp.on_join
    async def joined(session, details):
        print("session ready")
        mySession = session
        eventId = None
        eng = create_engine(os.environ.get(ENV_DB_URL))
        Session = sessionmaker(bind=eng)
        
        
        def mgr_msg_handler(msg):
            print(f'{msg} on mgr topic')
            if (msg == 'QUIT'):
                eng.dispose()
                session.leave()
                
                print(f"{__file__} done")

        def do_archive(a):
            
            with eng.connect() as con:
                dbSession = Session(bind=con)
                w = WampData(EventId=eventId, Data=a)
                dbSession.add(w)
                dbSession.commit()

        

        try:
            print("joined {}: {}".format(session, details))
            
            # await session.register(doSomething, crossbarConfig.rpcEndpoint)
            manifests = await session.call(u'racelog.get_manifests', id)            
            info = await session.call(u'racelog.get_event_info', id)
            print(f"{info}")
            event_data = dict()
            event_data['manifests'] = manifests[0]
            event_data['info'] = info[0]
            
            with eng.connect() as con:
                dbSession = Session(bind=con)
                entry = dbSession.query(Event).filter_by(EventKey=id).first()    
                if (entry == None):
                    entry = Event(Name=f"test-{id}", EventKey=id, Data=event_data)
                    dbSession.add(entry)
                    dbSession.flush()
                dbSession.commit()
                eventId = entry.Id

            
            await session.subscribe(do_archive, topic)       
            await session.subscribe(mgr_msg_handler, mgr_topic)             
        except Exception as e:
            print("error registering subscriber: {0}".format(e))

    run([comp])            


    

VERSION = "0.1"
crossbarConfig = ConfigSection()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version='fake data provider %s' % VERSION, help='show version and exit')    
    parser.add_argument('--id',  required=True, help='use this id as a prefix')
    parser.add_argument('--crossbar', help='specifies the crossbar websocket connection (ws://host:port/ws)')
    parser.add_argument('--realm', help='sets the url for the backend')
    parser.add_argument('--config',  help='use this config file', default="config.yaml")
        
    #args = parser.parse_known_args()
    args = parser.parse_args()

    configFilename = "config.yaml"
    if args.config:
        configFilename = args.config
    try:
        with open(configFilename, "r") as ymlfile:
            cfg = yaml.safe_load(ymlfile)
            if "crossbar" in cfg.keys():
                crossbarConfig.merge(**cfg['crossbar'])
    except IOError as e:
        print(f'WARN: Could not open {configFilename}: {e}. continuing...')

    # TODO: settings via environment

    if args.crossbar:
        crossbarConfig.websocket = args.crossbar

    print(f'Using this websocket: {crossbarConfig.websocket}')

    runDirect(crossbarConfig.websocket, crossbarConfig.realm, id, crossbarConfig.topic, "nomanager")

