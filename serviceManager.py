import sys
import asyncio
import argparse
from os import getenv
import yaml
from enum import Enum
from autobahn.asyncio.component import Component, run
from autobahn.wamp.types import CallResult

from mainProcessorSubscriber import runDirect as livetimingMain
from fileArchiver import runDirect as fileArchiverMain
from multiprocessing import Process
import multiprocessing as mp
import glob
import codecs
import json

from logging import Logger
import logging.config
class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", rpcEndpoint="racelog.manager",logdir="logs/json", user="datapublisher", credentials=None):        
        self.websocket = websocket
        self.realm = realm
        self.rpcEndpoint = rpcEndpoint
        self.logdir = logdir
        self.user = user
        self.credentials = credentials
    
    def merge(self, **entries):
        self.__dict__.update(entries)    

class ProviderData:
    def __init__(self, key=None, manifests=[], info={}, name='NoName', description="NoDescription" ) -> None:
        self.key = key
        self.manifests = manifests
        self.name = name
        self.description = description
        self.info = info
    
    def list_output(self):
        return {'key':self.key, 'name': self.name, 'description': self.description}
    
def main():
    comp = Component(transports=crossbarConfig.websocket, realm=crossbarConfig.realm,
    authentication={
        'ticket': {
            'authid': crossbarConfig.user,
            'ticket': crossbarConfig.credentials
        }})
    serviceLookup = {}
    mp.set_start_method('spawn')

    
    @comp.on_join
    async def joined(session, details):
        print("service manager session ready")
        mySession = session

        def register_provider(args):
            print(f'called with {args}')

            # inform other handlers about the new provider
            mySession.publish(f'racelog.manager.provider', args)

            key = args['id']
            if key not in serviceLookup.keys():
                serviceLookup[key] = ProviderData(key=key, manifests=args['manifests'], info=args['info'])
                p = Process(target=livetimingMain, args=((crossbarConfig.websocket, crossbarConfig.realm, key, f'racelog.state.{key}', f'racelog.manager.command.{key}', crossbarConfig.user, crossbarConfig.credentials)))
                p.start()
                # p.daemon()                

                p = Process(target=fileArchiverMain, args=((crossbarConfig.websocket, crossbarConfig.realm, key,  f'racelog.state.{key}', f'racelog.manager.command.{key}')))
                p.start()
                

            else:            
                log.debug(f"Provider with key {key} already registered")

        def remove_provider(key):            
            log.debug(f'remove_provider called with {key}')            
            mySession.publish(f'racelog.manager.command.{key}', "QUIT")
            if key in serviceLookup.keys():
                serviceLookup.pop(key)
                return "removed"
            else:            
                log.debug(f"Provider with key {key} not found")

        def list_provider():     
            return [x.list_output() for x in serviceLookup.values()]
            

        def get_provider_manifests(key):                        
            if key in serviceLookup:
                return [serviceLookup[key].manifests]
            return None

        def get_event_info(key):                        
            if key in serviceLookup:
                return [serviceLookup[key].info]
            return None
            
        # Archive manager (move to own module)
        def retrieve_archiver_manifest(id):
            
            manifests = glob.glob(f'{crossbarConfig.logdir}/manifest-{id}.json');
            if len(manifests) > 0:
                with codecs.open(manifests[0], "r", encoding='utf-8') as data_file:
                    lines = data_file.readlines()                    
                    return lines
            else:
                return "{}"



        def retrieve_archiver_data(id, from_timestamp):
            log.debug("start retrieving data")
            data_files = glob.glob(f'{crossbarConfig.logdir}/data-{id}.json');
            with codecs.open(data_files[0], "r", encoding='utf-8') as data_file:
                lines = f'[{",".join(data_file.readlines())}]'
                json_data = json.loads(lines)                
                ret = [x for x in json_data if x['timestamp'] > from_timestamp]                
                log.debug(f"done retrieving data. got {len(ret)} results")
                return "\n".join([json.dumps(x) for x in ret])
        # Archive manager end

        # --start-- simulate a live provider by replaying a stored race 
        # Note: do not use this when a "real" event with that key is active
        # Note: this section is not indented to be permanent. Just for development

        # gets the event key
        def simulate_provider(id): 
            print(f'id is {id} ')
            if id not in serviceLookup.keys():
                manifests = glob.glob(f'{crossbarConfig.logdir}/manifest-{id}.json');
                if len(manifests) > 0:
                    with codecs.open(manifests[0], "r", encoding='utf-8') as data_file:
                        lines = data_file.readlines()        
                        
                        json_data = json.loads(lines[0])                
                        # mySession.publish(f'racelog.manager.provider', {'id':id, 'manifests': json_data})
                        serviceLookup[id] = ProviderData(id, json_data, name="Replay for Development", description=f"Info: {id}")
                        
        
        # gets the event key
        def remove_simulate_provider(id): 
            print(f'remove simulator: id is {id} ')
            if id in serviceLookup.keys():
                serviceLookup.pop(id)
                return "removed"
            else:            
                log.debug(f"Provider with key {id} not found")

        # --end-- simulate a live provider by replaying a stored race 


        # this is here to play around with different result types. use call on racelog.test to see results
        def test_something():            
            return CallResult("Huhu", ["xyz"], {'i':12, 's':"34"}, res0="single", res1=["abc"], res2={'a':12, 'c':"34"})
        try:
            print("joined {}: {}".format(session, details))
            
            await session.register(register_provider, "racelog.register_provider")
            await session.register(remove_provider, "racelog.remove_provider")
            await session.register(list_provider, "racelog.list_providers")
            await session.register(get_provider_manifests, "racelog.get_manifests")
            await session.register(get_event_info, "racelog.get_event_info")

            # Archive manager
            await session.register(retrieve_archiver_manifest, f"racelog.archive.get_manifest")
            await session.register(retrieve_archiver_data, f"racelog.archive.get_data")
            # Archive manager (end)

             # debug listener, which simulate a race
            await session.register(simulate_provider, "racelog.debug.simulate_provider")
            await session.register(remove_simulate_provider, "racelog.debug.remove_provider")

            await session.register(test_something, "racelog.test")

            # await session.subscribe(ondata, u'livetiming.directory')        
        except Exception as e:
            print("error registering rpc: {0}".format(e))

    run([comp], log_level='debug')            


ENV_CROSSBAR_URL="ENV_CROSSBAR_URL"
VERSION = "0.1"
crossbarConfig = ConfigSection()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version='fake data provider %s' % VERSION, help='show version and exit')    
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
    if getenv(key=ENV_CROSSBAR_URL) != None:
        crossbarConfig.websocket = getenv(key=ENV_CROSSBAR_URL)
    if args.crossbar:
        crossbarConfig.websocket = args.crossbar

    with open('logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    log = logging.getLogger("ServiceManager")
    log.info(f'Using this websocket: {crossbarConfig.websocket}')

    main()

