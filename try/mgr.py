import asyncio
import argparse
from os import getenv
import yaml
from enum import Enum
from autobahn.asyncio.component import Component, run
from logging import Logger
import logging.config
import multiprocessing as mp
from multiprocessing import Process, Queue
from sub import runFromQueue as subMain

class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", rpcEndpoint="racelog.manager"):        
        self.websocket = websocket
        self.realm = realm
        self.rpcEndpoint = rpcEndpoint
    
    def merge(self, **entries):
        self.__dict__.update(entries)    

    
def main():
    comp = Component(transports=crossbarConfig.websocket, realm=crossbarConfig.realm)
    serviceLookup = {}
    mp.set_start_method('spawn')
    @comp.on_join
    async def joined(session, details):
        print("session ready")
        mySession = session
        q = Queue()

        def mgr_handler(msg, num):
            print(f'{msg} with {num}')
            if num not in serviceLookup.keys():
                print(f'start new process')
                p = Process(target=subMain, args=((q,crossbarConfig.websocket, crossbarConfig.realm, f'dummy.{num}')))
                p.start()
                # p.daemon()
                q.put({'num': 'b', 'crossbar_websocket': crossbarConfig.websocket, 'realm': 'racelog', 'topic': f'dummy.{num}' })

        try:
            print("joined {}: {}".format(session, details))
            
            await session.subscribe(mgr_handler, u'dummy.mgr')        
        except Exception as e:
            print("error registering rpc: {0}".format(e))

    run([comp])            


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

