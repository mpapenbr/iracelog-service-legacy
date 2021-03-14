import asyncio
import argparse
from os import makedirs, mkdir
import yaml
import json
from datetime import datetime
import codecs
from enum import Enum
from autobahn.asyncio.component import Component, run

class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", topic="racelog.state", logdir="logs/json"):        
        self.websocket = websocket
        self.realm = realm
        self.topic = topic
        self.logdir = logdir
    
    def merge(self, **entries):
        self.__dict__.update(entries)    


        
def runDirect(crossbar_websocket=None, realm="racelog", id=None, topic=None, mgr_topic=None):
    comp = Component(transports=crossbar_websocket, realm=realm)

    @comp.on_join
    async def joined(session, details):
        print("session ready")
        mySession = session
        
        makedirs(crossbarConfig.logdir, exist_ok=True)
        timestr = datetime.now().strftime("%Y-%m-%d-%H%M%S")        
        json_log_file = codecs.open(f"{crossbarConfig.logdir}/send-data-{id}-{timestr}.json", "w", encoding='utf-8')

        def mgr_msg_handler(msg):
            print(f'{msg} on mgr topic')
            if (msg == 'QUIT'):
                json_log_file.close()
                session.leave()
                print(f"{__file__} done")

        def do_archive(a):
            json_data = json.dumps(a)
            print(f'received {len(json_data)} bytes ')
            json_log_file.write(f'{json_data}\n')

        try:
            print("joined {}: {}".format(session, details))
            
            # await session.register(doSomething, crossbarConfig.rpcEndpoint)
            manifests = await session.call(u'racelog.get_manifests', id)
            with codecs.open(f"{crossbarConfig.logdir}/manifest-{id}-{timestr}.json", "w", encoding='utf-8') as manifest_file:
                manifest_file.write(json.dumps(manifests))
                
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

