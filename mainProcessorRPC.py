import asyncio
import argparse
import yaml
from enum import Enum
from autobahn.asyncio.component import Component, run

class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", rpcEndpoint="racelog.state"):        
        self.websocket = websocket
        self.realm = realm
        self.rpcEndpoint = rpcEndpoint
    
    def merge(self, **entries):
        self.__dict__.update(entries)    


        
class MessageType(Enum):
    EMPTY = 0
    STATE = 1
    SESSION = 2
    INFO = 3
    CARS = 4
    PITS = 5

class Message:
    type = None
    timestamp = 0
    payload = None
    
    def __init__(self, type=None, payload=None) -> None:
        self.type = type        
        self.payload = payload


    

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

    if args.crossbar:
        crossbarConfig.websocket = args.crossbar

    print(f'Using this websocket: {crossbarConfig.websocket}')

    comp = Component(transports=crossbarConfig.websocket, realm=crossbarConfig.realm)

    @comp.on_join
    async def joined(session, details):
        print("session ready")
        mySession = session
        
        def doSomething(a):
            print(f'called with {a}')
            x = a['payload']['session']

            
            mySession.publish("session", {'type': MessageType.SESSION.value, 'timestamp': a['timestamp'], 'data':x})
            mySession.publish("messages", {'type': MessageType.INFO.value, 'timestamp': a['timestamp'], 'data':a['payload']['messages']})
            mySession.publish("cars", {'type': MessageType.CARS.value, 'timestamp': a['timestamp'], 'data':a['payload']['cars']})
            mySession.publish("pits", {'type': MessageType.PITS.value, 'timestamp': a['timestamp'], 'data':a['payload']['pits']})
            return "returnd from processor"

        try:
            print("joined {}: {}".format(session, details))
            
            await session.register(doSomething, crossbarConfig.rpcEndpoint)
            # await session.subscribe(ondata, u'livetiming.directory')        
        except Exception as e:
            print("error registering rpc: {0}".format(e))

    run([comp])            


