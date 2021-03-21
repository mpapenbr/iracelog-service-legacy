import asyncio
import argparse
import yaml
from enum import Enum
from autobahn.asyncio.component import Component, run

class ConfigSection():
    def __init__(self, websocket="ws://hostname:port", realm="racelog", topic="racelog.state"):        
        self.websocket = websocket
        self.realm = realm
        self.topic = topic
    
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


def runDirect(crossbar_websocket=None, realm="racelog", id=None, topic=None, mgr_topic=None):
    comp = Component(transports=crossbar_websocket, realm=realm)

    @comp.on_join
    async def joined(session, details):
        print("session ready")
        mySession = session
        
        def mgr_msg_handler(msg):
            print(f'{msg} on mgr topic')
            if (msg == 'QUIT'):
                session.leave()
                print 

        def doSomething(a):
            print(f'livetiming called with {a}')
            x = a['payload']['session']
            sessionTime = -1 # TODO: get this via manifest from a['payload']['session']
            mySession.publish(f"session.{id}", {'type': MessageType.SESSION.value, 'timestamp': a['timestamp'], 'data':x})
            mySession.publish(f"messages.{id}", {'type': MessageType.INFO.value, 'timestamp': a['timestamp'], 'sessionTime': sessionTime,'data':a['payload']['messages']})
            mySession.publish(f"cars.{id}", {'type': MessageType.CARS.value, 'timestamp': a['timestamp'], 'sessionTime': sessionTime, 'data':a['payload']['cars']})
            mySession.publish(f"pits.{id}", {'type': MessageType.PITS.value, 'timestamp': a['timestamp'], 'sessionTime':sessionTime, 'data':a['payload']['pits']})
            

        try:
            print("joined {}: {}".format(session, details))
            
            # await session.register(doSomething, crossbarConfig.rpcEndpoint)

            manifests = await session.call(u'racelog.get_manifests', id)
            await session.subscribe(doSomething, f'{topic}')    
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

    runDirect(crossbarConfig.websocket, crossbarConfig.realm, args.id, crossbarConfig.topic, "nomanager")


