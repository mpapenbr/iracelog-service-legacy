from multiprocessing import Queue
from autobahn.asyncio.component import Component, run
import time 

def runFromQueue(q, crossbar_websocket=None, realm="racelog", topic=None):
    print(f'about to get someting from queue')
    msg = q.get()
    print(f'{msg}')

    comp = Component(transports=crossbar_websocket, realm=realm)
    

    @comp.on_join
    async def joined(session, details):
        print(f"listener for {topic} ready")

        def handler(msg):
            print(f'{msg} on topic')
            
        try:
            print("joined {}: {}".format(session, details))
            
            await session.subscribe(handler, topic)        
        except Exception as e:
            print("error registering rpc: {0}".format(e))
    # comp.start()
    # time.sleep(10)
    run([comp])            
    print('exiting runFromQueue')
    
