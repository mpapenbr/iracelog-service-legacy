from multiprocessing import Queue
from autobahn.asyncio.component import Component, run
import time 

def runDirect(crossbar_websocket=None, realm="racelog", topic=None, mgr_topic=None):

    comp = Component(transports=crossbar_websocket, realm=realm)
    

    @comp.on_join
    async def joined(session, details):
        print(f"listener for {topic} ready")

        def handler(msg):
            print(f'{msg} on topic')
            
        
        def mgr_msg_handler(msg):
            print(f'{msg} on mgr topic')
            if (msg == 'QUIT'):
                session.leave()
                print 
            
        try:
            print("joined {}: {}".format(session, details))
            
            await session.subscribe(handler, topic)        
            await session.subscribe(mgr_msg_handler, mgr_topic)        
            

        except Exception as e:
            print("error registering rpc: {0}".format(e))
    # comp.start()
    # time.sleep(10)
    run([comp])            
    print('exiting runFromQueue')
    
