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
import logging
import logging.config

with open('logging.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
log = logging.getLogger("fileArchiver")

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
        log.info("fileArchiver ready")
        mySession = session
        
        os.makedirs(crossbarConfig.logdir, exist_ok=True)
        timestr = datetime.now().strftime("%Y-%m-%d-%H%M%S")        
        fulldir = str(Path(crossbarConfig.logdir).resolve())
        json_file_name = f"{fulldir}/data-{id}-{timestr}.json"
        manifest_file_name = f"{fulldir}/manifest-{id}-{timestr}.json"
        info_file_name = f"{fulldir}/info-{id}-{timestr}.json"
        json_log_file = codecs.open(json_file_name, "w", encoding='utf-8')
        json_link_filename = f"{fulldir}/data-{id}.json" # used for http server access during workaround
        manifest_link_filename = f"{fulldir}/manifest-{id}.json" # used for http server access during workaround
        info_link_filename = f"{fulldir}/info-{id}.json" # used for http server access during workaround
        
        remove_file_silent(json_link_filename)
        remove_file_silent(manifest_link_filename)
        remove_file_silent(info_link_filename)
        pwd = os.curdir
        os.chdir(fulldir)
        os.symlink(Path(json_file_name).resolve().name, Path(json_link_filename).resolve().name)
        os.symlink(Path(manifest_file_name).resolve().name, Path(manifest_link_filename).resolve().name)
        os.symlink(Path(info_file_name).resolve().name, Path(info_link_filename).resolve().name)
        os.chdir(pwd)


        
        def mgr_msg_handler(msg):
            log.debug(f'{msg} on mgr topic')
            if (msg == 'QUIT'):
                json_log_file.close()
                session.leave()
                log.info(f"leaving wamp session for eventKey {id}")
                

        def do_archive(a):
            json_data = json.dumps(a)
            log.debug(f'received {len(json_data)} bytes ')
            json_log_file.write(f'{json_data}\n')

        def retrieve_manifest(id):
            
            manifests = glob.glob(f'{crossbarConfig.logdir}/manifest-{id}.json');
            if len(manifests) > 0:
                with codecs.open(manifests[0], "r", encoding='utf-8') as data_file:
                    lines = data_file.readlines()                    
                    return lines
            else:
                return "{}"

        def retrieve_data(id, from_timestamp):
            
            data_files = glob.glob(f'{crossbarConfig.logdir}/data-{id}.json');
            with codecs.open(data_files[0], "r", encoding='utf-8') as data_file:
                lines = data_file.readlines()
                ret = []
                for line in lines:
                    json_data =  json.loads(line)
                    if (json_data['timestamp'] > from_timestamp):
                        ret.append(line)
                return ret

        try:
            log.debug("joined {}: {}".format(session, details))
            
            # await session.register(doSomething, crossbarConfig.rpcEndpoint)
            manifests = await session.call(u'racelog.get_manifests', id)
            with codecs.open(manifest_file_name, "w", encoding='utf-8') as file:
                file.write(json.dumps(manifests))
            info = await session.call(u'racelog.get_event_info', id)
            with codecs.open(info_file_name, "w", encoding='utf-8') as file:
                file.write(json.dumps(info))

            
            await session.subscribe(do_archive, topic)       
            await session.subscribe(mgr_msg_handler, mgr_topic)             
        except Exception as e:
            log.error("error registering subscriber: {0}".format(e))

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

