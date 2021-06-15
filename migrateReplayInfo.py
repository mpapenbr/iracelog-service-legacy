from dbArchiver import ENV_DB_URL
from sys import int_info
from sqlalchemy.orm import create_session
from storage.schema import Event,WampData
from dbAccess import compose_replay_infos
import codecs
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
ENV_DB_URL="DB_URL"

def update_replay_infos_in_events():
    
    
    eng = create_engine(os.environ.get(ENV_DB_URL))
    Session = sessionmaker(eng)
    with eng.connect() as con:
        with Session(bind=con) as dbSession:
            # events = dbSession.query(Event).filter_by(Id=15).all()                
            events = dbSession.query(Event).all()                
            for ev in events:
                
                res = compose_replay_infos(eventId=ev.Id)
                newData = {'replayInfo': {'minSessionTime': res['minSessionTime'], 'maxSessionTime': res['maxSessionTime'], 'minTimestamp': res['minTimestamp']}}
                jsonData = json.dumps(newData)
                con.execute(f"update event set data = mgm_jsonb_merge(data, '{jsonData}'::jsonb) where id={ev.Id}")
                
            
            dbSession.commit()


if __name__ == '__main__':
    update_replay_infos_in_events();



