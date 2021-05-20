from sqlalchemy.orm import create_session
from storage.schema import Event,WampData
import codecs
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def import_data(event_key=None):
    with codecs.open(f"logs/json/data-{event_key}.json", "r", encoding='utf-8') as data_file:
        eng = create_engine(os.environ.get("SQLALCHEMY_URL"))
        Session = sessionmaker(eng)
        with eng.connect() as con:
            with Session(bind=con) as session:
                    
                e = Event(Name="test", EventKey=event_key)
                session.add(e)
                session.flush()
                print(e.Id)
                to_insert = []
                for line in data_file:
                    j = json.loads(line)
                    w = WampData(EventId=e.Id, Data=j)
                    to_insert.append(w)
                print(f"{len(to_insert)} items read")
                session.bulk_save_objects(to_insert)
                print(f"save objects done")
                session.commit()


if __name__ == '__main__':
    import_data("neo")



