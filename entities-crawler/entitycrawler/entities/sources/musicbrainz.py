import musicbrainzngs
import datetime as dt
from pymongo import MongoClient
from string import ascii_lowercase
from entitycrawler import db as edb
from langdetect import detect


musicbrainzngs.set_useragent(
    "python-musicbrainzngs",
    "0.1",
    "https://github.com/alastair/python-musicbrainzngs/",
)

class MusicbrainzngsEntityImporter(object):

    DEFAULT_CONF = {
        'mongo_col_entities': 'entities',
        'mongo_col_entity_source': 'entity_source',
        'timeout': 10,  # seconds
        'bulk_insert_limit': 1000,
        'retry': 3,
    }

    source = 'musicbrainzngs'

    def __init__(self, db, **kw):
        self.conf = self.DEFAULT_CONF.copy()
        self.conf.update(**kw)
        self.db = db
        self.limit = 100
        self.offset = 0
        self.data = []
        self.page = 1
        self.cat = None

    def get_artists_list(self, query):
        print('fetching query: ' + query)
        result = musicbrainzngs.search_artists(query=query, limit=self.limit)
        print('result artist count: ' + str(result['artist-count']))
        self.data += result['artist-list']
        while len(result['artist-list']) >= self.limit:
            self.offset += self.limit
            self.page += 1
            print("fetching page number %d.." % self.page)
            try:
                result = musicbrainzngs.search_artists(query=query, limit=self.limit, offset=self.offset)
            except musicbrainzngs.MusicBrainzError:
                continue
            self.data += result['artist-list']
        print("\n%d artist entities on %d pages" % (len(self.data), self.page))
        return self.data

    def get_work_list(self, query):
        result = musicbrainzngs.search_works(query=query, limit=self.limit)
        print('result work count: ' + str(result['work-count']))
        self.data += result['work-list']
        while len(result['work-list']) >= self.limit:
            self.offset += self.limit
            self.page += 1
            print("fetching page number %d.." % self.page)
            try:
                result = musicbrainzngs.search_works(query=query, limit=self.limit, offset=self.offset)
            except musicbrainzngs.MusicBrainzError:
                continue
            self.data += result['work-list']
        print("\n%d works entities on %d pages" % (len(self.data), self.page))
        return self.data

    def import_work_entities(self, category, query):
        entities_to_save = list()
        entities = self.get_work_list(query)
        for entity in entities:
            if not entity.get('title', None):
                continue
            if detect(entity['title']) != 'en':
                continue
            entities_to_save.append({
                'name': entity['title'],
                '_name': entity['title'].lower(),
                'category': category,
                'source': self.source,
                'occur': 0,
                'added_at': dt.datetime.utcnow(),
            })
            if entity.get('artist-relation-list', None) is not None:
                for artist in entity['artist-relation-list']:
                    if not artist.get('name', None):
                        continue
                    if detect(artist['name']) != 'en':
                        continue
                    entities_to_save.append({
                        'name': artist['name'],
                        '_name': artist['name'].lower(),
                        'category': category,
                        'source': self.source,
                        'occur': 0,
                        'added_at': dt.datetime.utcnow(),
                    })
            if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                self.save_entities(entities_to_save)
                entities_to_save = list()
        self.save_entities(entities_to_save)

    def import_artist_entities(self, category, query):
        entities_to_save = list()
        entities = self.get_artists_list(query)
        for entity in entities:
            if not entity.get('name', None):
                continue
            if detect(entity['name']) != 'en':
                continue
            entities_to_save.append({
                'name': entity['name'],
                '_name': entity['name'].lower(),
                'category': category,
                'source': self.source,
                'occur': 0,
                'added_at': dt.datetime.utcnow(),
            })
            if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                self.save_entities(entities_to_save)
                entities_to_save = list()
        self.save_entities(entities_to_save)

    def save_entities(self, entities_to_save):
        try:
            self.db[self.conf['mongo_col_entities']].insert(entities_to_save,
                                                            continue_on_error=True,
                                                            upsert=True)
            print(len(entities_to_save), 'entities saved')
        except Exception:
            print(len(entities_to_save), 'entities saved')
            pass

    def import_entities(self, properties=None, mongo_category=None):
        for properties in ascii_lowercase:
            self.import_artist_entities(mongo_category, properties)
            self.import_work_entities(mongo_category, properties)
        now = dt.datetime.utcnow()
        edb.update_category(MongoClient()['entityextractor'], self.cat['_id'], {'$set': {'last_updated': now}})
        print('Musicbrainzngs import complete')



if __name__ == '__main__':
    c = MusicbrainzngsEntityImporter(MongoClient()['entityextractor'])
    c.import_artist_entities('/music', 'papa roach')
    c.import_work_entities('/music', 'papa roach')
