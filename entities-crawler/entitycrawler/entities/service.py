import datetime as dt

from entitycrawler.services.classes import SyncService
from entitycrawler.entities import db as edb
from entitycrawler.entities.sources.wikidata import WikidataEntityImporter
from entitycrawler.entities.sources.musicbrainz import MusicbrainzngsEntityImporter
from entitycrawler.entities.sources.imdb import IMDBEntityImporter


class UpdateEntitiesService(SyncService):
    
    name = 'UpdateEntitiesService'
    
    importers = {
        'wikidata': WikidataEntityImporter,
        'musicbrainzngs': MusicbrainzngsEntityImporter,
        'imdb': IMDBEntityImporter
    }

    def __init__(self, *args, **kw):
        self.freebase_api_key = kw.pop('freebase_api_key')
        super(UpdateEntitiesService, self).__init__(*args, **kw)
        self.wait_for = 5
        self.q = 'update_entities_queue'
        self.in_progress_set = 'update_entities_in_progress'
        self.init_importers()

    def init_importers(self):
        self.importers['wikidata'] = self.importers['wikidata'](self.mongodb)
        self.importers['musicbrainzngs'] = self.importers['musicbrainzngs'](self.mongodb)

    def _main(self, cat_id):
        if not cat_id:
            return
        cat = edb.get_category_by_id(self.mongodb, cat_id)
        if not cat:
            self.logger.error('Can not find category with id %s', cat_id)
            return
        for i in cat.get('importers', []):
            importer = self.importers.get(i['importer'])
            importer.import_entities(i.get('properties', None), cat['category'])
            self.logger.info('Importing categories for %s', cat)
            self.redis.sadd(self.in_progress_set, cat_id)
        now = dt.datetime.utcnow()
        edb.update_category(self.mongodb, cat_id, {'$set': {'last_updated': now}})
        self.redis.srem(self.in_progress_set, cat_id)

    def main_loop(self):
        cat_id = self.redis.lpop(self.q)
        try:
            self._main(cat_id)
        except:
            self.redis.srem(self.in_progress_set, cat_id)
            self.redis.lpush(self.q, cat_id)
            raise
