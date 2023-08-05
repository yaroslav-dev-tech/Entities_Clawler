import datetime as dt
import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
from langdetect import detect


class WikidataEntityImporter(object):

    DEFAULT_CONF = {
        'mongo_col_entities': 'entities',
        'mongo_col_entity_source': 'entity_source',
        'limit': 49,
        'timeout': 10,
        'bulk_insert_limit': 1000,
        'retry': 3,
    }

    source = 'wikidata'

    def __init__(self, db, **kw):
        self.conf = self.DEFAULT_CONF.copy()
        self.conf.update(**kw)
        self.db = db

    def get_properties_list(self, category):
        r = requests.get('https://www.wikidata.org/w/api.php?action=wbsearchentities&search='+category+'&language=en&type=property&limit=50&format=json',
                         timeout=self.conf['timeout'])
        properties_list = r.json()['search']
        print('properties found for category: ', category, ' is ', [prop['label'] for prop in properties_list])
        return properties_list

    def get_entities_list(self, prop):
        r = requests.get('http://wdq.wmflabs.org/api?q=claim['+str(prop)+']',
                         timeout=self.conf['timeout'])
        entities_list = r.json()['items']
        return entities_list

    def get_entity(self, ids):
        r = requests.get('https://www.wikidata.org/w/api.php?action=wbgetentities&ids='+str(ids)[:-1]+'&props=labels&languages=en&format=json',
                         timeout=self.conf['timeout'])
        entity_data = r.json()
        return entity_data

    def import_entities(self, properties, mongo_category):
        print(properties, mongo_category)
        entity_couter = 0
        ids = ''
        entities_to_save = []
        properties_list = properties.split(',')
        for prop in properties_list:
            entities_list = self.get_entities_list(int(re.search(r'\d+', prop).group()))
            for entity_id in entities_list:
                entity_couter += 1
                ids += 'Q'+str(entity_id)+'|'
                if entity_couter >= self.conf['limit']:
                    entities_data = self.get_entity(ids)['entities']
                    ids = ''
                    entity_couter = 0
                    for key, value in entities_data.iteritems():
                        if entities_data[key].get('labels', None) is None:
                            continue
                        if detect(entities_data[key]['labels']['en']['value']) != 'en':
                            continue
                        print(entities_data[key]['labels']['en']['value'])
                        entities_to_save.append({
                            'name': entities_data[key]['labels']['en']['value'],
                            '_name': entities_data[key]['labels']['en']['value'].lower(),
                            'category': mongo_category,
                            'source': self.source,
                            'occur': 0,
                            'added_at': dt.datetime.utcnow(),
                        })
                        if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                            try:
                                self.db[self.conf['mongo_col_entities']].insert(entities_to_save,
                                                                                continue_on_error=True,
                                                                                upsert=True)
                                print(len(entities_to_save), 'entities saved')
                                entities_to_save = []
                            except DuplicateKeyError:
                                entities_to_save = []
                                print('DuplicateKeyError')
                                pass
        if len(entities_to_save) > 0:
            self.db[self.conf['mongo_col_entities']].insert(entities_to_save,
                                                            continue_on_error=True,
                                                            upsert=True)

    def import_entities_by_property(self, prop, category):
        entity_couter = 0
        ids = ''
        entities_to_save = []
        entities_list = self.get_entities_list(int(re.search(r'\d+', prop).group()))
        for entity_id in entities_list:
            entity_couter += 1
            ids += 'Q' + str(entity_id) + '|'
            if entity_couter >= self.conf['limit']:
                entities_data = self.get_entity(ids)['entities']
                ids = ''
                entity_couter = 0
                for key, value in entities_data.iteritems():
                    if entities_data[key].get('labels', None) is None:
                        continue
                    print(entities_data[key]['labels']['en']['value'])
                    entities_to_save.append({
                        'name': entities_data[key]['labels']['en']['value'],
                        '_name': entities_data[key]['labels']['en']['value'].lower(),
                        'category': category,
                        'source': self.source,
                        'occur': 0,
                        'added_at': dt.datetime.utcnow(),
                    })
                    if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                        try:
                            print(self.db[self.conf['mongo_col_entities']].insert(entities_to_save,
                                                                                  continue_on_error=True,
                                                                                  upsert=True) )
                            print(len(entities_to_save), 'entities saved')
                            entities_to_save = []
                        except DuplicateKeyError:
                            entities_to_save = []
                            pass


if __name__ == '__main__':
    c = WikidataEntityImporter(MongoClient()['entityextractor'])
    print(c.import_entities('P1104,P6', 'Music'))

