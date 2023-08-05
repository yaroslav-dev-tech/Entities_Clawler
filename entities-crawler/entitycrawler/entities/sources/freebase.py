import json
import datetime as dt
import logging


import requests
from requests.exceptions import RequestException
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


log = logging.getLogger("entityextractor")


class FreebaseEntityImporter(object):

    DEFAULT_CONF = {
        'mongo_col_entities': 'entities',
        'mongo_col_entity_source': 'entity_source',


        'freebase_api_key': '',
        'freebase_url': 'https://www.googleapis.com/freebase/v1/mqlread',
        'limit': 150,

        'timeout': 10,
        'retry': 3,
    }

    source = 'freebase'

    def __init__(self, db, **kw):
        self.conf = self.DEFAULT_CONF.copy()
        self.conf.update(**kw)
        self.db = db

    def _auth(self, params):
        params['key'] = self.conf['freebase_api_key']
        return params

    def _req(self, params, n=1):
        try:
            return requests.get(self.conf['freebase_url'], params=params, timeout=self.conf['timeout'])
        except RequestException:
            if n <= self.conf['retry']:
                return self._req(params, n=n + 1)
            else:
                raise

    def _query(self, q, cursor=''):
        params = self._auth({
            'query': json.dumps(q),
            'cursor': cursor,
        })
        r = self._req(params)
        r = json.loads(r.text)
        if r.get('error') or r.get('errors') or not 'results' not in r:
            raise Exception(r)
        return r

    def _query_by_type(self, freebase_type, cursor=''):
        q = [{'id': None,
              'name': None,
              'type': freebase_type,
              'limit': self.conf['limit']}]
        return self._query(q, cursor)

    def _query_domains(self, cursor=''):
        q = [{'id': None,
              'name': None,
              'type': '/type/domain',
              'limit': self.conf['limit'],
              '!/freebase/domain_category/domains': {'id': '/category/commons'},
              }]
        return self._query(q, cursor)

    def _query_categories_by_domain(self, domain, cursor=''):
        q = [{'id': None,
              'name': None,
              'type':
              '/type/type',
              'domain': domain,
              'limit': self.conf['limit']}]
        return self._query(q, cursor)

    def _iter(self, query_fn, *args, **kw):
        cursor = ''
        more_results = True
        while more_results:
            r = query_fn(*args, cursor=cursor, **kw)
            for obj in r.get('result', []):
                yield obj
            cursor = r.get('cursor')
            more_results = bool(cursor)

    def _iter_entities(self, freebase_type):
        return self._iter(self._query_by_type, freebase_type)

    def _iter_domains(self):
        return self._iter(self._query_domains)

    def _iter_categories(self, domain):
        return self._iter(self._query_categories_by_domain, domain)

    def import_entities(self, freebase_type, mongo_category):
        bulk = self.db[self.conf['mongo_col_entities']].initialize_unordered_bulk_op()
        for entity in self._iter_entities(freebase_type):
            if entity.get('name', None) is None:
                continue
            bulk.insert({
                'name': entity['name'],
                '_name': entity['name'].lower(),
                'category': mongo_category,
                'source': self.source,
                'occur': 0,
                'added_at': dt.datetime.utcnow(),
            })
        try:
            print(bulk.execute())
        except Exception as e:
            print(e)

    def import_categories(self):
        count = 0
        for domain in self._iter_domains():
            cat = domain['id']
            try:
                self.db[self.conf['mongo_col_entity_source']].insert({
                    'source_category': cat,
                    'category': None,
                    'source': self.source,
                    'case_sensitive': True,
                    'last_updated': None,
                })
            except DuplicateKeyError:
                log.info('%s already exists (skipping)', cat)
                continue
            else:
                count += 1

            if count % 50 == 0:
                log.info('Imported %s entity categories from %s', count, self.source)
        log.info('Total: %s', count)



if __name__ == '__main__':
    import sys

    key = "AIzaSyDI1GlWwoLElDNczf3eBWiOycD9-H1qW5I"

    category = '/music/artist'
    if len(sys.argv) > 1:
        category = sys.argv[1]

    e = FreebaseEntityImporter(MongoClient()['trendin'], freebase_api_key=key)
