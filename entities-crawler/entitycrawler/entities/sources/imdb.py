
import requests
from BeautifulSoup import BeautifulSoup
import datetime as dt
import time
from pymongo import MongoClient
from entitycrawler import db as edb

class IMDBEntityImporter(object):

    DEFAULT_CONF = {
        'category': '/film',
        'mongo_col_entities': 'entities',
        'mongo_col_entity_source': 'entity_source',
        'timeout': 100,
        'bulk_insert_limit': 1000,
        'retry': 3,
    }

    source = 'imdb'

    def __init__(self, db, **kw):
        self.conf = self.DEFAULT_CONF.copy()
        self.conf.update(**kw)
        self.db = db
        self.limit = 100
        self.offset = 0
        self.data = []
        self.page = 1
        self.cat = None

    def _get_imdb_actors_page(self, url=None, gender='male'):
        if url is None:
            url = '/search/name?gender='+gender+'&sort=starmeter,asc&start=1'
        r = self._get_url(url)
        soup = BeautifulSoup(r.text)
        pages = soup.find("div", {"id": "right"}).findAll('a')
        next_url = None
        for page in pages:
            if page.text.find('Next') != -1:
                next_url = page['href']
        data = []
        for td in soup.findAll('td'):
            if td.get('class', '') == 'name':
                data.append({
                    'name': td.a.text,
                    '_name': td.a.text.lower(),
                    'category': self.conf['category'],
                    'source': self.source,
                    'occur': 0,
                    'added_at': dt.datetime.utcnow(),
                })
        return data, next_url

    def _get_imdb_films_page(self, url=None):
        url = '/search/title?count=100?start=1'
        r = self._get_url(url)
        soup = BeautifulSoup(r.text)
        pages = soup.find("div", {"id": "right"}).findAll('a')
        next_url = None
        for page in pages:
            if page.text.find('Next') != -1:
                next_url = page['href']
        data = []
        for td in soup.findAll('td'):
            if td.get('class', '') == 'title':
                data.append({
                    'name': td.a.text,
                    '_name': td.a.text.lower(),
                    'category': self.conf['category'],
                    'source': self.source,
                    'occur': 0,
                    'added_at': dt.datetime.utcnow(),
                })
        return data, next_url

    def get_actors_entities(self):
        entities_to_save = []
        data, next_url = self._get_imdb_actors_page()
        while next_url is not None:
            data, next_url = self._get_imdb_actors_page(url=next_url)
            entities_to_save += data
            if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                self._save_entities(entities_to_save)
                entities_to_save = list()
        self._save_entities(entities_to_save)
        data, next_url = self._get_imdb_actors_page(gender='female')
        while next_url is not None:
            data, next_url = self._get_imdb_actors_page(url=next_url)
            entities_to_save += data
            if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                self._save_entities(entities_to_save)
                entities_to_save = list()
        self._save_entities(entities_to_save)

    def get_films_entities(self):
        entities_to_save = []
        data, next_url = self._get_imdb_films_page()
        while next_url is not None:
            data, next_url = self._get_imdb_films_page(url=next_url)
            entities_to_save += data
            if len(entities_to_save) >= self.conf['bulk_insert_limit']:
                self._save_entities(entities_to_save)
                entities_to_save = list()
        self._save_entities(entities_to_save)

    def _save_entities(self, entities_to_save):
        try:
            self.db[self.conf['mongo_col_entities']].insert(entities_to_save,
                                                            continue_on_error=True,
                                                            upsert=True)
            print(len(entities_to_save), 'entities saved')
        except Exception:
            print(len(entities_to_save), 'entities saved')
            pass

    def _get_url(self, url):
        while True:
            try:
                r = requests.get('http://www.imdb.com'+url)
                if r.status_code == 200:
                    return r
                else:
                    print('imdb request error, waiting ...')
                    time.sleep(self.conf['timeout'])
            except Exception:
                print('imdb request timeout, waiting ...')
                time.sleep(self.conf['timeout'])

    def run_import(self):
        categories = edb.get_categories(MongoClient()['entityextractor'])
        for cat in categories:
            if cat['category'] == self.conf['category']:
                self.cat = cat
                if time.mktime(dt.datetime.utcnow().timetuple()) - time.mktime(cat['last_updated'].timetuple()) < 86400:
                    print('imdb not need update')
                    return

        self.get_films_entities()
        self.get_actors_entities()

        now = dt.datetime.utcnow()
        edb.update_category(MongoClient()['entityextractor'], self.cat['_id'], {'$set': {'last_updated': now}})
        print('imdb import complete')

#for test
if __name__ == '__main__':
    a = IMDBEntityImporter(MongoClient()['entityextractor'])
    a.get_actors_entities()
