import datetime
import logging
import re
from bson.objectid import ObjectId
from urlparse import urlparse
from pymongo import MongoClient
from scrapers import (NewspaperScrapper, SoupScrapper,
                      ReadabilityScrapper, DefaultScrapper, SCRAPPERS_BY_TYPE)
from crawlers import LinksCrawler, SitemapCrawler, RSSCrawler, choose_crawler_type
from entitycrawler.db import (
    CRAWLERS_POOL_NAME,
    WEBSITES_COL,
    CRAWLERS_COL,
    CRAWLED_PAGES_COL,
    EXTRACTED_PAGES_COL,
    WEBSITES_ENTITIES_COL,
    WEBSITES_CANDIDATES_COL,
    URL_MATCHING_COL,
)
from entitycrawler.extractor import EntityExtractor, ExtractedPage
from entitycrawler.crawler.exceptions import NoMatchedPatternError


STATUS = {'disabled': 0,
          'enabled': 1,
          'waiting': 1,
          'running': 2}

DEFAULT_MAX_AGE = 31536000
DEFAULT_FREQUENCY = 3600

logger = logging.getLogger('crawler')


class WebsiteURLPatterns:

    ''' Patterns manager '''

    def __init__(self, crawler):
        self.crawler = crawler
        self.db = crawler.db

        self.website = crawler.get_website().hostname
        self.default = crawler.default_url_pattern
        self.patterns = list(self.db[URL_MATCHING_COL].find({'crawler': crawler._id}))
        self.regexes = {}

        for pattern in self.patterns:
            self.regexes[pattern['_id']] = self._make_regex(pattern['pattern'])

    @staticmethod
    def _make_regex(pattern):
        return re.compile(pattern, re.IGNORECASE)

    def get_list(self):
        return self.patterns

    def validate_link(self, url):
        if url[-4:] in ('.jpg', '.png'):
            return False
        url = url.split("#")[0]
        for _id, regex in self.regexes.iteritems():
            if regex.match(url):
                return True
        return False

    def match(self, url):
        matches = []
        default_match = False
        for _id, regex in self.regexes.iteritems():
            if regex.match(url):
                if _id == self.default:
                    default_match = True
                else:
                    matches.append(_id)
        if not default_match and len(matches) == 0:
            return None
        if len(matches) > 0:
            if len(matches) > 1:
                print("URL %s matched for more then one non-default patterns: %s" % (
                    url, [pattern['pattern'] for pattern in self.patterns]))
                print ("using first match")
            return self.get_pattern(_id=matches[0], db=self.db, redis=self.crawler.redis)
        else:
            return self.get_pattern(_id=self.default, db=self.db, redis=self.crawler.redis)

    @staticmethod
    def arbitrary_match(url, db):
        website = urlparse(url).hostname
        s = website.split('.')
        if s[0] == "www":
            patterns = list(db[URL_MATCHING_COL].find({'website': website}))
            website = '.'.join(s[1:])
            patterns.extend(list(db[URL_MATCHING_COL].find({'website': website})))
        else:
            patterns = list(db[URL_MATCHING_COL].find({'website': "www." + website}))
            patterns.extend(list(db[URL_MATCHING_COL].find({'website': website})))
        default_ids = [crawler['default_url_pattern'] for crawler in db[CRAWLERS_COL].find()]
        matches = []
        default_match = False
        for pattern in patterns:
            regex = WebsiteURLPatterns._make_regex(pattern['pattern'])
            if regex.match(url):
                if pattern['_id'] in default_ids:
                    default_match = True
                    default = pattern['_id']
                else:
                    matches.append(pattern['_id'])
        if not default_match and len(matches) == 0:
            return None
        if len(matches) > 0:
            if len(matches) > 1:
                print("URL %s matched for more then one non-default patterns: %s" % (
                    url, [pattern['pattern'] for pattern in patterns]))
                print("using first match")
            matched = WebsiteURLPatterns.get_pattern(db=db, _id=matches[0], redis=None)
        else:
            matched = WebsiteURLPatterns.get_pattern(db=db, _id=default, redis=None)
        return matched

    @staticmethod
    def delete(db, _id):
        db[URL_MATCHING_COL].remove({'_id': _id})

    @staticmethod
    def get_pattern(_id, db, redis=None):
        class Pattern:

            def __init__(self, **entries):
                self.__dict__.update(entries)
                self.harvester_categories = u",".join(self.harvester_categories)
                self.exclude = u",".join(self.exclude_words)

            def save(self, data):
                crawler = WebsiteCrawler.get_by_id(self.crawler, db=self.db, redis=self.redis)
                crawler.url_patterns.save(data, new=False)
        if not isinstance(_id, ObjectId):
            _id = ObjectId(_id)
        pattern = db[URL_MATCHING_COL].find_one({'_id': _id})
        print("PATTERN FROM DB", pattern)
        pattern['id'] = str(pattern['_id'])
        pattern['db'] = db
        pattern['redis'] = redis
        dflt = WebsiteCrawler.get_by_id(pattern['crawler'], db=db, redis=None).default_url_pattern
        pattern['default'] = dflt == pattern['_id'] or dflt == None
        return Pattern(**pattern)

    def _save(self, pattern):
        print("Trying to save: ", pattern)
        return self.db[URL_MATCHING_COL].save(pattern)

    @classmethod
    def create(cls, default_pattern, crawler):
        matcher = cls(crawler)
        matcher.save(default_pattern)
        return matcher

    def save(self, data, new=True):
        ''' add URL_pattern to crawler '''
        print("DATA", data)

        if isinstance(data['harvester_categories'], unicode):
            data['harvester_categories'] = data['harvester_categories'].split()
        if isinstance(data['exclude_words'], unicode):
            data['exclude_words'] = data['exclude_words'].split()
        url_pattern = {
            'website': self.website,
            'crawler': self.crawler._id,
            'pattern': data.get('url_pattern', data.get('pattern')),
            'harvester_categories': data['harvester_categories'],
            'exclude_words': data.get('exclude_words', ''),
            'ad_script': data.get('ad_script', "")
        }
        if not new:
            if data.has_key('_id'):
                url_pattern['_id'] = data['_id']
            elif data.has_key('id'):
                url_pattern['_id'] = ObjectId(data['id'])
        _id = self._save(url_pattern)

        self.regexes[_id] = self._make_regex(data.get('pattern', data.get('url_pattern')))
        if not new:
            url_pattern['_id'] = _id
            self.patterns = [pattern for pattern in self.patterns if pattern['_id'] != _id]
            self.patterns.append(url_pattern)
        if data.get('default', False) or len(self.crawler.url_patterns.get_list()) == 0:
            self.crawler.default_url_pattern = _id
        self.crawler.update()


class WebsiteCrawler:

    ''' Crawler that will crawl it's portion of the site '''
    ''' document {  name : text,
                    status: text,
                    website_id: ID,
                    website_name: text,
                    scraper: BSOUP\NEWSP,
                    extractor: our_extractor1,
                    pages: int,
                    start_url: text,
                    url_patterns: [],
                    default_url_pattern: id,
                    age: int,
                    frequency: int ,
                    date_created: Date,
                    date_lastupdated: Date }'''
    ENABLED = STATUS['enabled']
    DISABLED = STATUS['disabled']

    def __init__(self, db_record, website=None, db=None, redis=None):
        # FIXME: implement
        self.doc = db_record
        print ("Website1, ", website)
        print ("db, ", db)
        print ("redis, ", redis)
        if website is not None:
            self.website = website
            self.db = website.db
            self.redis = website.redis
        else:
            self.db = db
            self.redis = redis
        #    self.website = Website.get_by_id(db_record["website_id"], db, redis)
        print ("Website2, ", website)
        print ("db, ", db)
        print ("redis, ", redis)
        print (db_record)

        self._id = db_record['_id']
        self.id = str(db_record['_id'])
        self.name = db_record["name"]
        self.status = db_record["status"]
        self.crawling_status = db_record.get("crawling_status", 0)
        self.website_id = db_record["website_id"]
        self.category = db_record['category']
        self.scraper_type = db_record["scraper"]
        self.extractor_type = db_record["extractor"]
        self.crawled_pages = db_record.get("crawled_pages", 0)
        self.start_url = db_record["start_url"]
        self.age = db_record["age"]
        self.frequency = db_record["frequency"]
        self.date_created = db_record["date_created"]
        self.date_lastupdated = db_record["date_lastupdated"]
        self.crawler_type = db_record["crawler_type"]
        self.default_url_pattern = db_record.get("default_url_pattern", None)

        self.scraper = self._get_scrapper(db_record["scraper"])
        self.crawler = self._get_crawler(db_record["crawler_type"])
        assert (db_record["extractor"][:-2] == EntityExtractor.name[:-2])
        self.extractor = EntityExtractor(db)
        self.url_patterns = WebsiteURLPatterns(self)

    def _getstatus(self):
        return False if self.status == STATUS['disabled'] else True
    enabled = property(_getstatus, doc="Check if crawler Enabled or Disabled.")

    def _get_scrapper(self, name):
        if name.startswith(NewspaperScrapper._type):
            return NewspaperScrapper
        elif name.startswith(SoupScrapper._type):
            return SoupScrapper
        elif name.startswith(ReadabilityScrapper._type):
            return ReadabilityScrapper
        else:
            logger.info("Unknown scrapper [%s] for crawler [%s], using default" % (
                        name, self.name))
            return DefaultScrapper

    def _get_crawler(self, name):
        assert name in [LinksCrawler._type, SitemapCrawler._type, RSSCrawler._type]

        if name == LinksCrawler._type:
            return LinksCrawler(self.scraper, self)
        elif name == RSSCrawler._type:
            return RSSCrawler(self.scraper, self)
        else:
            return SitemapCrawler(self.scraper, self)

    @classmethod
    def create(cls, kwargs, db, redis):
        ''' Create website crawler '''

        WebsiteURLPatterns._make_regex(kwargs['url_pattern'])

        crawler_data = {
            'name': kwargs['name'],
            'status': kwargs['status'],
            'crawling_status': 0,
            'website_id': kwargs['website_id'],
            'scraper': kwargs.get('scraper', DefaultScrapper.name),
            'category': kwargs['category'],
            'extractor': kwargs.get('extractor', EntityExtractor.name),
            'start_url': kwargs['start_url'],
            'age': kwargs.get('age') or DEFAULT_MAX_AGE,
            'frequency': kwargs.get('frequency') or DEFAULT_FREQUENCY,
            'crawler_type': kwargs['crawler_type'],
            'crawled_pages': 0,
            'date_created': datetime.datetime.now(),
            'date_lastupdated': datetime.datetime.now()}
        assert kwargs['crawler_type'] in [LinksCrawler._type, SitemapCrawler._type, RSSCrawler._type]
        crawler_data['_id'] = db[CRAWLERS_COL].insert(crawler_data)
        new_crawler = WebsiteCrawler(crawler_data, db=db, redis=redis)

        initial_url_pattern = {
            'website =': new_crawler.get_website().hostname,
            'pattern': kwargs['url_pattern'],
            'harvester_categories': kwargs['harvester_categories'].split(),
            'exclude_words': kwargs.get('exclude_words', ''),
            'ad_script': kwargs.get('ad_script', "")
        }
        url_patterns = WebsiteURLPatterns.create(initial_url_pattern, new_crawler)

        new_crawler.update_queue()
        return new_crawler

    def delete(self):
        """Delete crawler with ulr_patterns"""
        # TODO: implement
        self.dequeue()
        self.db[CRAWLERS_COL].remove({'_id': ObjectId(self._id)})
        self.db[URL_MATCHING_COL].remove({'crawler': ObjectId(self._id)})

    def update(self, doc=None, upsert=False, check_status=True):
        if doc is None:
            doc = {'_id': self._id,
                   'name': self.name,
                   'status': self.status,
                   'crawling_status': self.crawling_status,
                   'website_id': self.website_id,
                   'category': self.category,
                   'scraper': self.scraper_type,
                   'extractor': self.extractor.name,
                   'start_url': self.start_url,
                   'default_url_pattern': self.default_url_pattern,
                   'age': self.age,
                   'frequency': self.frequency,
                   'crawler_type': self.crawler_type,
                   'crawled_pages': self.crawled_pages,
                   'date_created': self.date_created,
                   'date_lastupdated': datetime.datetime.now()}
        print("Updating Crawler, ", doc )
        old_doc = self.db[CRAWLERS_COL].find_one({'_id': ObjectId(self._id)})
        if isinstance(old_doc, dict):
            if old_doc['age'] != self.age:
                self.db[CRAWLED_PAGES_COL].update({'category': self.category},
                                                  {"$set": {'crawled_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.age)}},
                                                  multi=True)
        doc['date_lastupdated'] = datetime.datetime.now()
        doc['date_created'] = self.date_created
        self.db[CRAWLERS_COL].save(doc)

        self.__init__(doc, db=self.db, redis=self.redis)
        if check_status:
            self.update_queue()

    @classmethod
    def get_all(cls, db, redis, status=None):
        ''' List crawlers '''
        crawlers = []
        for c in db[CRAWLERS_COL].find():
            crawlers.append(WebsiteCrawler(c, db=db, redis=redis))
        return crawlers

    @classmethod
    def get_by_id(cls, _id, db, redis=None):
        print("Get By ID")
        record = db[CRAWLERS_COL].find_one({"_id": ObjectId(_id)})
        print("Found record: ", record)
        if record:
            return cls(record, db=db, redis=redis)
        else:
            return None

    def get_website(self):
        try:
            website = self.website
        except:
            website = Website.get_by_id(self.website_id, db=self.db, redis=self.redis)
        return website

    def check_url_age(self, url):
        if self.start_url == url:
            return True
        record = self.db[CRAWLED_PAGES_COL].find_one({"url": url})
        if record is None:
            print("check_url_age record expired:", url)
            return True
        else:
            print("check_url_age record exist:", url)
            return False

    def queue(self):
        self.redis.rpush(CRAWLERS_POOL_NAME, self.id)

    def dequeue(self):
        self.redis.lrem(CRAWLERS_POOL_NAME, self.id)

    def update_queue(self):
        if self.enabled:
            self.queue()
        else:
            self.dequeue()

    def inc_crawled_count(self):
        print("Inc Crawler Count")
        self.crawled_pages += 1
        self.update(check_status=False)


class Website:

    ''' Website desctiption, contain list of website crawlers '''
    ''' document {  name : text,
                    status: text,
                    publisher_id: ObjectID,
                    publisher_name: text,
                    website_url: ID,
                    website_name: text,
                    website_ad_position: json\text,
                    website_templates: [],
                    category: text,
                    crawler_type: sitemaps\links,
                    pages: int,
                    date_created: Date,
                    date_lastupdated: Date } '''
    name = ''
    id = None
    ENABLED = STATUS['enabled']
    DISABLED = STATUS['disabled']
    status = DISABLED

    def _getstatus(self):
        return False if self.status == STATUS['disabled'] else True

    enabled = property(_getstatus, doc="Check if crawler Enabled or Disabled.")

    def __init__(self, db_record, db, redis):
        self.db = db
        self.redis = redis

        self.doc = db_record
        self._id = db_record['_id']
        self.id = str(db_record['_id'])
        self.name = db_record['name']
        self.status = db_record.get('status', STATUS['enabled'])
        self.publisher_id = db_record.get("publisher_id", "")
        self.publisher_name = db_record['publisher_name']
        self.website_url = db_record['website_url']
        self.hostname = urlparse(db_record['website_url']).hostname
        self.website_ad_position = db_record.get('website_ad_position', u"")
        self.website_templates = db_record.get('website_templates')
        self.category = db_record['category']
        self.crawler_type = db_record.get('crawler_type', choose_crawler_type(self.website_url))

        self.pages = db_record.get('pages', self.update_pages())
        self.date_created = db_record.get('date_created', datetime.datetime.now())
        self.date_lastupdated = db_record.get('date_lastupdated', datetime.datetime.now())
        self.crawlers = [WebsiteCrawler(record, website=self, db=self.db, redis=self.redis) for record in
                         db[CRAWLERS_COL].find({"website_id": self._id})]

    @classmethod
    def get_all(cls, db, redis, status=None):
        ''' List websites '''
        websites = []
        if status is not None:
            wersites_records = db[WEBSITES_COL].find({'status': status})
        else:
            wersites_records = db[WEBSITES_COL].find()
        for c in wersites_records:
            websites.append(Website(c, db, redis=redis))
        return websites

    @classmethod
    def get_by_id(cls, _id, db, redis=None):
        record = db[WEBSITES_COL].find_one({"_id": ObjectId(_id)})
        if record:
            return cls(record, db=db, redis=redis)
        else:
            return None

    def get_url_patterns(self):
        patterns = []
        for crawler in self.crawlers:
            patterns.extend(crawler.url_patterns.get_list())
        return patterns

    def start(self):
        for crawler in self.crawlers:
            crawler.update_queue()

    def stop(self):
        for crawler in self.crawlers:
            crawler.dequeue()

    def update_pages(self):
        try:
            pages = self.pages
        except:
            pages = 0
        return pages

    def update(self, doc=None, upsert=False):
        db_record = {'name': self.name,
                     'status': self.status,
                     'publisher_id': self.publisher_id,
                     'publisher_name': self.publisher_name,
                     'website_url': self.website_url,
                     'website_ad_position': self.website_ad_position,
                     'website_templates': self.website_templates,
                     'category': self.category,
                     'crawler_type': self.crawler_type,
                     'pages': self.update_pages(),
                     'date_created': self.date_created,
                     'date_lastupdated': datetime.datetime.now()
                     }
        for key in doc.keys():
            if key == "enabled":
                db_record['status'] = STATUS['enabled'] if doc['enabled'] else STATUS['disabled']
            else:
                db_record[key] = doc[key]
        self.db[WEBSITES_COL].update({'_id': self._id}, db_record, upsert=upsert)

    def add_crawler(self, kwargs):
        kwargs['website_id'] = self._id
        kwargs['website_name'] = self.name
        kwargs['category'] = self.category
        kwargs['status'] = kwargs.get('status', self.status)
        kwargs['start_url'] = kwargs.get('start_url', self.website_url)
        kwargs['crawler_type'] = self.crawler_type
        crawler = WebsiteCrawler.create(kwargs=kwargs, db=self.db, redis=self.redis)
        return crawler

    @classmethod
    def _choose_crawler_type(cls, url):
        return choose_crawler_type(url)

    @classmethod
    def create(cls, publisher_name, website_name, website_url, site_category, db, redis):


        assert isinstance(website_name, unicode) and len(website_name) < 100
        assert isinstance(website_url, unicode) and len(website_url) < 100
        assert isinstance(publisher_name, unicode) and len(publisher_name) < 100
        assert isinstance(site_category, unicode) and len(site_category) < 100


        crawler_type = cls._choose_crawler_type(website_url)


        website_data = {'name': website_name,
                        'status': STATUS['enabled'],
                        'publisher_id': "",
                        'publisher_name': publisher_name,
                        'website_url': website_url,
                        'website_ad_position': "",
                        'website_templates': [],
                        'category': site_category,
                        'crawler_type': crawler_type,
                        'pages': 0,
                        'date_created': datetime.datetime.now(),
                        'date_lastupdated': datetime.datetime.now()}
        website_data['_id'] = db[WEBSITES_COL].insert(website_data)
        return Website(website_data, db, redis)

    def get_stats(self):
        LIMIT_QUERY = 40
        top_entities = list(self.db[WEBSITES_ENTITIES_COL].find(
            {'site': self.hostname}).sort(
            'count', -1).limit(LIMIT_QUERY))
        if len(top_entities) == 0:
            return None
        last_top_count = top_entities[len(top_entities) - 1]['count']
        entities_min_count = last_top_count if last_top_count < 10 else 10

        top_candidates = list(self.db[WEBSITES_CANDIDATES_COL].find(
            {'site': self.hostname}).sort(
            'count', -1).limit(LIMIT_QUERY))

        top_postitive = list(self.db[WEBSITES_ENTITIES_COL].find(
            {'site': self.hostname,
             'count': {'$gte': entities_min_count}}).sort(
            'sentiment', -1).limit(LIMIT_QUERY))
        top_negative = list(self.db[WEBSITES_ENTITIES_COL].find(
            {'site': self.hostname,
             'count': {'$gte': entities_min_count}}).sort(
            'sentiment', 1).limit(LIMIT_QUERY))
        return top_entities, top_candidates, top_postitive, top_negative

    def generate_report(self, include, exclude):
        urls = self.db[EXTRACTED_PAGES_COL]
        words = list(include.difference(exclude))

        entities_aggr = urls.aggregate([
            {"$match": {"$and": [{"site": self.hostname}, {"entities.text": {"$in": words}}]}},
            {"$unwind": "$entities"},
            {"$project": {"id": "$entities.text",
                          "sentiment": "$entities.sentiment.type",
                          "count": "$entities.count"}},
            {"$match": {"id": {"$in": words}}},
            {"$group": {"_id": {"entity": "$id",
                                "sentiment": "$sentiment"},
                        "count": {"$sum": "$count"}}},
            {"$project": {"_id": 0, "entity": "$_id.entity",
                          "sentiment": {"type": "$_id.sentiment",
                                        "count": "$count"}}},
            {"$out": "website_report"}
        ])
        candidates_aggr = urls.aggregate([
            {"$match": {"$and": [{"site": self.hostname}, {"candidates.text": {"$in": words}}]}},
            {"$unwind": "$candidates"},
            {"$project": {"id": "$candidates.text"}},
            {"$match": {"id": {"$in": words}}},
            {"$group": {"_id": {"entity": "$id",
                                "sentiment": "unknown"},
                        "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "entity": "$_id.entity",
                          "sentiment": {"type": "$_id.sentiment",
                                        "count": "$count"}}},
        ])
        if candidates_aggr['result']:
            self.db.website_report.insert(candidates_aggr['result'])
        total = self.db.website_report.aggregate([
            {"$group": {"_id": "$entity", "sentiments": {"$push": "$sentiment"}}},
            {"$unwind": "$sentiments"},
            {"$group": {"_id": {"entity": "$_id",
                                "sentiment": "$sentiments.type"},
                        "count": {"$sum": "$sentiments.count"}}},
            {"$project": {"_id": "$_id.entity",
                          "sentiment": "$_id.sentiment",
                          "count": "$count"}},
            {"$sort": {"_id": -1}}
        ])
        csv = '''-------------------------------------------\n\n
TrendIN Site report for:
{0}

Total Include words:
{1}

Total Exluded words:
{2}

Total URL's Scanned:
{3}

Total URL's Matched:
{4}

-------------------------------------------
Word, Sentiment, count\n'''.format("%s (%s)" % (self.name, self.website_url), len(include), len(exclude),
                                   urls.find().count(),
                                   urls.find({"site": self.hostname}).count()
                                   )
        for e in total['result']:
            csv += "%s|%s|%s\n" % (e['_id'], e['sentiment'], e['count'])

        return csv


class CrawlerService:

    ''' Main crawler functions '''

    def _get_local_connection(self):
        mongo = MongoClient('localhost', 27017)
        return mongo['entityextractor']

    def __init__(self, mongodb=None, redis=None):
        '''
        db - MongoDB
        redis - Redis connection
        '''
        if mongodb is None:
            self.db = self._get_local_connection()
        else:
            self.db = mongodb
        self.redis = redis
        self.extractor = EntityExtractor(mongodb)

    def create_website_record(self, publisher_name, name, website_url, site_category):
        ''' Create website record '''
        return Website.create(publisher_name, name, website_url, site_category,
                              db=self.db, redis=self.redis)

    def get_crawlers(self, status=None):
        ''' Return list of WebsiteCrawler '''
        return WebsiteCrawler.get_all(self.db, redis=self.redis, status=status)

    def get_crawler(self, _id):
        return WebsiteCrawler.get_by_id(_id=_id, db=self.db, redis=self.redis)

    def get_websites(self, status=None):
        ''' Return list of Website '''
        return Website.get_all(self.db, redis=self.redis, status=status)

    def get_website(self, _id):
        return Website.get_by_id(_id=_id, redis=self.redis, db=self.db)

    def get_pattern(self, _id=None, url=None):
        assert _id or url, "Provide pattern ID or URL for URLPattern lookup"
        if _id:
            pattern = WebsiteURLPatterns.get_pattern(_id=_id, db=self.db, redis=self.redis)
        else:
            pattern = WebsiteURLPatterns.arbitrary_match(url=url, db=self.db)

            if not pattern:
                raise NoMatchedPatternError(url)
        return pattern

    def get_extracted_page(self, url, scrapper=None,
                           save=True, keep_candidates=False, db_lookup=True):
        return ExtractedPage.get_extract(url,
                                         extractor=self.extractor,
                                         scrapper=scrapper,
                                         save=save,
                                         keep_candidates=keep_candidates,
                                         db_lookup=db_lookup,
                                         db=self.db)

    def get_extracted_page_with_matched_pattern(self, url, scrapper=None,
                                                save=True, keep_candidates=True, db_lookup=True):
        url_mapping = self.get_pattern(url=url)

        extract = self.get_extracted_page(url, scrapper=scrapper,
                                          save=save, keep_candidates=keep_candidates)
        assert extract
        return extract, url_mapping
