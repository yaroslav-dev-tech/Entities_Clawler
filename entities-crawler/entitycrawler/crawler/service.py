import datetime

from entitycrawler.services.classes import AsyncService
from entitycrawler.db import (
    REDIS_SPLIT_SYMBOL,
    CRAWLERS_POOL_NAME,
    CRAWLED_PAGES_COL,
    CRAWLER_QUEUE_PREFIX,
    ensure_crawler_indexes,
)
from classes import Website, WebsiteCrawler, STATUS
from bson import ObjectId


class CrawlerService(object):

    name = 'crawler'
    mongodb_col = CRAWLED_PAGES_COL

    def __init__(self, crawler, db, redis):
        self.db = db
        self.redis = redis
        self.crawler = crawler
        self.name = crawler.name
        self.type = crawler.crawler_type

    def run_job(self):
        print ("running crawler: ", self.name, self.type)
        page = self.crawler.crawler.crawl_page()
        if page is None:
            return {'status': 'Exception',
                    'totalTransactions': 0,
                    'doc': {}}
        self.db.crawlers.update(
            {'_id': ObjectId(self.crawler.crawler.name)},
            {'$inc': {'crawled_pages': 1}})
        extract = self.crawler.extractor.extract(page)
        return {'status': 'OK',
                'totalTransactions': 1,
                'doc': {'page': page,
                        'extract': extract,
                        'crawler': self.crawler}}


class MultiCrawlerService(AsyncService):

    def __init__(self, *args, **kwargs):
        print("MultiCrawlerService init")
        super(MultiCrawlerService, self).__init__(*args, **kwargs)
        ensure_crawler_indexes(self.db)

        self.crawlers = {}
        self.crawlers_paused = {}
        self.init_crawlers()

    def init_crawlers(self):
        ''' check if crawlers in queue '''
        print("MultiCrawlerService init crawlers")
        self.redis.delete(CRAWLERS_POOL_NAME)
        for website in Website.get_all(db=self.db, redis=self.redis, status=Website.ENABLED):
            for crawler in website.crawlers:
                if crawler.enabled:
                    self.start_crawler(crawler)

    def check_crawlers_pool(self):
        print("MultiCrawlerService check pool")
        for website in Website.get_all(db=self.db, redis=self.redis, status=None):
            if website.status == 0:
                for crawler in website.crawlers:
                    self.stop_crawler(crawler, status=-1)
            if website.status == 1:
                for crawler in website.crawlers:
                    if crawler.status == STATUS['disabled']:
                        self.stop_crawler(crawler, status=0)
                    if crawler.status == STATUS['enabled']:
                        self.start_crawler(crawler)

        crawlers_paused = set(self.crawlers_paused.keys())
        print("PAUSED CRAWLERS: ", crawlers_paused)
        for crawler_id in crawlers_paused:
            crawler = self.crawlers_paused[crawler_id]
            if crawler['service'].crawler.crawler.can_resume():
                self.resume_crawler(crawler['crawler'])

        crawlers_enabled = set(self.redis.lrange(CRAWLERS_POOL_NAME, 0, -1))
        print("ENABLED CRAWLERS: ", crawlers_enabled)
        crawlers_active = set(self.crawlers.keys())
        for crawler_id in crawlers_active.difference(crawlers_enabled):
            crawler = self.crawlers[crawler_id]['crawler']
            self.stop_crawler(crawler)

        self.crawlers = {}
        for crawler_id in crawlers_enabled:
            crawler = WebsiteCrawler.get_by_id(crawler_id, db=self.db, redis=self.redis)
            self.start_crawler(crawler)

    def pause_crawler(self, crawler):
        self.crawlers_paused[crawler.id] = self.crawlers.pop(crawler.id)
        self.db.crawlers.update(
            {'_id': ObjectId(crawler.id)},
            {'$set': {'crawling_status': 2}})
        crawler.dequeue()

    def resume_crawler(self, crawler):
        self.crawlers[crawler.id] = self.crawlers_paused.pop(crawler.id)
        self.crawlers[crawler.id]['service'].crawler.crawler.resume()
        self.db.crawlers.update(
            {'_id': ObjectId(crawler.id)},
            {'$set': {'crawling_status': 1}})
        crawler.queue()

    def start_crawler(self, crawler):
        print("MultiCrawlerService start crawler ", crawler.name)
        self.crawlers[crawler.id] = {
            'crawler': crawler,
            'service': CrawlerService(crawler, self.db, self.redis),
        }
        self.db.crawlers.update(
            {'_id': ObjectId(crawler.id)},
            {'$set': {'crawling_status': 1}})
        crawler.queue()

    def stop_crawler(self, crawler, status=0):
        print("MultiCrawlerService stop crawler ", crawler.name)
        self.db.crawlers.update(
            {'_id': ObjectId(crawler.id)},
            {'$set': {'crawling_status': status}})
        crawler.dequeue()
        self.crawlers[crawler.id]['service'].stop()
        del self.crawlers[crawler.id]

    def choose_crawler(self):
        print("MultiCrawlerService choose crawler")
        crawler_id = self.redis.brpoplpush(CRAWLERS_POOL_NAME, CRAWLERS_POOL_NAME, timeout=3)
        crawler = self.crawlers[crawler_id]
        print("got crawler", crawler['service'].name)
        if crawler['service'].crawler.crawler.on_pause:
            print(' crawler on pause')
            self.pause_crawler(crawler['crawler'])
            return None
        return crawler

    def get_item(self):
        print("MultiCrawlerService get item")
        crawler = self.choose_crawler()
        return crawler

    def _process(self, crawler):
        print("MultiCrawlerService _process")
        return crawler['service'].run_job()

    def _every_minute(self):
        print("MultiCrawlerService every minute")

        self.check_crawlers_pool()

    def _save_data(self, result):
        print("MultiCrawlerService _save_data")
        page = result.get('page')
        if not page:
            return
        extracted_page = result['extract']
        crawler = result['crawler']
        page.save()
        extracted_page.save()
        crawler.extractor.save_entities(extracted_page.doc)
        self.db.url_queue.insert({'url': page.url})
        crawler.inc_crawled_count()
