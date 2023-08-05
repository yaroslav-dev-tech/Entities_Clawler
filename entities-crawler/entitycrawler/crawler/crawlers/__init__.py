import datetime as dt
import logging
import requests
import re
from BeautifulSoup import BeautifulSoup
import feedparser
import urllib2

from entitycrawler.crawler.scrapers import ScrappedPage
from entitycrawler.db import CRAWLER_QUEUE_PREFIX

log = logging.getLogger("crawler")
log.level = logging.DEBUG


class CrawlerClass(object):
    name = ''

    def __init__(self, scrapper, crawler_manager):
        self.scrapper = scrapper
        self.crawler = crawler_manager
        self.redis = crawler_manager.redis
        self.db = crawler_manager.db
        self.name = crawler_manager.id

        self.start_url_crawled_at = None
        self.on_pause = False

    def get_url(self):
        ''' Get URL from queue, or try to generate queue '''
        log.debug("geturl")
        url = self.redis.spop(CRAWLER_QUEUE_PREFIX + self.name)
        log.debug("url from redis %s", url)
        if url is None:
            url = self._generate_urls()
            log.debug("generated url %s", url)
        if not self.crawler.check_url_age(url):
            url = self.get_url()
        log.debug("got url: %s", url)
        return url

    @staticmethod
    def is_html(url):
        if re.search('.html|.htm', url) is None:
            resp = requests.head(url)
            if 'content-type' in resp.headers:
                if resp.headers['content-type'].split(';')[0] != 'text/html':
                    log.debug("wrong content type: " + url)
                    return False
            return True
        else:
            return True

    def valid_url(self, url):
        return self.crawler.url_patterns.validate_link(url)

    def can_resume(self):
        '''If crawler was paused before for some reason - can it resume right now?'''
        return self._can_crawl_start_page()

    def pause(self):
        self.on_pause = True

    def resume(self):
        self.on_pause = False

    def _can_crawl_start_page(self):
        log.debug('Last crawled at %s (freq: %s)', self.start_url_crawled_at,
                  self.crawler.frequency)
        now = dt.datetime.utcnow()
        if not self.start_url_crawled_at:
            self.start_url_crawled_at = now
            return True
        freq = dt.timedelta(seconds=int(self.crawler.frequency))
        expire = self.start_url_crawled_at + freq
        if expire > now:
            log.debug('  skipping until %s', expire)
            return False
        log.debug('  crawling again')
        self.start_url_crawled_at = now
        return True

    def crawl_page(self):
        url = self.get_url()
        if url is None:
            return None

        if url == self.crawler.start_url:
            log.debug('Start url reached')
            if not self.can_resume():
                return self.pause()

        if not self.is_html(url):
            return None

        log.debug("crawling url %s", url)
        try:
            page = ScrappedPage(url=url, scrapper=self.scrapper._type,
                                db=self.db)
        except Exception as e:
            log.debug("Exception in scrapper for url: %s\n%s", url, str(e))
            return None
        assert isinstance(page.page, dict)
        assert isinstance(page, ScrappedPage)
        self._process_links(page.page)
        return page


class LinksCrawler(CrawlerClass):
    _type = "links_crawler"

    def _process_links(self, page):
        links = page['links']
        for url in links:
            if self.valid_url(url):
                log.debug("found url: %s", url)
                self.redis.sadd(CRAWLER_QUEUE_PREFIX + self.name, url)

    def _generate_urls(self):
        return self.crawler.start_url


class SitemapCrawler(CrawlerClass):
    _type = "sitemap_crawler"

    @staticmethod
    def has_sitemaps(url):
        ''' Check if site has sitemap '''
        return False


FEED_LINKS_ATTRIBUTES = [
    'application/rss+xml',
    'application/atom+xml',
    'application/rss',
    'application/atom',
    'application/rdf+xml',
    'application/rdf',
    'text/rss+xml',
    'text/atom+xml',
    'text/rss',
    'text/atom',
    'text/rdf+xml',
    'text/rdf'
    'text/xml',
    'application/xml',
]


class RSSCrawler(CrawlerClass):
    _type = "rss_crawler"

    @staticmethod
    def find_rss(url):
        page = urllib2.urlopen(url)
        soup = BeautifulSoup(page)
        link = soup.find('link', type='application/rss+xm')
        if link is not None:
            return str(link['href'])
        else:
            return None

    @staticmethod
    def is_rss(url):
        resp = requests.head(url)
        if 'content-type' in resp.headers:
            content_type = resp.headers['content-type'].split(';')
            if content_type[0] in FEED_LINKS_ATTRIBUTES:
                log.debug("valid rss_feed: %s ", url)
                return True
        log.debug("not rss_feed content-type in:%s ", url)
        return False

    def _parse_rss(self, url):
        rss = feedparser.parse(url)
        first_url = rss.entries.pop()
        for post in rss.entries:
            log.debug("found url: %s", post.link)
            self.redis.sadd(CRAWLER_QUEUE_PREFIX + self.name, post.link)
        return first_url.link

    def _generate_urls(self):
        if not self.can_resume():
            return self.pause()
        rss = feedparser.parse(self.crawler.start_url)
        if len(rss.entries) == 0:
            return None
        url = rss.entries.pop()
        for post in rss.entries:
            log.debug("found url: %s", post.link)
            self.redis.sadd(CRAWLER_QUEUE_PREFIX + self.name, post.link)
        return url.link

    def crawl_page(self):
        url = self.get_url()
        if url is None:
            return None

        if not self.is_html(url):
            return None

        log.debug("crawling url %s", url)
        try:
            page = self.scrapper(url).scrape_rss()
        except Exception as e:
            log.debug("Exception in scrapper for url: %s\n%s", url, str(e))
            return None
        assert isinstance(page, dict)
        return page


def choose_crawler_type(url):
    ''' Check if we should use sitemaps '''
    log.info(" choose_crawler_type for url: %s ", url)
    if SitemapCrawler.has_sitemaps(url):
        return SitemapCrawler._type
    else:
        return LinksCrawler._type


CRAWLERS_LIST = [
    (RSSCrawler._type, RSSCrawler._type),
    (LinksCrawler._type, LinksCrawler._type),
    (SitemapCrawler._type, SitemapCrawler._type)
]
