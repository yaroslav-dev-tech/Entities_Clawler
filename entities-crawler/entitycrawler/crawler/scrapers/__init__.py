# Scrapers
import re
import logging
import pytz
import requests
from datetime import datetime, timedelta
from urlparse import urljoin, urlparse, urlunparse
from itertools import chain, groupby
from io import StringIO
from operator import itemgetter

import bs4
import boilerpipy as bp
from bs4 import BeautifulSoup
from lxml import etree
from newspaper import Article
from dateutil.parser import parse
from ftfy import fix_text

BSOUP = "SoupScrapper"
NEWSP = "NewspScrapper"
REQUEST_TIMEOUT = 180

from entitycrawler.utils import lazyprop
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


class Scrapper(object):

    name = None

    def __init__(self, url, fuzzy_date=True):
        self.url = url
        self.html = self._download(url)
        self.encoding = 'utf-8'
        self.date = self._get_article_date(self.html, fuzzy_date)

    def _absolutize_url(self, url):
        url = urljoin(self.url, url)
        if isinstance(url, str):
            url = unicode(url, self.encoding)
        return url

    def _remove_hashtag(self, url):
        u = urlparse(url)
        url = urlunparse((u.scheme, u.netloc, u.path, u.params, u.query, ''))
        return url

    def _download(self, url):
        headers = {'User-Agent': 'TrendIn'}
        try:
            html = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=True).content
        except requests.ConnectionError as err:
            raise requests.ConnectionError(err)
        return ' '.join(html.split())

    def _get_article_date(self, html, fuzzy_date):
        '''parse html string'''

        parser = etree.HTMLParser()
        tree = etree.parse(StringIO(unicode(html.decode('utf-8'))), parser)
        # Find all time tags
        time_tags_list = tree.findall('.//time')
        current_time = datetime.now(pytz.utc)
        list_of_dates = []
        for elem in time_tags_list:
            time_tag = parse(elem.attrib.get('datetime', elem.text),
                             fuzzy=True)
            if isinstance(time_tag, datetime):
                time_tag = time_tag.replace(tzinfo=pytz.UTC)
            else:
                continue

            print("Type of time_tag: ", type(time_tag))
            if time_tag < current_time:
                list_of_dates.append(time_tag)

        if list_of_dates:
            print('_get_article_date', list_of_dates)
            return sorted(list_of_dates)[-1]
        elif fuzzy_date is True:
            list_of_regex = [
                r"(?:(?:jan(?:(?:.)?|(?:uary)?)|feb(?:(?:.)?|(?:ruary)?)|mar(?:(?:.)?|(?:ch)?)|apr(?:(?:.)?|(?:il)?)|may|jun(?:(?:.)?|(?:e)?)|jul(?:(?:.)?|(?:y)?)|aug(?:(?:.)?|(?:gust)?)|sep(?:(?:.)?|(?:ept(?:(?:.)?))?|(?:tember)?)|oct(?:(?:.)?|(?:ober)?)|nov(?:(?:.)?|(?:ember)?)|dec(?:(?:.)?|(?:ember)?)) (?:[123][0-9]|[1-9])[ \t\r\f\v]?(?:rd|st|th)?(?:,)?[ \t\r\f\v]?(?:[0-2][0-9][0-9][0-9])?)",
                r"(?:(?:[0]?[1-9])|(?:[1][0-2]))[-/](?:(?:[012]?[0-9])|(?:[3][01]))[/-][12]?[0-9]?[0-9][0-9]",  # find all cases of American style date i.e. 10/12/98
                # r"(?:christmas|memorial day|labor day|halloween|new years eve|new year's eve|mothers day|mother's day|martin luther king day|presidents day|president's day|memorial day| independence day|labor day|columbus day|veterans day|valentines day|valentine's day|halloween|st. patricks day|st. patricks day|veteran's day|thanksgiving|thanksgiving day)", #find all instances of important holidays
                r"(?:mon(?:\.|day)?|tue(?:\.|sday)?|wed(?:\.|nesday)?|thur(?:\.|sday)?|fri(?:\.|day)?|sat(?:\.|urday)?|sun(?:\.|day)?)?[ \t\r\f\v]?the (?:[123][0-9]|[1-9])?[ \t\r\f\v]?(?:rd|st|th)?(?:,)?[ \t\r\f\v]?[ \t\r\f\v]?of?[ \t\r\f\v]?(?:jan(?:\.|uary)?|feb(?:\.|ruary)?|mar(?:\.|ch)?|apr(?:\.|il)?|may|jun(?:\.|e)?|jul(?:\.|y)?|aug(?:\.|ust)?|oct(?:\.|ober)?|nov(?:\.|ember)?|dec(?:\.|ember)?),?[ \t\r\f\v]?(?:[0-2][0-9][0-9][0-9])?",  # find instances of dates formatted like "the 21st of december 2014"
                r"(?:mon(?:|day)?|tue(?:|sday)?|wed(?:|nesday)?|thur(?:|sday)?|fri(?:|day)?|sat(?:|urday)?|sun(?:|day)?)[ \t\r\f\v](?:the)?[ \t\f\r\v]?(?:(?:[123][0-9]|[1-9])[ \t\r\f\v]?(?:rd|st|th)?)?",  # find instances of dates formatted like "Monday the 23rd"
            ]
            dates = set()
            for regex in list_of_regex:
                matches = re.findall(regex, html.decode('utf-8'), re.IGNORECASE)
                map(dates.add, matches)

            for elem in dates:
                try:
                    date = parse(elem).replace(tzinfo=pytz.UTC)
                except:
                    pass
                else:
                    delta = current_time - date
                    if delta.days >= 0:
                        list_of_dates.append(date)
            print('_get_article_date', list_of_dates)
            if len(list_of_dates):
                return sorted(list_of_dates)[-1]
        return None

    def get_page_meta(self):
        ''' Get page metadata '''
        return {'keywords': []}

    def get_text(self):
        '''placeholder for get_text'''
        return '' * 2

    def scrape(self):
        ''' Get all usable info from page '''
        meta = self.get_page_meta()
        links = self.get_links()

        text, highlighted_strings, title = self.get_text()

        result = {'url': self.url,
                  'parser': self.name,
                  'html': self.html,
                  'date': self.date,
                  'metadata': meta,
                  'links': links,
                  'text': text,
                  'title': title,
                  'highlighted_strings': highlighted_strings}
        return result

    def scrape_rss(self):
        ''' Get all usable info from page '''
        meta = self.get_page_meta()

        text, highlighted_strings, title = self.get_text()

        result = {'url': self.url,
                  'parser': self.name,
                  'html': self.html,
                  'date': self.date,
                  'metadata': meta,
                  'links': [],
                  'text': text,
                  'title': title,
                  'highlighted_strings': highlighted_strings}
        return result

    def get_links(self):
        ''' Get all links on page, using RegExe's '''
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                          self.html)
        relative_urls = re.findall('<a href="?\'?([^"\'>]*)',
                                   self.html)
        for url in relative_urls:
            urls.append(self._absolutize_url(url))
        links = set([self._remove_hashtag(l) for l in urls])
        return list(links)


class SoupScrapper(Scrapper):
    version = "3"
    _type = BSOUP
    name = _type + version

    VISIBLE_TAGS = [
        # 'html',
        'body',
        'div',
        'span',
        'h1',
        'h2',
        'h3',
        'h4',
        'h5',
        'h6',
        'p',
        'blockquote',
        'pre',
        'a',
        'abbr',
        'acronym',
        'address',
        'big',
        'cite',
        'code',
        'del',
        'dfn',
        'em',
        'img',
        'ins',
        'kbd',
        'q',
        's',
        'samp',
        'small',
        'strike',
        'strong',
        'sub',
        'sup',
        'tt',
        'var',
        'b',
        'u',
        'i',
        'center',
        'dl',
        'dt',
        'dd',
        'ol',
        'ul',
        'li',
        'fieldset',
        'form',
        'label',
        'legend',
        'table',
        'caption',
        'tbody',
        'tfoot',
        'thead',
        'tr',
        'th',
        'td',
        'article',
        'aside',
        'canvas',
        'details',
        'figcaption',
        'footer',
        'header',
        'hgroup',
        'menu',
        'nav',
        'output',
        'ruby',
        'section',
        'summary',
        'time',
        'mark',
    ]

    SEMANTIC_TEXT_MAX_LEN = 5
    SEMANTIC_TAGS = [
        u'span',
        u'em',
        u'strong',
        u'dfn',
        u'a',
        u'big',
        u'b',
        u'u',
        u'i',
        u'mark',
        u'figcaption',
        u'q',
    ]

    GROUPING_TAGS = [
        'p',
        'div',
        'article',
        'aside',
        'figcaption',
        'main',
        'nav',
        'section',
    ]

    JUNK_CUTOFF = 0.3

    def __init__(self, *args, **kwargs):
        self.use_readability = kwargs.pop('use_readability', False)
        super(SoupScrapper, self).__init__(*args, **kwargs)
        html = self.html
        if self.use_readability:
            self.readability = bp.Extractor(self.html, loglevel=logging.getLogger().getEffectiveLevel())
            html = self.readability.extracted()
        self.soup = BeautifulSoup(html)
        self.encoding = self.extract_encoding()
        self.REPLACEMENT_TAB = dict((ord(char), None) for char in u'@#${}')

    def get_text(self):
        '''Scrape visible text and get highlighted text list with BeautifulSoup'''
        text, highlighted_strings = self._extract_text_pieces()
        if not text:
            return [], []
        text = self._group_text(text)
        text = [t[0].strip() for t in text if t[0].strip()]
        print '/n/n', text
        text, highlighted_strings = self._cut_junk(text, highlighted_strings)
        if self.use_readability:
            title = self.readability.title()
        else:
            title = self.soup.title.string
        text.insert(0, title)
        return text, highlighted_strings, title

    def get_title(self):
        '''get title of the page'''
        return self.soup.title()

    def get_page_meta(self):
        ''' Get page metadata '''

        def tag_key(tag):
            key = (tag.get('name') or
                   tag.get('http-equiv') or
                   tag.get('property') or
                   tag.get('itemprop'))
            return key

        meta = self.soup.find_all('meta')
        meta = groupby(meta, tag_key)
        result = {}
        for key, val in meta:
            content = [t.get('content') for t in val if t.get('content')]
            if content:
                if key == 'keywords':

                    content = [k.split(',') for k in content]
                    content = chain(*content)  # flatten list
                    content = [k.strip() for k in content if k]
                result[key.lower().replace(".", "_")] = content
        return result

    def get_links(self):
        ''' Get all links on page '''
        links = self.soup.find_all('a')
        links = [self._absolutize_url(l.get('href')) for l in links if l.get('href')]
        links = set([self._remove_hashtag(l) for l in links])
        return list(links)

    def extract_encoding(self):
        charset = None
        for m in self.soup.find_all('meta'):
            attrs = {a.lower(): v.lower() for a, v in m.attrs.iteritems()}
            if attrs.get('http-equiv') == 'content-type':
                charset_re = re.compile('.*charset=(.+)$')
                m = charset_re.match(attrs.get('content', ''))
                if m:
                    charset = m.group(1)
                    break
        return charset or 'utf-8'

    def _is_string(self, tag):
        print('/n/n', tag, 'string', type(tag) == bs4.element.NavigableString)
        return type(tag) == bs4.element.NavigableString

    def _is_visible_tag(self, tag):
        print('/n/n', tag, 'visible', tag.name in self.VISIBLE_TAGS)
        return tag.name in self.VISIBLE_TAGS

    def _is_semantic_string(self, s):
        semantic = s.parent.name in self.SEMANTIC_TAGS
        short = len(s.split()) <= self.SEMANTIC_TEXT_MAX_LEN
        return semantic and short

    def _grouping_parent(self, e):
        while e and e.name not in self.GROUPING_TAGS:
            e = e.parent
        return e

    def _extract_text_pieces(self, elements=None):
        text = []
        highlighted_strings = []
        if not elements:
            elements = self.soup.children
        for e in elements:
            if self._is_string(e) and self._is_visible_tag(e.parent):
                t = fix_text(e).strip()
                if t and self._is_semantic_string(e):
                    highlighted_strings.append(t)
                if len(t) > 2:
                    text.append((t, e))
            elif getattr(e, 'children', False):
                t, h = self._extract_text_pieces(e.children)
                text.extend(t)
                highlighted_strings.extend(h)
        return text, highlighted_strings

    def _group_text(self, text):
        if not text:
            return []
        result = text[:1]
        for t, e in text:
            prev_t, prev_e = result[-1]
            if self._grouping_parent(e) == self._grouping_parent(prev_e):
                result[-1] = (u'%s %s' % (prev_t, t), prev_e)
            else:
                result.append((t, e))
        return result

    def _cut_junk(self, text, highlights):
        '''Remove all the pieces with great probability of being some kind of junk
        (ads, menu links, etc) and leave only main parts (article).

        Implemented by removing all text pieces that is too short
        compared to longest piece for this page.

        Also removes all entity candidates (highlights) that don't appear in resulting text.
        '''
        print("text: ", text)
        text = [(t, len(t)) for t in text]
        print("text2: ", text)
        new_highlights = set()
        _, longest = max(text, key=itemgetter(1))
        if longest > 0:
            text = [t for t, length in text
                    if (float(length) / longest) > self.JUNK_CUTOFF]
            for h in highlights:
                for t in text:
                    if h in t:
                        new_highlights.add(h)
                        break
        return text, list(new_highlights)

class NewspaperScrapper(Scrapper):
    version = "2"
    _type = NEWSP
    name = _type + version

    def __init__(self, *args, **kwargs):
        super(NewspaperScrapper, self).__init__(*args, **kwargs)
        self.article = Article(self.url)
        self.article.set_html(self.html)
        self.article.parse()

    def get_text(self):
        text = u". ".join([fix_text(self.article.title), fix_text(self.article.text)])
        highlighted_strings = []
        return [text], highlighted_strings, fix_text(self.article.title)

class ReadabilityScrapper(Scrapper):
    version = "2"
    _type = 'Readability'
    name = _type + version

    def __init__(self, *args, **kwargs):
        super(ReadabilityScrapper, self).__init__(*args, **kwargs)
        self.extractor = bp.Extractor(self.html, loglevel=logging.getLogger().getEffectiveLevel())

    def get_text(self):
        highlighted_strings = []
        text = self.extractor.extracted()
        title = fix_text(self.extractor.title())
        text = fix_text(BeautifulSoup(text).get_text())
        return [text], highlighted_strings, title


class DefaultScrapper(ReadabilityScrapper):
    pass


SCRAPPERS_LIST = [
    (ReadabilityScrapper._type, ReadabilityScrapper.name),
    (SoupScrapper._type, SoupScrapper.name),
    (NewspaperScrapper._type, NewspaperScrapper.name),
]
SCRAPPERS_BY_TYPE = {
    ReadabilityScrapper._type: ReadabilityScrapper,
    SoupScrapper._type: SoupScrapper,
    NewspaperScrapper._type: NewspaperScrapper,
}


class ScrappedPage(object):

    '''
        scraper - scraper backend BSOUP for BeautifullSoup, NEWSP for newspaper lib
    '''

    SCRAPPED_PAGE_FIELDS = {
        'metadata',
        'url',
        'parser',
        'html',
        'date',
        'links',
        'text',
        'title',
        'highlighted_strings',
    }

    def __init__(self, url, scrapper=None, db=None):
        self.db = db
        self._id = None
        self.url = url
        if scrapper:
            scrapper = SCRAPPERS_BY_TYPE.get(scrapper, None)
            if not scrapper:
                raise Exception("Can't find scrapper: %s" % scrapper)
        else:
            scrapper = DefaultScrapper

        if db:
            page = db[CRAWLED_PAGES_COL].find_one({"url": url})
            if self.check_fields(page) is False:
                print("Scrapped page in DB for: ", url, " is not valid, rescrapping..")
                page = None
        else:
            page = None

        if not page:
            page = self.scrape_page(url, scrapper)
            assert self.check_fields(page)

        self.__dict__.update(page)
        self.page = page
        self.scrapper = scrapper

    @classmethod
    def scrape_page(cls, url, scraper=DefaultScrapper):
        '''
            Get text and other data from one page
        '''
        return scraper(url).scrape()

    @classmethod
    def check_fields(cls, page):
        if page is None:
            print("check_fields: page is None")
            return False
        check_result = cls.SCRAPPED_PAGE_FIELDS.issubset(page)
        print("Checked fields in scrapped page [%s], result: %s" %
              (page.get('url', "NoURL"), check_result))
        return check_result

    @lazyprop
    def is_saved(self):
        if self._id is not None:
            return True
        else:
            return False

    def save(self, expire=60):
        self.page['crawled_at'] = datetime.utcnow() + timedelta(seconds=expire)
        opstatus = self.db[CRAWLED_PAGES_COL].update({'url': self.url}, self.page, upsert=True)
        assert opstatus.get(u'upserted', False) or opstatus.get(u'nModified', False)
        self._id = opstatus.get('nUpserted', None)
        return self.is_saved
