import pymongo
from math import ceil


CRAWLERS_POOL_NAME = 'crawlers'
CRAWLER_QUEUE_PREFIX = 'crawler_'
REDIS_SPLIT_SYMBOL = '||'


URL_MATCHING_COL = "url_patterns"
CRAWLED_PAGES_COL = "crawled_pages"
EXTRACTED_PAGES_COL = "extracted_pages"
WEBSITES_ENTITIES_COL = "website_entities"
WEBSITES_CANDIDATES_COL = "website_candidates"
WEBSITES_COL = "website"
CRAWLERS_COL = "crawlers"

CRAWLER_INDEX_CHECKED = False
EXTRACTOR_INDEX_CHECKED = False
CRAWLED_PAGES_INDEX_CHECKED = False


def paginate(cursor, page, per_page=25, max_count=250):
    objects_count = min(cursor.count(), max_count)
    pages_count = int(ceil(objects_count/float(per_page)))
    data = list(cursor.skip(per_page * (page - 1)).limit(per_page))
    return data, pages_count


def ensure_crawled_pages_index(db):
    global CRAWLED_PAGES_INDEX_CHECKED
    if not CRAWLED_PAGES_INDEX_CHECKED:
        db[CRAWLED_PAGES_COL].ensure_index('crawled_at', expireAfterSeconds=3600)
        CRAWLED_PAGES_INDEX_CHECKED = True


def ensure_crawler_indexes(db):
    global CRAWLER_INDEX_CHECKED
    if not CRAWLER_INDEX_CHECKED:
        db[CRAWLED_PAGES_COL].ensure_index([
            ('url', pymongo.ASCENDING),
        ])
        CRAWLER_INDEX_CHECKED = True


def ensure_extractor_indexes(db):
    global EXTRACTOR_INDEX_CHECKED
    if not EXTRACTOR_INDEX_CHECKED:
        db[EXTRACTED_PAGES_COL].ensure_index([
            ('url', pymongo.ASCENDING),
        ])
        db[EXTRACTED_PAGES_COL].ensure_index([
            ('site', pymongo.ASCENDING),
        ])
        
        
        db[WEBSITES_ENTITIES_COL].ensure_index([
            ('site', pymongo.ASCENDING),
        ])
        db[WEBSITES_ENTITIES_COL].ensure_index([
            ('count', pymongo.DESCENDING),
        ])
        db[WEBSITES_ENTITIES_COL].ensure_index([
            ('sentiment', pymongo.ASCENDING),
        ])
        db[WEBSITES_ENTITIES_COL].ensure_index([
            ('name', pymongo.DESCENDING),
        ])
        db[WEBSITES_ENTITIES_COL].ensure_index([
            ('name', pymongo.ASCENDING),
            ('site', pymongo.ASCENDING),
        ], unique=True, drop_dups=True)
        
        
        db[WEBSITES_CANDIDATES_COL].ensure_index([
            ('site', pymongo.ASCENDING),
        ])
        db[WEBSITES_CANDIDATES_COL].ensure_index([
            ('count', pymongo.DESCENDING),
        ])
        db[WEBSITES_CANDIDATES_COL].ensure_index([
            ('name', pymongo.DESCENDING),
        ])
        db[WEBSITES_CANDIDATES_COL].ensure_index([
            ('name', pymongo.ASCENDING),
            ('site', pymongo.ASCENDING),
        ], unique=True, drop_dups=True)
        EXTRACTOR_INDEX_CHECKED = True
