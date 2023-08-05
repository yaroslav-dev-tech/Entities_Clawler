'''
Created on 12 aug. 2014

'''
import logging
import pymongo

from bson import ObjectId

from entitycrawler.entities.sources.freebase import FreebaseEntityImporter
from entitycrawler.db import paginate

log = logging.getLogger("entityextractor")


def ensure_entities_index(db):
    log.info("Making entities indexes")
    db['entities'].ensure_index([
        ('category', pymongo.ASCENDING),
        ('name', pymongo.ASCENDING),
    ], unique=True)
    db['entities'].ensure_index([
        ('occur', pymongo.DESCENDING),
    ])
    db['entities'].ensure_index([
        ('name', pymongo.ASCENDING),
    ])
    db['entities'].ensure_index([
        ('_name', pymongo.ASCENDING),
    ])
    db.entity_source.ensure_index([
        ('source_category', pymongo.ASCENDING),
        ('source', pymongo.ASCENDING),
    ], unique=True)


def fetch_entity_categories(db):
    log.info('Fetching top-level categories from freebase into mongo')
    key = "AIzaSyDI1GlWwoLElDNczf3eBWiOycD9-H1qW5I"
    e = FreebaseEntityImporter(db, freebase_api_key=key)
    e.import_categories()


def get_categories(db):
    return list(
        db.entity_source.find().sort(
            [('source', pymongo.ASCENDING),
             ('source_category', pymongo.ASCENDING)]))


def get_category_by_id(db, _id):
    return db.entity_source.find_one({'_id': ObjectId(_id)})


def update_category(db, _id, update):
    return db.entity_source.update({'_id': ObjectId(_id)}, update)


def add_category(db, data):
    try:
        return db.entity_source.insert(data)
    except pymongo.errors.DuplicateKeyError:
        return False


def add_entity(db, data):
    try:
        return db.entities.insert(data)
    except pymongo.errors.DuplicateKeyError:
        return False


def get_entities(db, q):
    return list(db.entities.find(q.update({'disabled': {'$ne': True}})).limit(25))


def get_entities_by_category(db, category, size=25, page=1):
    entities, pages_count = paginate(db.entities.find({'category': category}), page=page, per_page=size)
    return entities, pages_count


def remove_entity(db, _id):
    return db.entities.remove({'_id': ObjectId(_id)})
