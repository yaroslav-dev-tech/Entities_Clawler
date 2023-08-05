import pymongo
import logging
from entities.db import ensure_entities_index, fetch_entity_categories


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("entityextractor")

class AttrDict(dict):
    """A dictionary with attribute-style access. It maps attribute access to
    the real dictionary.  """
    def __init__(self, init={}):
        dict.__init__(self, init)

    def __getstate__(self):
        return self.__dict__.items()

    def __setstate__(self, items):
        for key, val in items:
            self.__dict__[key] = val

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

    def __setitem__(self, key, value):
        return super(AttrDict, self).__setitem__(key, value)

    def __getitem__(self, name):
        return super(AttrDict, self).__getitem__(name)

    def __delitem__(self, name):
        return super(AttrDict, self).__delitem__(name)

    __getattr__ = __getitem__
    __setattr__ = __setitem__

    def copy(self):
        ch = AttrDict(self)
        return ch


def _make_db(host, port, db_name, user, password):
    conn = pymongo.MongoClient(host, int(port))
    db = conn[db_name]
    if user != "":
        log.info("Authentication to db")
        db.authenticate(user, password)
    return db


def ensure_indexes(host='127.0.0.1', port=27017, db_name='entityextractor',
                   user="", password=""):
    db = _make_db(host, port, db_name, user, password)
    ensure_entities_index(db)


def insert_initial_data(host='127.0.0.1', port=27017, db_name='entityextractor',
                        user="", password=""):
    db = _make_db(host, port, db_name, user, password)
    fetch_entity_categories(db)


def lazyprop(fn):
    # lazy property
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazyprop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazyprop


if __name__ == '__main__':
    ensure_indexes()
    insert_initial_data()
