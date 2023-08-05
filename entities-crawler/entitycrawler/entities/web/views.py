try:
    import simplejson as json
except ImportError:
    import json

import datetime
from flask import Blueprint, Flask, jsonify, request, current_app, render_template
from flask.ext.cors import cross_origin
from web.utils import make_packet
from web.decorators import crossdomain, jsonp, requires_auth
from web.auth import get_user
from web import db, redis, entity_db

from entitycrawler.entities import db as edb

mod = Blueprint('entities', __name__, template_folder='templates')


@mod.route("/", methods=['GET', 'OPTIONS'])
@jsonp
def main():
    results = {'ok': 'OK'}
    return jsonify(**{'results': results})


@mod.route("/categories", methods=['GET', 'OPTIONS'])
@jsonp
def categories():
    results = {
        'ok': 'OK',
        'categories': edb.get_categories(entity_db),
    }
    return jsonify(**{'results': results})
