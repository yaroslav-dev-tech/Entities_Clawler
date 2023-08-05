try:
    import simplejson as json
except ImportError:
    import json

import datetime
from urlparse import urlparse

from bson import ObjectId
from entitycrawler.crawler import WebsiteCrawler, Website, WebsiteURLPatterns
from flask import Blueprint, Flask, jsonify, request, current_app, render_template
from flask.ext.cors import cross_origin
from web import entity_db, redis
from web.auth import get_user
from web.decorators import crossdomain, jsonp, requires_auth
from web.utils import get_domain_from_url
mod = Blueprint('crawler', __name__, template_folder='templates')


def get_counters_for_domain(domain):
    host = urlparse(domain["website_url"]).hostname
    if host.startswith('www.'):
        other_host = host[4:]
    else:
        other_host = 'www.' + host
    indexed_pages = entity_db.extracted_pages.find(
        {"site": {'$in': [host, other_host]}}).count()
    waiting_urls = entity_db.extracted_pages.find(
        {'approval': 'awaiting', 'site': {'$in': [host, other_host]}}).count()
    return host, indexed_pages, waiting_urls


@mod.route("/", methods=['GET', 'OPTIONS'])
@jsonp
def main():
    results = {'ok': 'OK'}
    return jsonify(**{'results': results})


@mod.route("/domains", methods=['GET', 'OPTIONS'])
def list_domains():
    websites = entity_db.website.find({})
    domains = []
    for site in websites:
        host, indexed_pages, waiting_urls = get_counters_for_domain(site)
        domains.append({'_id': site['_id'],
                        'name': site['name'],
                        'domain':  get_domain_from_url(site['website_url']),
                        'category': site['category'],
                        'site': site['website_url'],
                        'indexed_pages': indexed_pages,
                        'waiting_urls': waiting_urls})
    if not domains:
        return jsonify(**{'status': 'No websites'})
    else:
        return jsonify(**{'domains': domains})


@mod.route("/domain/<object_id>", methods=['GET', 'POST', 'OPTIONS'])
def domain_view(object_id):
    try:
        domain_id = ObjectId(object_id)
    except Exception:
        return jsonify(**{'status': 'Invalid ID for domain'})
    # domain = entity_db.website.find_one({'_id': domain_id})
    website = Website.get_by_id(_id=domain_id, db=entity_db)
    domain = website.doc

    if request.method == 'GET':
        if domain is None:
            return jsonify({'status': 'No such domain'})

        host, indexed_pages, waiting_urls = get_counters_for_domain(domain)
        domain['indexed_pages'] = indexed_pages
        domain['waiting_urls'] = waiting_urls
        domain['url_patterns'] = website.get_url_patterns()
        domain['domain'] = get_domain_from_url(domain['website_url'])
        return jsonify(**domain)

    if request.method == 'POST':
        try:
            data = json.loads(request.data)
            website.update(doc=data)
            domain = Website.get_by_id(_id=domain_id, db=entity_db).doc
            return jsonify(**domain)
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/domain/add", methods=['POST', 'OPTIONS'])
def domain_add():
    """add domain"""
    if request.method == 'POST':
        try:
            data = json.loads(request.data)
            domain = {
                "category": unicode(data.get("category", '')),
                "name": unicode(data.get("name", '')),
                "publisher_name": unicode(data.get("publisher_name", '')),
                "website_url": unicode(data.get("website_url", ''))
            }
            domain = Website.create(publisher_name=domain['publisher_name'],
                                    website_name=domain['name'],
                                    website_url=domain['website_url'],
                                    category=domain['category'],
                                    db=entity_db,
                                    redis=redis)
            return jsonify(**domain.doc)
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/add", methods=['POST', 'OPTIONS'])
def crawler_add():
    """add crawler"""
    if request.method == 'POST':
        try:
            data = json.loads(request.data)
            crawler = WebsiteCrawler.create(
                kwargs=data, db=entity_db, redis=redis)
            return jsonify(**crawler.doc)
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/delete/<crawler_id>", methods=['GET', 'OPTIONS'])
def crawler_delete(crawler_id):
    """delete crawler with url_pattrens"""
    if request.method == 'GET':
        try:
            crawler_id = ObjectId(crawler_id)
        except Exception:
            return jsonify(**{'status': 'Invalid ID for crawler'})

        try:
            crawler = WebsiteCrawler.get_by_id(
                _id=crawler_id, db=entity_db, redis=redis)
            crawler.delete()
            return jsonify(**{'status': 'SUCCESS'})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/<crawler_id>", methods=['GET', 'POST', 'OPTIONS'])
def crawler_upd(crawler_id):
    """update/get crawler"""
    try:
        crawler_id = ObjectId(crawler_id)
    except Exception:
        return jsonify(**{'status': 'Invalid ID for crawler'})

    crawler = WebsiteCrawler.get_by_id(
        _id=crawler_id, db=entity_db, redis=redis)
    if request.method == 'GET':
        try:
            return jsonify(**crawler.doc)
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})

    if request.method == 'POST':
        try:
            data = json.loads(request.data)
            crawler.update(doc=data)
            return jsonify(**crawler.doc)
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/crawlers", methods=['GET', 'OPTIONS'])
def crawlers():
    """get all crawlers"""
    if request.method == 'GET':
        try:
            crawlers = WebsiteCrawler.get_all(db=entity_db, redis=redis)
            return jsonify(**{'crawlers':  [c.doc for c in crawlers]})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/url_patterns/<crawler_id>", methods=['GET', 'POST', 'OPTIONS'])
def url_patterns(crawler_id):
    """get/add crawler url_patterns """
    try:
        crawler_id = ObjectId(crawler_id)
    except Exception:
        return jsonify(**{'status': 'Invalid ID for crawler'})

    crawler = WebsiteCrawler.get_by_id(
        _id=crawler_id, db=entity_db, redis=redis)

    if request.method == 'GET':
        try:
            url_pattern = WebsiteURLPatterns(crawler)
            return jsonify(**{'crawler_id': crawler_id, 'url_patterns':  url_pattern.get_list()})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})

    if request.method == 'POST':
        try:
            data = json.loads(request.data)
            url_pattern = WebsiteURLPatterns(crawler)
            url_pattern.create(default_pattern=data, crawler=crawler)
            return jsonify(**{'crawler_id': crawler_id, 'url_patterns':  url_pattern.get_list()})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/url_patterns", methods=['GET', 'POST', 'OPTIONS'])
def all_patterns():
    """all url_patterns and crawlers"""
    crawlers = WebsiteCrawler.get_all(db=entity_db, redis=redis)

    if request.method == 'GET':
        try:
            for crawler in crawlers:
                crawler.doc['url_patterns'] = WebsiteURLPatterns(
                    crawler).get_list()
            return jsonify(**{'crawlers':  [c.doc for c in crawlers]})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})


@mod.route("/url_patterns/del/<pattern_id>", methods=['GET', 'OPTIONS'])
def url_pattern_del(pattern_id):
    """delete url_patterns """
    try:
        pattern_id = ObjectId(pattern_id)
    except Exception:
        return jsonify(**{'status': 'Invalid ID for url_pattern'})

    if request.method == 'GET':
        try:
            WebsiteURLPatterns.delete(db=entity_db, _id=pattern_id)
            return jsonify(**{'status': 'SUCCESS'})
        except Exception as e:
            return jsonify(**{'status': 'FAIL', 'msg': e})
