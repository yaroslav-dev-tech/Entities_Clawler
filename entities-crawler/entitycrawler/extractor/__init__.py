import math
import datetime
import os
import re
from collections import Counter
from urlparse import urlparse
import nltk
import copy
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktParameters

from entitycrawler.db import (EXTRACTED_PAGES_COL,
                              WEBSITES_ENTITIES_COL,
                              WEBSITES_CANDIDATES_COL,
                              ensure_extractor_indexes)

import entitycrawler.crawler
from entitycrawler.extractor.exceptions import ExtractionError
from entitycrawler.crawler.scrapers import ScrappedPage
from entitycrawler.utils import lazyprop
try:
    from web import db as unitsdb
except:
    print("Couldn't import UnitsDB connection")
    unitsdb = None

current_path = os.path.dirname(os.path.realpath(__file__))

ENTITIES_CACHE = dict()
CANDIDATES_CACHE = set()
LOCAL_CACHE_SIZE = 120


class Entity(object):

    def __init__(self, doc, db):
        self.doc = doc
        self.db = db
        self.name = doc['name']
        self.category = doc['category']
        self.disabled = doc.get('disabled', False)
        self._id = doc['_id']
        self.sentiment = None

    @classmethod
    def check(cls, name, db):
        ''' Check if entity in our entities database '''

        if name in ENTITIES_CACHE:
            return ENTITIES_CACHE[name]
        if name in CANDIDATES_CACHE:
            return None

        result = db.entities.find_and_modify({'_name': name.lower()},
                                             update={"$inc": {"occur": 1}})

        if result is None or result.get('disabled', False) is True:

            if len(CANDIDATES_CACHE) >= LOCAL_CACHE_SIZE:
                CANDIDATES_CACHE.clear()
            else:
                CANDIDATES_CACHE.add(name)
            return None
        else:
            entity = cls(result, db)

            if len(ENTITIES_CACHE) >= LOCAL_CACHE_SIZE:
                ENTITIES_CACHE.clear()
            else:
                ENTITIES_CACHE[name] = entity
            return entity

    def key(self):
        return self.name + "." + self.category

    def update_sentiment(self, sentiment):
        if self.sentiment is not None:

            count = self.sentiment['count'] + 1
            new_score = (self.sentiment['score'] * (count - 1) + sentiment) / count
            self.sentiment = {'count': count,
                              'score': new_score}
        else:
            self.sentiment = {'count': 1,
                              'score': sentiment}

    def db_representation(self):
        data_struct = {
            '_id': self._id,
            'name': self.name,
            'category': self.category,
            'sentiment': self.sentiment,
            'disabled': self.disabled
        }
        return data_struct

    def dict_representation(self):
        data_struct = self.db_representation()
        data_struct['type'] = 'entity'
        return data_struct

    def __repr__(self):
        return "Entity(" + self.doc.__repr__() + ", " + self.db.__repr__() + ")"

    def __unicode__(self):
        return "Entity[" + self.name + "]"


class EntitiesBag(Counter):

    def __init__(self, entities=[], **kwargs):
        super(EntitiesBag, self).__init__(
            self._preprocess_entitites(entities),
            **kwargs)

    def update(self, entities, **kwargs):
        super(EntitiesBag, self).update(
            self._preprocess_entitites(entities),
            **kwargs)

    def sorted(self):
        return [i[0] for i in self.most_common()]

    def _preprocess_entitites(self, entities):
        if isinstance(entities, list):
            entities = [e.name if isinstance(e, Entity) else e for e in entities]
        return entities

    def add_weight(self, weight):
        print('add ', weight, ' to:\n', self)
        for entity in self.iterkeys():
            self[entity] = self[entity] * weight
        print("added weight:\n", self)
        return self

    def __repr__(self):
        return "EntitiesBag(" + dict(self).__repr__() + ")"

    def __unicode__(self):
        return "Bag of Entities"


class SentimentClassificator:

    SENTIMENT_CALIBRATION_PARAMETER = 2

    def __init__(self):
        nltk.data.path.append(os.path.join(current_path, "./datasets/nltk_data/"))

        with open(os.path.join(current_path, "datasets/AFINN-111.txt"), 'r') as afinnfile:
            scores = {}
            for line in afinnfile:
                term, score = line.split("\t")
                scores[term] = int(score)
        self.scores = scores

        self.pattern_split = re.compile(r"\W+")

    def _sigmoid(self, x):
        x = x * self.SENTIMENT_CALIBRATION_PARAMETER
        sigmoid = (1 / (1 + math.exp(-x))) * 2 - 1
        return sigmoid

    def get_sentiment(self, text):
        ''' Compute sentiment for text using word-sentiment table '''

        words = self.pattern_split.split(text.lower())
        sentiments = map(lambda word: self.scores.get(word, 0), words)

        sentiments = [i for i in sentiments if i != 0]
        sentiments_count = len(sentiments)

        if sentiments_count > 0:
            sentiment = float(sum(sentiments)) / sentiments_count
            sentiment = self._sigmoid(sentiment)
        else:
            sentiment = 0.0

        return sentiment

    def get_sentiment_class(self, sentiment):
        if sentiment > 0:
            sentiment_type = "positive"
        elif sentiment < 0:
            sentiment_type = "negative"
        else:
            sentiment_type = "neutral"
        return sentiment_type

    def classify(self, text):
        sentiment = self.get_sentiment(text)
        sentiment_type = self.get_sentiment_class(sentiment)
        return sentiment_type, sentiment


class ExtractedPage(object):

    EXTRACTED_PAGE_FIELDS = {
        'extractor',
        'parser',
        'text',
        'site',
        'url',
        'extracted_at',
        'keywords',
        'entities',
        'candidates',
        'suggested_entities',
        'title'
    }

    PATTERN_RELATED_FIELDS = {
        'category',
        'exclude',
        'url_pattern_id'
    }

    def __init__(self, doc, db_connection=None):
        self.__dict__ = doc
        self.doc = copy.deepcopy(doc)
        self.db = db_connection
        self._id = None
        self.url_pattern_id = doc.get('url_pattern_id', None)
        self.must_match_url_pattern = None

    @classmethod
    def check_fields(cls, page):
        if page is None:
            return False
        return cls.EXTRACTED_PAGE_FIELDS.issubset(page)

    def check_pattern_fields(self):
        return self.PATTERN_RELATED_FIELDS.issubset(self.doc)

    @classmethod
    def get_extract(cls,
                    url,
                    extractor,
                    scrapper=None,
                    save=False,
                    keep_candidates=True,
                    db_lookup=True,
                    db=None,
                    must_match_url_pattern=False):
        if db_lookup:
            page_extract = extractor.get_extract(url)
        else:
            page_extract = None

        if not page_extract:
            page_scrap = ScrappedPage(url, scrapper, db=db)
            if save and not page_scrap.is_saved:
                page_scrap.save()
            try:
                if save:
                    page_extract = extractor.extract_and_save(page_scrap, keep_candidates=keep_candidates)
                else:
                    page_extract = extractor.extract(page_scrap)
            except ExtractionError as e:
                print(e.message)
                raise
            assert len(page_extract.doc['entities']) == 0 or len([e for e in page_extract.doc['entities'] if 'type' in e['sentiment']]) > 0
            assert cls.EXTRACTED_PAGE_FIELDS.issubset(page_extract.doc)

        if page_extract.url_pattern is None and must_match_url_pattern:
            print('Must match at least one registered URL Pattern')
            return None

        page_extract.must_match_url_pattern = must_match_url_pattern
        return page_extract

    @lazyprop
    def url_pattern(self):
        if self.url_pattern_id:
            return entitycrawler.crawler.WebsiteURLPatterns.get_pattern(_id=self.url_pattern_id, db=self.db)
        pattern = entitycrawler.crawler.WebsiteURLPatterns.arbitrary_match(url=self.url, db=self.db)
        if pattern:
            self.url_pattern_id = pattern._id
        return pattern

    @lazyprop
    def is_saved(self):
        if self._id is not None:
            return True
        else:
            return False

    @lazyprop
    def categories(self):
        if self.doc.get('category', None) is not None:
            return self.doc['category']
        if self.url_pattern:
            return self.url_pattern.harvester_categories
        else:
            return None

    @lazyprop
    def exclude(self):
        if self.doc.get('exclude', None) is not None:
            return self.doc['exclude']
        return self.url_pattern.exclude if self.url_pattern else []

    @lazyprop
    def entities(self):
        return [Entity(e, self.db) for e in self.doc.get('entities', [])]

    def save(self, db=None):
        self.doc['category'] = self.categories
        self.doc['exclude'] = self.exclude
        self.doc['url_pattern_id'] = self.url_pattern_id
        self.doc['extracted_at'] = datetime.datetime.utcnow()
        db = db or self.db
        opstatus = db[EXTRACTED_PAGES_COL].update({'url': self.doc['url']},
                                                  self.doc, upsert=True)
        print('Save opstatus', opstatus)
        self._id = opstatus.get('nUpserted')
        return self.is_saved

    def update(self, changed_fields):
        for field, value in changed_fields.iteritems():
            self.__dict__[field] = value
            self.doc[field] = value


class EntityExtractor(object):
    ver = "2"
    _type = "entitycrawler_extractor"
    name = _type + ver
    entities_cache = {}
    candidates_cache = []

    TITLE_WEIGHT = ENTITIES_OVER_CANDIDATES_WEIGHT = 2

    def __init__(self, mongodb):
        self.db = mongodb
        ensure_extractor_indexes(mongodb)
        self.classificator = SentimentClassificator()
        self.pattern_split = re.compile(r"\W+")
        punkt_param = PunktParameters()
        punkt_param.abbrev_types = ('dr', 'vs', 'mr', 'mrs', 'prof', 'inc')
        self.sentence_splitter = PunktSentenceTokenizer(punkt_param)

    def wrap_entities_for_db(self, scored_entities):
        ''' convert Entity Object
            to list of items {entity fields,
                "sentiment" :{ "score": sentiment, "count": int, "type": sent_type } } '''
        wrapped_entities = []
        print("Wrapping scored_entities: ", scored_entities)
        for key, entity in scored_entities.iteritems():
            if entity.sentiment['score'] > 0:
                sentiment_type = "positive"
            elif entity.sentiment['score'] < 0:
                sentiment_type = "negative"
            else:
                sentiment_type = "neutral"
            entity.sentiment = {
                "score": entity.sentiment['score'],
                "count": entity.sentiment['count'],
                "type": sentiment_type
            }
            wrapped_entities.append(entity.db_representation())
        print("WRAPPED entities: ", wrapped_entities)
        return wrapped_entities

    def wrap_candidates_for_db(self, scored_candidates):
        ''' convert dict with items "candidate_name": {"count": count, "score": sentiment}
            to list of items {"name":"candidate_name",
                                 "sentiment" :{ "score": sentiment, "count": int, "type": sent_type } } '''
        wrapped_candidates = []

        for name, e in scored_candidates.iteritems():
            if e['score'] > 0:
                sentiment_type = "positive"
            elif e['score'] < 0:
                sentiment_type = "negative"
            else:
                sentiment_type = "neutral"
            entity = {"name": name,
                      "sentiment": {
                          "score": e['score'],
                          "count": e['count'],
                          "type":  sentiment_type}
                      }
            wrapped_candidates.append(entity)

        return wrapped_candidates

    def _updated_sentiment(self, entity, sentiment, items_dict, keyword=False):
        print("updating sentiment(", sentiment, " for : ", entity)
        if isinstance(entity, Entity):
            entity.update_sentiment(sentiment)
            print("UPDated Entity: ", entity.__repr__())
            return entity
        else:
            key = entity
            if key in items_dict and 'sentiment' in items_dict[key]:
                count = items_dict[key]['sentiment']['count'] + 1
                if keyword:
                    new_score = sentiment
                else:
                    new_score = (items_dict[key]['sentiment']['score'] * (count - 1) + sentiment) / count
                new_sentiment = {'count': count,
                                 'score': new_score}
            else:
                new_sentiment = {'count': 1,
                                 'score': sentiment}
            print("Updated Non Entity: ", new_sentiment)
            return new_sentiment

    def _suggested_entities(self,
                            entities,
                            title_entities,
                            candidates=None,
                            title_candidates=None):
        '''
            Score entities occurrences and sort entities
            based on occurrences.
        '''
        print('_suggested_entities\n')

        weighted_entities = EntitiesBag(title_entities)
        weighted_entities.add_weight(self.TITLE_WEIGHT)
        weighted_entities.update(entities)
        weighted_entities = weighted_entities.add_weight(self.ENTITIES_OVER_CANDIDATES_WEIGHT)
        if len(title_candidates) > 0 or len(candidates) > 0:
            weighted_candidates = EntitiesBag(title_candidates)
            weighted_candidates.add_weight(self.TITLE_WEIGHT)
            weighted_candidates.update(candidates)
            weighted_entities.update(weighted_candidates)
        return weighted_entities.sorted()

    def get_sentiment(self, sentance, sent_entities, sent_candidates,
                      scored_entities={}, scored_candidates={}):
        ''' Compute sentiment for entities in sentance using word-sentiment table '''
        entities_count = len(sent_entities)
        candidates_count = len(sent_candidates)
        if entities_count == 0 and candidates_count == 0:
            return scored_entities, scored_candidates

        sentiment = self.classificator.get_sentiment(sentance)
        print("get_sent : sent entities: ", sent_entities)
        print("get_sent : sent_candidates: ", sent_candidates)
        for e in sent_entities:
            scored_entities[e.key()] = self._updated_sentiment(e, sentiment, scored_entities)
        for c in sent_candidates:
            scored_candidates[c] = self._updated_sentiment(c, sentiment, scored_candidates)
        print("get_sent: finish")

        return scored_entities, scored_candidates

    def named_entity_extractor(self, text):
        ''' Detect entities candidates and check them
            returns: entities_list, candidates_list, text_without_entities (for sentiment analisys) '''

        print('named_entity_extractor')
        sent_no_entities = []
        sent_entities = []
        sent_candidates = []
        try:
            chunks = nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(text)))
        except Exception as e:
            print(e)
        for chunk in chunks:
            print("detect named entity chunks out of position tags")
            if isinstance(chunk, nltk.tree.Tree):
                entity_candidate = u" ".join(c[0] for c in chunk.leaves())
                if len(entity_candidate) < 2:
                    continue
                checked_entity = Entity.check(entity_candidate, self.db)
                if checked_entity is not None:
                    sent_entities.append(checked_entity)
                else:
                    sent_candidates.append(entity_candidate)
                    sent_no_entities.append(entity_candidate)
            else:
                sent_no_entities.append(chunk[0])

        return sent_entities, sent_candidates, u" ".join(sent_no_entities)

    def extract_entities(self, title, text, highlighted_strings=[]):
        ''' returns two dicts - entities and entity_candidates
            with items in format "entity_name": { "count": count, "score": sentiment} '''
        entities = []
        candidates = []
        scored_text_entities = {}
        scored_text_candidates = {}

        sentences = self.sentence_splitter.tokenize(text)
        for sent in sentences:
            if len(sent) < 3:
                continue
            sent_entities, sent_candidates, sent_no_entities = self.named_entity_extractor(sent)
            entities.extend(sent_entities)
            candidates.extend(sent_candidates)
            for piece in highlighted_strings:

                if len(piece) < 2:
                    continue
                if piece not in sent_candidates and\
                        piece not in sent_entities and\
                        piece in sent:
                    checked_entity = Entity.check(piece, self.db)
                    if checked_entity is not None:
                        sent_entities.append(checked_entity)
                    else:
                        sent_candidates.append(piece)

            scored_text_entities, scored_text_candidates = self.get_sentiment(sent_no_entities,
                                                                              sent_entities, sent_candidates,
                                                                              scored_text_entities, scored_text_candidates)
        title_entities, title_candidates, title_no_entities = self.named_entity_extractor(title)
        scored_entities, scored_candidates = self.get_sentiment(title_no_entities,
                                                                title_entities, title_candidates,
                                                                scored_text_entities, scored_text_candidates)

        suggested_entities = self._suggested_entities(entities,
                                                      title_entities,
                                                      candidates,
                                                      title_candidates)
        return (suggested_entities, scored_entities, scored_candidates)

    def _preprocess_text(self, t_list):
        text = u" . ".join(t_list)
        return text

    def _process_keywords(self, page, scored_entities):
        for keyword in page['metadata']['keywords']:
            checked_kayword = Entity.check(keyword, self.db)
            if checked_kayword is not None:
                scored_entities[keyword] = self._updated_sentiment(checked_kayword,
                                                                   sentiment=0, items_dict=scored_entities, keyword=True)
        return scored_entities

    def extract(self, page):
        return ExtractedPage(doc=self._extract(page), db_connection=self.db)

    def _extract(self, page):
        if len(page.text) == 0:
            raise ExtractionError(page.url, page)
        text = self._preprocess_text(page.text)
        extracted_data = {
            "extractor": self.name,
            "url": page.url,
            "site": urlparse(page.url).hostname,
            "title": page.title,
            "text": text,
            "extracted_at": None,
            "parser": page.parser,
            'suggested_entities': None,
            "entities": [],
            "candidates": [],
            "keywords": page.metadata.get('keywords', [])
        }

        extract = self.extract_entities(
            page.title, text, page.highlighted_strings)
        extracted_data['suggested_entities'], extracted_data['entities'], extracted_data['candidates'] = extract

        if len(extracted_data["keywords"]) > 0:
            extracted_data['entities'] = self._process_keywords(page.page, extracted_data['entities'])

        extracted_data['entities'] = self.wrap_entities_for_db(extracted_data['entities'])
        extracted_data['candidates'] = self.wrap_candidates_for_db(extracted_data['candidates'])

        return extracted_data

    def prepare_site_entity(self, site, entity):
        name = entity.get('name', entity.get('text'))
        entity_in_db = self.db[WEBSITES_ENTITIES_COL].find_one(
            {'name': name, 'site': site})
        entity_record = {'site': site,
                         'name': name,
                         '_name': name.lower(),
                         'count': entity['sentiment']['count'],
                         'sentiment': entity['sentiment']['score']}
        if entity_in_db:
            old_sentiment_sum = entity_in_db['sentiment'] * entity_in_db['count']
            new_sentiment_sum = old_sentiment_sum + entity_record['sentiment'] * entity_record['count']
            new_count = entity_in_db['count'] + entity_record['count']
            new_sentiment_average = new_sentiment_sum / new_count
            entity_record['count'] = new_count
            if entity_record['sentiment'] != 0:
                entity_record['sentiment'] = new_sentiment_average
            else:
                entity_record['sentiment'] = entity_in_db['sentiment']
        return entity_record

    def prepare_site_entitycandidate(self, site, candidate):
        name = candidate.get('name', candidate.get('text'))
        candidate_in_db = self.db[WEBSITES_CANDIDATES_COL].find_one(
            {'name': name, 'site': site})

        candidate_record = {'site': site,
                            'name': name,
                            '_name': name.lower(),
                            'count': candidate['sentiment']['count'],
                            'sentiment': candidate['sentiment']['score']}
        if candidate_in_db:
            old_sentiment_sum = candidate_in_db['sentiment'] * candidate_in_db['count']
            new_sentiment_sum = old_sentiment_sum + candidate_record['sentiment'] * candidate['sentiment']['count']
            new_count = candidate_in_db['count'] + candidate['sentiment']['count']
            new_sentiment_average = new_sentiment_sum / new_count
            candidate_record['count'] = new_count
            candidate_record['sentiment'] = new_sentiment_average

        return candidate_record

    def save_entities(self, extracted_data, keep_candidates=True):
        if extracted_data is None:
            return
        site = extracted_data['site']
        extracted_data['extracted_at'] = datetime.datetime.utcnow()
        bulk = self.db[WEBSITES_ENTITIES_COL].initialize_unordered_bulk_op()
        for entity in extracted_data['entities']:
            if entity.get('name') is not None:
                e = self.prepare_site_entity(site, entity)
                if e is not None:
                    bulk.find({'name': e['name'], 'site': site}).upsert().update({'$set': e})
        try:
            print(bulk.execute())
        except Exception as e:
            print(e)

        if keep_candidates:
            print('Saving candidates')
            bulk = self.db[WEBSITES_CANDIDATES_COL].initialize_unordered_bulk_op()
            for candidate in extracted_data['candidates']:
                if candidate.get('name') is not None:
                    c = self.prepare_site_entitycandidate(site, candidate)
                    if c is not None:
                        bulk.find({'name': c['name'], 'site': site}).upsert().update({'$set': c})
            try:
                bulk.execute()
            except Exception as e:
                print(e)
        print("Bulk execute done ")

    def extract_and_save(self, page, keep_candidates=True):
        print("\nPAGE: ", page)
        extract = self._extract(page)
        assert len(extract['entities']) == 0 or len([e for e in extract['entities'] if 'type' in e['sentiment']]) > 0
        if extract is None:
            return None
        print('\nExtract: ', extract)
        extracted_page = ExtractedPage(doc=extract, db_connection=self.db)
        extracted_page.save()
        self.save_entities(extract, keep_candidates=keep_candidates)
        return extracted_page

    def get_extract(self, url):
        extract = self.db[EXTRACTED_PAGES_COL].find_one({'url': url})
        if extract is None:
            return None
        if extract is not None and len(extract['entities']) > 0:
            if 'text' in extract['entities'][0]:
                self.db[EXTRACTED_PAGES_COL].remove(extract)
                extract = None
        print('EntityExtractor.get_extract :', extract)
        if ExtractedPage.check_fields(extract):
            page = ExtractedPage(doc=extract, db_connection=self.db)
        else:
            page = None
        return page
