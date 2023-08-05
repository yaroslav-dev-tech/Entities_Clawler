import tornado.ioloop
import tornado.gen
import time
from datetime import datetime
import logging

LOG_FORMAT = '%(asctime)-15s %(levelname)-10s %(module)s:%(lineno)s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class AsyncService(object):
    ''' Service class for async services '''

    name = ''
    mongodb_col = None
    es_index = None
    queue_prefix = 'work_queue:'
    stop_flag = False

    def __init__(self, db, redis,
                 es=None,
                 log_level=logging.DEBUG,
                 wait_for=1,
                 transactions_limit=950,
                 concurrent_requests_limit=2,
                 sentry=None):
        self.db = db
        self.es = es
        self.redis = redis
        self.wait_for = wait_for
        self.transactions_limit = transactions_limit
        self.concurrent_requests_limit = concurrent_requests_limit
        self.concurrent_requests = 0

        self.logger = logging.getLogger('services')
        self.logger.setLevel(log_level)
        self.sentry = sentry

    def run(self):
        interval_ms = 1000 * self.wait_for
        main_loop = tornado.ioloop.IOLoop.instance()

        runner = tornado.ioloop.PeriodicCallback(
            self._run, interval_ms, io_loop=main_loop)
        runner.start()

        hourly_tasks = tornado.ioloop.PeriodicCallback(
            self.hourly, 1000 * 60 * 60, io_loop=main_loop)
        hourly_tasks.start()

        minute_tasks = tornado.ioloop.PeriodicCallback(
            self.every_minute, 1000 * 60, io_loop=main_loop)
        minute_tasks.start()

        main_loop.start()

    def get_item(self):
        self.logger.debug("AsyncService.get_item")
        item = self.redis.lpop(self.queue_prefix + self.name)
        return item

    def put_item_back(self, item):
        self.redis.rpush(self.queue_prefix + self.name, item)

    @tornado.gen.coroutine
    def _run(self):
        if self.concurrent_requests >= self.concurrent_requests_limit:

            return
        while True:
            item = self.get_item()
            print("ITEM: ", item)
            if item:
                print("AsyncService Got item: ", item)
                transactions_today = self.redis.get('transactions_today:%s' % self.name)
                if not transactions_today:
                    transactions_today = 0
                else:
                    transactions_today = int(transactions_today)
                try:
                    if transactions_today > self.transactions_limit:
                        self.put_item_back(item)
                        self.logger.info('[INTERRUPTED] daily limit exceeded')
                        return
                    self.concurrent_requests += 1
                    response = yield tornado.gen.Task(self.process, item)
                    self.concurrent_requests -= 1
                    assert isinstance(response, dict)
                    if response['status'] == 'OK':
                        transactions_today += int(response['totalTransactions'])
                        self.redis.set('transactions_today:%s' % self.name, transactions_today)
                        yield tornado.gen.Task(self.save_data, response)
                except Exception as  e:
                    import traceback
                    traceback.print_exc()
                    if self.sentry:
                        self.sentry.captureException()
            else:
                break

    @tornado.gen.coroutine
    def every_minute(self):
        self._every_minute()

    @tornado.gen.coroutine
    def hourly(self):
        if datetime.utcnow().hour == 0:
            print(self.redis.set('transactions_today:%s' % self.name, 0))

    def process(self, item, callback):
        ''' _process should return doc:
                        {'status':'OK',
                         'totalTransactions':0,
                         'doc': {}}'''
        resp = self._process(item)
        assert isinstance(resp, dict)
        assert resp['status'] in ['OK', 'Exception']
        assert isinstance(resp['totalTransactions'], int)
        return callback(resp)

    @tornado.gen.coroutine
    def save_data(self, resp):
        assert isinstance(resp, dict)
        assert isinstance(resp['doc'], dict)

        doc = resp['doc']

        self._save_data(doc)


class SyncService(object):
    ''' Simple loop service '''

    name = ''

    def __init__(self,
                 mongodb=None,
                 es=None,
                 redis=None,
                 wait_for=60,
                 log_level=logging.DEBUG,
                 sentry=None):
        self.mongodb = mongodb
        self.es = es
        self.redis = redis

        self.wait_for = wait_for
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(log_level)
        self.sentry = sentry

    def main_loop(self):
        pass

    def run(self):
        while True:
            try:
                self.main_loop()
            except Exception as inst:
                self.logger.error("Service:%s Exception in main loop: %s" %
                                  (self.name, inst))
                if self.sentry:
                    self.sentry.captureException()
                time.sleep(self.wait_for * 10)
            time.sleep(self.wait_for)
