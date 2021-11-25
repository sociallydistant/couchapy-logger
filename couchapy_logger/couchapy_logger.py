from    couchapy import CouchDB, CouchError
from    datetime import datetime
from    json import dumps
from    queue import Queue, Empty
import  threading
from    traceback import print_exc
import  uuid


class Logger():
    """
    Threaded logging to couchdb database
    """
    __allowed_keys = ['config', 'db_conn']
    __SLEEP_TIME_ON_EMPTY_TWEET_QUEUE = 2

    config = None
    temp_dir = None

    def __init__(self, **kwargs):
        for k in kwargs.keys():
            if k in self.__allowed_keys:
                self.__setattr__(k, kwargs.get(k, None))

        self.db_conn = CouchDB(name=self.config['db']['username'],
                               password=self.config['db']['password'],
                               host=self.config['db']['host'],
                               port=self.config['db']['port'],
                               database_kwargs={"db": self.config['db']['name']},
                               auto_connect=True,
                               keep_alive=True,
                               session_timeout=(600 * 0.9))

        self.log_events = Queue()
        self.logging_thread = None

        self.is_exiting = threading.Event()
        self.is_stopped = threading.Event()

        if isinstance(self.db_conn, CouchError) or self.db_conn.session.auth_token is None:
            self.is_stopped.set()
            self.is_exiting.set()
            return

        if self.db_conn.db.exists() is False:
            create_result = self.db_conn.server.create_database(uri_segments={'db': self.config['db']['name']})

            if isinstance(create_result, CouchError):
                self.is_stopped.set()
                self.is_exiting.set()

    def start(self, **kwargs):
        if self.logging_thread is None and self.is_exiting.is_set() is False:
            self.logging_thread = threading.Thread(target=self._process_log_events, daemon=kwargs.get('daemon', False))
            self.logging_thread.start()
        elif self.logging_thread is not None and self.logging_thread.is_alive() is False and self.is_exiting.is_set() is False:
            self.logging_thread = threading.Thread(target=self._process_log_events, daemon=kwargs.get('daemon', False))
            self.logging_thread.start()
        else:
            pass

    def stop(self):
        if self.logging_thread is not None:
            self.is_exiting.set()

    def create(self, **kwargs):
        """
        Adds a logging event to the queue for eventual persistence to the data store.

        Keyword Arg Params:
            record: details of the event to be logged, which represents the 'data' attribute of the log entry.

        See https://github.com/torusoft/p2p/wiki/General:-Schema#logging for structure of data attribute.
        """
        entry = kwargs.get('record', None)

        if entry is not None:
            self.log_events.put(entry)
            self.start()

    def _process_log_events(self):
        try:
            while self.is_exiting.is_set() is False:
                try:
                    log_entry = self.log_events.get(timeout=5)

                    log_record = {
                        '_id': f'log_2_{str(uuid.uuid1()).upper()}',
                        'data': log_entry
                    }

                    # explicitly set the registration timestamp (overwriting any existing value)
                    log_record['data']['registrationTimestamp'] = datetime.utcnow().isoformat()

                    save_result = self.db_conn.db.save(data=log_record)

                    if isinstance(save_result, CouchError):
                        if self.db_conn.session.auth_token == "" or self.db_conn.session.auth_token is None:
                            auth_result = self.db_conn.session.authenticate(data={'name': self.config['db']['username'], 'password': self.config['db']['password']})

                            if isinstance(auth_result, CouchError) is False and 'name' in auth_result and auth_result['name'] is not None:
                                self.create(record=log_record)
                            else:
                                pass
                        else:
                            self.create(record=log_record)
                    else:
                        pass

                    self.log_events.task_done()
                except Empty:
                    # An empty queue is fine, no-op and wait again for an entry
                    self.stop()
                    pass
                except Exception:
                    print_exc()

            if self.log_events.empty() is False:
                self.log_events.put(None)
                log_events = []
                while True:
                    log_entry = self.log_events.get(timeout=5)

                    if log_entry is None:
                        break

                    log_record = {
                        '_id': f'log_2_{str(uuid.uuid4()).upper()}',
                        'data': log_entry
                    }

                    log_record['data']['registrationTimestamp'] = datetime.utcnow().isoformat()
                    log_events.append(log_record)
                    self.log_events.task_done()

                with open("../uncommited_logs", "w") as outfile:
                    outfile.write(dumps(log_events))

            self.is_stopped.set()
            self.is_exiting.clear()

        except Exception:
            print_exc()
