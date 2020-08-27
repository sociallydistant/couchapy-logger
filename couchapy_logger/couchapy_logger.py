from    couchapy import CouchDB, CouchError
from    datetime import  datetime
from    queue import Queue, Empty
import  threading
import  signal
import  uuid

from pprint import pprint


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

        if isinstance(self.db_conn, CouchError) or self.db_conn.session.auth_token is None:
            human_readable_message = "The logging database connection failed."
            print(human_readable_message)
            exit()

        if self.db_conn.db.exists() is False:
            print(f"Logging database does not exist, attempting to create it now using name '{self.config['db']['name']}'")
            create_result = self.db_conn.server.create_database(uri_segments={'db': self.config['db']['name']})

            if isinstance(create_result, CouchError):
                print(f"An attempt to create the logging database failed.  Reason: {create_result.reason}")
                exit()

        self.is_exiting = threading.Event()
        self.is_stopped = threading.Event()
        self._parent_sigint_handler = signal.signal(signal.SIGINT, self._keyboard_interrupt_handler)

    def _keyboard_interrupt_handler(self, signum, frame):
        self.stop()
        print("Logging stop signal issued...waiting for graceful exit.")
        self.is_stopped.wait()

    def start(self):
        if self.logging_thread is None and self.is_exiting.is_set() is False:
            print("Starting logging thread...")
            self.logging_thread = threading.Thread(target=self._process_log_events, daemon=False)
            self.logging_thread.start()
        print("Logging thread has started.")

    def stop(self):
        if self.logging_thread is not None:
            print("\nSignalling logging thread to exit...")
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

    def _process_log_events(self):
        try:
            while self.is_exiting.is_set() is False:
                try:
                    log_entry = self.log_events.get(timeout=5)

                    log_record = {
                        '_id': f'log_2_{str(uuid.uuid4()).upper()}',
                        'data': log_entry
                    }

                    # explicitly set the registration timestamp (overwriting any existing value)
                    log_record['data']['registrationTimestamp'] = datetime.utcnow().isoformat()

                    save_result = self.db_conn.db.save(data=log_record)

                    if isinstance(save_result, CouchError):
                        self.create(record=log_record)

                    print(save_result)
                    self.log_events.task_done()
                except Empty:
                    # An empty queue is fine, no-op and wait again for an entry
                    pass
                except Exception as e:
                    print(str(e))

            print("Signal to abort logging received; logging thread is exiting...")

            print("Dumping uncommited log events to disk.")
            print("TODO: DUMP ANY QUEUE CONTENTS TO DISK BEFORE EXITING...")
            with open("../uncommited_logs", "w") as outfile:
                outfile.write("dddd")

            self.is_stopped.set()
        except Exception as e:
            print(str(e))
