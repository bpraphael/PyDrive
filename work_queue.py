import time
import threading
import traceback

def __debug(str):
    print(threading.current_thread().name + ': ' + str)

def _debug_w(str):
    #__debug(str)
    pass

def _debug_locks(str):
    #__debug(str)
    pass


"""
Main class. Dispatcher for queued tasks.
"""
class Dispatcher:
    def __init__(self, num_workers, queue_size, task_func):
        self.queue = Queue(queue_size)
        self.num_workers = num_workers
        self.workers = [ None ] * num_workers
        for i in range(num_workers):
            self.workers[i] = Worker(i, self.queue, task_func)
    
    def start(self):
        for w in self.workers:
            w.start()
    
    def stop(self):
        for w in self.workers:
            w.stop(False)
        self.queue.release_all()
        for w in self.workers:
            w.stop(True)
    
    def add_data(self, data):
        return self.queue.add_data(data)
    
    def has_data(self):
        return self.queue.has_data()
    
    def clear_data(self):
        return self.queue.clear()
    
    def is_full(self):
        return self.queue.is_full()
    
    def is_busy(self):
        for w in self.workers:
            if w.is_busy():
                return True
        return False


"""
Inner class. Threaded worker/consumer.
"""
class Worker:
    def __init__(self, id, queue, task):
        self.id = id
        self.queue = queue
        self.task = task
        self.thread = None
        self.run = False
        self.busy = False
    
    def start(self):
        self.thread = threading.Thread(target=Worker._thread_entry, args=(self,), name=str(self.id))
        self.run = True
        self.thread.start()
    
    def stop(self, wait=True):
        self.run = False
        if wait:
            self.thread.join()

    def is_busy(self):
        return self.busy

    def current_thread_id():
        try:
            return int(threading.current_thread().name)
        except:
            return -1

    def _thread_entry(self):
        _debug_w('thread started')
        while self.run:
            try:
                data = self.queue.get_data(1000)
                if data != None:
                    _debug_w('thread is busy')
                    self.busy = True
                    self.task(data)
                else:
                    _debug_w('thread is idle')
                    self.busy = False
            except BaseException as e:
                print(traceback.format_exc())
        _debug_w('thread ended')


"""
Inner class. Thread-safe fixed length circular queue for task distribution
between workers.
"""
class Queue:
    def __init__(self, size):
        self.size = size
        self.queue = [ None ] * size
        self.head = 0
        self.tail = 0
        self.general_lock = threading.Lock()
        self.empty_lock = threading.Lock()
        self.empty_lock.acquire(blocking=False) # starts empty
        _debug_locks('##EMPTY LOCK ACQUIRED##$INIT')

    def add_data(self, data):
        with self.general_lock:
            if not self.is_full():
                was_empty = not self.has_data()
                self.queue[self.tail] = data
                self.tail = (self.tail + 1) % self.size
                if self.tail == self.head:
                    self.tail = Queue.FULL
                if was_empty:
                    _debug_locks('##EMPTY LOCK RELEASED##$ADD')
                    self.empty_lock.release()
                return True
            else: # full queue
                return False
    
    def get_data(self, timeout):
        if self.empty_lock.acquire(timeout=timeout/1000): # proceeds if/when not empty
            _debug_locks('##EMPTY LOCK ACQUIRED##$GET')
            try:
                with self.general_lock:
                    if self.has_data():
                        if self.is_full():
                            self.tail = self.head
                        data = self.queue[self.head]
                        self.queue[self.head] = None
                        self.head = (self.head + 1) % self.size
                        if self.has_data(): # didn't become empty
                            _debug_locks('##EMPTY LOCK RELEASED##$GET/1')
                            self.empty_lock.release()
                        return data
                    else: # empty queue
                        _debug_locks('##EMPTY LOCK RELEASED##$GET/2')
                        self.empty_lock.release()
            except:
                _debug_locks('##EMPTY LOCK RELEASED##$GET/EXCEPTION')
                self.empty_lock.release()
        return None
    
    def clear(self):
        with self.general_lock:
            self.head = self.tail
    
    def release_all(self):
        try:
            self.general_lock.acquire(timeout=3000)
            while self.empty_lock.locked():
                self.empty_lock.release()
            while self.general_lock.locked():
                self.general_lock.release()
        except:
            pass
    
    FULL = -1

    def has_data(self):
        return self.tail != self.head

    def is_full(self):
        return self.tail == Queue.FULL
