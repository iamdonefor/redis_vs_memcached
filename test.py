#!/usr/bin/python
# -*- coding: utf-8 -*-

import redis, memcache
import os
from multiprocessing import Process, Array, active_children
from threading import Thread
from datetime import datetime
from time import sleep
from random import randint

class Redis:
    def __init__(self):
        self.r = redis.StrictRedis(host="localhost")
    def get(self, key):
        return self.r.get(key)
    def set(self, key, value, lifetime):
        self.r.set(key, value, ex=lifetime)
    def flushall(self):
        self.r.flushall()

class Memcached:
    def __init__(self):
        self.m = memcache.Client(['127.0.0.1:11211'], debug=0)
    def get(self, key):
        return self.m.get(key)
    def set(self, key, value, lifetime):
        self.m.set(key, value, time=lifetime)
    def flushall(self):
        self.m.flush_all()

def create_db(db_type):
    if db_type == "redis":
        db = Redis()
    elif db_type == "memcached":
        db = Memcached()
    else:
        raise NotImplemented

    return db

def do_actually_test(db, stats):
    CYCLES = 25000
    MIN_SIZE=20000
    MAX_SIZE=600000
    MAX_OBJECTS=512
    LIFETIME=2

    stream = open("/dev/zero")

    def get_string(id):
        size = randint(MIN_SIZE,MAX_SIZE)
        s = stream.read(size)
        return str(s)

    for i in range(CYCLES):
        id = str(randint(1,MAX_OBJECTS))
        x = db.get(id)
        if not x:
            s = get_string(id)
            db.set(id, s, LIFETIME)
            stats[1] += 1
        else:
            stats[0] += 1

    return

def process(db_type, stats):
    db = create_db(db_type)

    start = datetime.now()
    do_actually_test(db, stats)
    delta = datetime.now() - start

    stats[2] += float((delta.seconds * 100) + (delta.microseconds / 10000))

    return
    
def run_processes(n, db_type):
    a = Array('f', 3)

    for i in range(n):
       p = Process(target=process, args=(db_type, a))
       p.start()

    while active_children():
        sleep(1)

    return a

def run_threaded(n, db_type):
    stats = [0.0, 0.0, 0,0]
    threads = []

    for i in range(n):
        t = Thread(target=process, args=(db_type, stats))
        t.start()
        threads.append(t)

    while True:
        for t in threads[:]:
            if not t.is_alive():
                t.join()
                threads.remove(t)
        if not threads:
            break
        else:
            sleep(1)

    return stats
        
def run_single(db_type):
    from array import array
    stats = [0.0, 0.0, 0,0]

    process(db_type, stats)

    return stats

def run_tests(method, db_type):
    NWORKERS = 16

    print "Running", method, "test using", db_type

    create_db(db_type).flushall()

    if method == "single":
        stats = run_single(db_type)
    elif method == "processes":
        stats = run_processes(NWORKERS, db_type)
    elif method == "threaded":
        stats = run_threaded(NWORKERS, db_type)
    else:
        raise NotImplemented

    print "Statistics: reads:", stats[0], "writes:", stats[1], "cache hit:", (stats[0]/(stats[0]+stats[1])), "time taken:", stats[2]/100

if __name__ == '__main__':
    for method in ["single", "processes", "threaded"]:
        for db_type in ["redis", "memcached"]:
            run_tests(method, db_type)
    
