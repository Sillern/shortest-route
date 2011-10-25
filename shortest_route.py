import urllib
def load_remote_file( url ):
    counter = 0
    while counter < 5:
        try:
            return urllib.urlopen( url ).read()
        except:
            print "unable to access %s, trying again." % ( url )
            counter += 1
            pass

import redis
import json
class GMaps:
    def __init__( self ):
        self.db = redis.Redis( 'localhost' )

    def clear_database( self ):
        self.db.flushall()

    def proper_name( self, name, proper_name = None):
        if proper_name:
            self.db.set( "gmaps:translate:%s" % ( name ), proper_name )
            self.db.set( "gmaps:translate:%s" % ( proper_name ), proper_name )

        else:
            proper_name = self.db.get( "gmaps:translate:%s" % ( name ) )

        return proper_name

    def lookup( self, origin, destination ):
        origin_id = self.location_id( origin )
        destination_id = self.location_id( destination )

        distance = self.db.get( "gmaps:distance:%d:%d" % ( origin_id, destination_id ) )
        duration = self.db.get( "gmaps:duration:%d:%d" % ( origin_id, destination_id ) )

        if not distance or not duration:
            return None, None

        return ( int( distance ), int( duration ) )


    def location_id( self, name ):
        index = self.db.get( "gmaps:location:index" )
        if not index:
            self.db.set( "gmaps:location:index", 0 )
            index = 0

        location_id = self.db.get( "gmaps:location:%s" % ( name ) )
        
        if not location_id:
            location_id = index
            self.db.set( "gmaps:location:%s" % ( name ), location_id )
            self.db.incr( "gmaps:location:index" )

        return int( location_id )


    def store( self, origin, destination, distance, duration ):
        origin_id = self.location_id( origin )
        destination_id = self.location_id( destination )

        self.db.set( "gmaps:distance:%d:%d" % ( origin_id, destination_id ), distance )
        self.db.set( "gmaps:duration:%d:%d" % ( origin_id, destination_id ), duration )

        print "stored %s -> %s [%d]" % ( origin, destination, distance )


    def query( self, origin, destination ):
        origin_lookup = self.proper_name( origin )
        destination_lookup = self.proper_name( destination )

        if origin_lookup and destination_lookup:
            origin = origin_lookup
            destination = destination_lookup

            distance, duration = self.lookup( origin, destination )
            if distance and duration:
                return distance, duration

        query_url = 'http://maps.googleapis.com/maps/api/distancematrix/json?origins=%s&destinations=%s&mode=car&language=en-US&sensor=false' % ( origin, destination )
        response = load_remote_file( query_url )
        
        data = json.loads( response )
        if data[ "status" ] == "OK":

            for address in data[ "origin_addresses" ]:
                origin = self.proper_name( origin, address )
            for address in data[ "destination_addresses" ]:
                destination = self.proper_name( destination, address )

            shortest_distance = 0
            distance, duration = ( 0, 0 )

            for row in data[ 'rows' ]:
                for element in row[ 'elements' ]:
                    if element[ 'status' ] == 'OK':
                        if element[ 'distance' ][ 'value' ] < shortest_distance or shortest_distance == 0:
                            distance = element[ 'distance' ][ 'value' ]
                            duration = element[ 'duration' ][ 'value' ]
                            shortest_distance = distance

            if shortest_distance > 0:
                self.store( origin, destination, distance, duration )
                return ( distance, duration )

import itertools

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee( iterable )
    next( b, None )
    return list( itertools.izip( a, b ) )

from Queue import Queue
from threading import Thread

class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.result = []
        self.daemon = True
        self.start()
    
    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try: 
                self.result.append( func(*args, **kargs) )
            except Exception, e: 
                print e
            self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        self.workers = []
        for _ in range(num_threads): 
            self.workers.append( Worker(self.tasks) )

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()
        result = []
        for worker in self.workers:
            result.extend( worker.result )
        return result


def compute_routes( gmaps, workload ):
    shortest_distance = 0
    shortest_route = []
    
    for places in workload:
        route = list( places )
        route.append( places[0] )

        distance = 0
        for origin, destination in pairwise( route ):
            distance += gmaps.query( origin, destination )[ 0 ]

        if distance < shortest_distance or shortest_distance == 0:
            shortest_route = route
            shortest_distance = distance

    return shortest_route, distance


import sys
def report_progress( index = 0, length = 0 ):
    if index == 0 or length == 0:
        sys.stdout.write( "\r\n" )
        sys.stdout.flush()
        return

    progress = "Progress: %.2f%c" % ( ( float( index )/float( length ) )*100.0, "%" )
    sys.stdout.write( "\r%s" % progress )
    sys.stdout.flush()

def shortest_route( cities ):
    gmaps = GMaps()
    #gmaps.clear_database()

    routes = list( itertools.permutations( cities, len( cities ) ) )

    workload_size = 100
    workloads = zip( *[iter( routes )] * workload_size )
    print "Each thread will go through %d routes" % ( workload_size )

    number_of_threads = 4

    pool = ThreadPool( number_of_threads )
    
    for index, workload in enumerate( workloads ):
        report_progress( index, len( workloads ) )

        pool.add_task( compute_routes, gmaps, workload)
    report_progress()
    
    results = pool.wait_completion()

    shortest_distance = 0
    shortest_route = []
    for route, distance in results:
        if distance < shortest_distance or shortest_distance == 0:
            shortest_route = route
            shortest_distance = distance

    return ( shortest_route, shortest_distance )
        

cities = [
        "arboga", 
        "karlskrona", 
        "vadstena", 
        "goteborg",
        "ornskoldsvik",
        "smygehuk",
        "karlstad",
        "vasteras",
        "falun",
        ]

route, distance = shortest_route( cities )
print route, "%d mil" % ( distance / 10000 )
