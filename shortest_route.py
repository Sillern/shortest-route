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

import threading

class compute_distance ( threading.Thread ):

    def __init__( self, gmaps, workload ):
        self.gmaps = gmaps
        self.workload = workload

        self.shortest_distance = 0
        self.shortest_route = []
        threading.Thread.__init__ ( self )

    def run ( self ):
        for places in self.workload:
            route = list( places )
            route.append( places[0] )

            distance = 0
            for origin, destination in pairwise( route ):
                distance += self.gmaps.query( origin, destination )[ 0 ]

            if distance < self.shortest_distance or self.shortest_distance == 0:
                self.shortest_route = route
                self.shortest_distance = distance


def shortest_route( cities ):
    gmaps = GMaps()
    #gmaps.clear_database()

    shortest_distance = 0
    shortest_route = []
    routes = list( itertools.permutations( cities, len( cities ) ) )

    number_of_threads = 20
    workload_size = len( routes ) / number_of_threads
    workloads = zip( *[iter( routes )] * workload_size )

    threads = []
    for workload in workloads:
        thread = compute_distance( gmaps, workload )
        thread.start()
        threads.append( thread )

    for thread in threads:
        thread.join()
        if thread.shortest_distance < shortest_distance or shortest_distance == 0:
            shortest_route = thread.shortest_route
            shortest_distance = thread.shortest_distance

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
