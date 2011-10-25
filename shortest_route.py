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


    def proper_name( self, name, proper_name = None):
        if proper_name:
            self.db.set( "translate:%s" % ( name ), proper_name )
            self.db.set( "translate:%s" % ( proper_name ), proper_name )

        else:
            proper_name = self.db.get( "translate:%s" % ( name ) )

        return proper_name

    def lookup( self, origin, destination ):
        origin = self.proper_name( origin )
        destination = self.proper_name( destination )

        if not origin or not destination:
            return None, None

        origin_id = self.location_id( origin )
        destination_id = self.location_id( destination )

        distance = self.db.get( "distance:%d:%d" % ( origin_id, destination_id ) )
        duration = self.db.get( "duration:%d:%d" % ( origin_id, destination_id ) )

        if not distance or not duration:
            return None, None

        return ( int( distance ), int( duration ) )


    def location_id( self, name ):
        index = self.db.get( "location:index" )
        if not index:
            self.db.set( "location:index", 0 )
            index = 0

        location_id = self.db.get( "location:%s" % ( name ) )
        
        if not location_id:
            location_id = index
            self.db.set( "location:%s" % ( name ), location_id )
            self.db.incr( "location:index" )

        return int( location_id )


    def store( self, origin, destination, distance, duration ):
        origin_id = self.location_id( self.proper_name( origin ) )
        destination_id = self.location_id( self.proper_name( destination ) )

        self.db.set( "distance:%d:%d" % ( origin_id, destination_id ), distance )
        self.db.set( "duration:%d:%d" % ( origin_id, destination_id ), duration )


    def query( self, origin, destination ):
        
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

def shortest_route( cities ):
    gmaps = GMaps()

    shortest_distance = 0
    shortest_route = []
    for places in list( itertools.permutations( cities, len( cities ) ) ):
        route = list( places )
        route.append( places[0] )

        distance = 0
        for origin, destination in pairwise( route ):
            distance += gmaps.query( origin, destination )[ 0 ]

        if distance < shortest_distance or shortest_distance == 0:
            shortest_route = route
            shortest_distance = distance

    return shortest_route, shortest_distance

cities = [
        "arboga", 
        "karlskrona", 
        "vadstena", 
        "goteborg"
        ]
print shortest_route( cities )
