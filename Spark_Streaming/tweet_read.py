import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


# Set up your credentials
consumer_key='jRfp692CC4UH2zQQQlxPPaDNR'
consumer_secret='1TdvAOAayFmYwJajkgMmLkCprlCwPj1FG3RlTP7gMOG4WKgZCl'
access_token ='2348335033-fYwmXBXAnqAQFe6qka6KPzT8uqsEwX3DXGnGrj6'
access_secret='VZhLxgWc0yC35cAZrAo4hT9KjvwOnVJ6yv4M6pefJmBT3'


class TweetsListener(StreamListener):

  def __init__(self, csocket):
    self.client_socket = csocket

  def on_data(self, data):
    try:
      msg = json.loads( data )
      print( msg['text'].encode('utf-8') )
      self.client_socket.send( msg['text'].encode('utf-8') )
      return True
    except BaseException as e:
      print("Error on_data: %s" % str(e))
    return True

  def on_error(self, status):
    print(status)
    return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['guitar'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "localhost"          # Get local machine name
  port = 9009                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
  