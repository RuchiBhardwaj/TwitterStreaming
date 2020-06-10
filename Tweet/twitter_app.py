import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = '1252138135477239808-dq1QTxj6dAd8c7yletPtaHpEqc1UHX'
ACCESS_SECRET = 'mGx3dZ4x35JX8YpDTwQpOXFzsnxidxt5MM6h51Foz9jQg'
CONSUMER_KEY = 'hL3hXKTgAElnMxSXoYyAmQQ5z'
CONSUMER_SECRET = 'ecL8pxpufQqPmqMplDvbMqyrZOyprvur5xsy9pPXAJ4IZ7QqSe'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            words = full_tweet['text'].split(' ')
            tweet = ''
            for i in words:
                if '#' in i:
                    i = "".join(i.split(' '))
                    tweet += i
                    break
            time = full_tweet['created_at']
            location = "".join(full_tweet["user"]["location"].encode("utf-8"))
            if tweet is not '':
                tweet_text = tweet.encode('utf-8') + '&%' + location + '&%' + time
                print("Tweet Text: " + tweet_text)
                print ("------------------------------------------")
                tcp_connection.send(tweet_text + '\n')

        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'),
                  ('track', '#')]  # this location value is San Francisco & NYC
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
