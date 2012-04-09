from tornado import httpclient
from hbpush.message import Message

import logging
import time
import urllib


class Channel(object):

    class DoesNotExist(Exception):
        pass
    class Duplicate(Exception):
        pass
    class Gone(Exception):
        pass
    class NotModified(Exception):
        pass

    def __init__(self, id, store):
        self.store = store
        self.id = id
        self.sentinel = None
        self.subscribers = {}

        # Empty message, we just want to keep etag and lastmodified data
        self.last_message = Message(0, -1)

        self.client = httpclient.AsyncHTTPClient()

    def send_to_subscribers(self, message):
        # We work on a copy to deal with reentering subscribers
        subs = self.subscribers.copy()
        self.subscribers = {}
        nb = 0
        for (id_subscriber, (cb, eb)) in subs.items():
            cb(message)
            nb += 1
        return nb

    def post(self, content_type, body, callback, errback):
        def _process_message(message):
            nb_subscribers = self.send_to_subscribers(message)
            # This piece assumes we will always get to that callback in the order
            # we posted messages
            assert self.last_message < message
            self.last_message = Message(message.last_modified, message.etag)
            # Give back control to the handler with the result of the store
            callback((message, nb_subscribers))
            
        message = self.make_message(content_type, body)
        self.store.post(self.id, message, callback=_process_message, errback=errback)

    def wait_for(self, last_modified, etag, request, passthrough, id_subscriber, callback, errback):
        request_msg = Message(last_modified, etag)

        def _cb(message):
            if request_msg >= message:
                self.subscribe(id_subscriber, request, passthrough, _cb, errback)
            else:
                callback(message)

        self.subscribe(id_subscriber, request, passthrough, _cb, errback)

    def _passthrough(self, action, request, passthrough):
        if not passthrough or request.method != 'GET':
            return

        def ignore(response):
            pass

        url = passthrough
        body = urllib.urlencode({'channel_id': self.id, action: 1})
        self.client.fetch(url, ignore, method='POST', body=body, headers=request.headers)

    def subscribe(self, id_subscriber, request, passthrough, callback, errback):
        self._passthrough('subscribe', request, passthrough)
        self.subscribers[id_subscriber] = (callback, errback)

    def unsubscribe(self, id_subscriber, request, passthrough):
        self._passthrough('unsubscribe', request, passthrough)
        self.subscribers.pop(id_subscriber, None)

    def get(self, last_modified, etag, callback, errback):
        request_msg = Message(last_modified, etag)

        if request_msg < self.last_message:
            try:
                self.store.get(self.id, last_modified, etag, callback=callback, errback=errback)
                return
            except Message.Expired:
                pass
        
        errback(Channel.NotModified())

    def delete(self, callback, errback):
        for id, (cb, eb) in self.subscribers.items():
            eb(Channel.Gone())
        # Just for the record
        self.subscribers = {}

        # Delete all messages from the store
        self.store.flush(self.id, callback, errback)

    def make_message(self, content_type, body):
        if not self.sentinel:
            self.sentinel = self.last_message

        last_modified = int(time.time())
        if last_modified == self.sentinel.last_modified:
            etag = self.sentinel.etag+1
        else:
            etag = 0

        self.sentinel = Message(last_modified, etag, content_type, body)
        return self.sentinel


