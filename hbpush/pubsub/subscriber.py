from tornado.web import asynchronous, HTTPError
from hbpush.pubsub import PubSubHandler
from hbpush.channel import Channel

from email.utils import formatdate, parsedate_tz, mktime_tz
from functools import partial
import logging
import calendar

# mktime_tz has some problems on Windows (http://bugs.python.org/issue14653),
# so we are converting manually
def convert_timestamp(timestamp):
    t = parsedate_tz(timestamp)
    if t[9] is None:
        return mktime_tz(t)
    else:
        g = calendar.timegm(t[:9])
        return g - t[9]

class Subscriber(PubSubHandler):
    def __init__(self, *args, **kwargs):
        self.create_on_get = kwargs.pop('create_on_get', False)
        self.passthrough = kwargs.pop('passthrough', None)
        super(Subscriber, self).__init__(*args, **kwargs)

    @asynchronous
    def get(self, channel_id):
        try:
            etag = int(self.request.headers.get('If-None-Match', -1))
            last_modified = int('If-Modified-Since' in self.request.headers and convert_timestamp(self.request.headers['If-Modified-Since']) or 0)
        except Exception, e:
            logging.warning('Error parsing request headers: %s', e)
            raise HTTPError(400)

        getattr(self.registry, 'get_or_create' if self.create_on_get else 'get')(channel_id,
            callback=self.async_callback(partial(self._process_channel, last_modified, etag)),
            errback=self.errback)

    def options(self, channel_id):
        self.add_accesscontrol_headers()

    def _process_message(self, message):
        self.set_header('Etag', message.etag)
        # Chrome and other WebKit-based browsers do not (yet) support Access-Control-Expose-Headers,
        # but they allow access to Cache-Control so we use it to additionally store etag information there
        # (This field is by standard extendable with custom tokens)
        self.set_header('Cache-Control', '%s=%s' % ('etag', message.etag))
        self.set_header('Last-Modified', formatdate(message.last_modified, localtime=False, usegmt=True))
        self.add_vary_header()
        self.add_accesscontrol_headers()
        self.set_header('Content-Type', message.content_type)
        self.write(message.body)
        self.finish()
        
    def _process_channel(self, last_modified, etag, channel):
        channel.get(last_modified, etag,
            callback=self.async_callback(self._process_message),
            errback=self.errback)


class LongPollingSubscriber(Subscriber):
    def unsubscribe(self):
        if hasattr(self, 'channel'):
            self.channel.unsubscribe(id(self), self.request, self.passthrough)
    on_connection_close = unsubscribe

    def finish(self, chunk=None):
        self.unsubscribe()
        super(LongPollingSubscriber, self).finish(chunk)

    def _process_channel(self, last_modified, etag, channel):
        @self.async_callback
        def _wait_for_message(error):
            if error.__class__ == Channel.NotModified:
                self.channel.wait_for(last_modified, etag, self.request, self.passthrough, id(self), callback=self.async_callback(self._process_message), errback=self.errback)
            else:
                self.errback(error)
        
        self.channel = channel
        self.channel.get(last_modified, etag,
            callback=self.async_callback(self._process_message),
            errback=_wait_for_message)
