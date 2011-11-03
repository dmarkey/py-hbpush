from hbpush.store import Store
from hbpush.message import Message

from bisect import bisect
import time


class MemoryStore(Store):
    def __init__(self, *args, **kwargs):
        super(MemoryStore, self).__init__(*args, **kwargs)
        self.min_messages = kwargs.pop('min_messages', 0)
        self.max_messages = kwargs.pop('max_messages', 0)
        self.message_timeout = kwargs.pop('message_timeout', 0)
        self.messages = {}
        self.expired_channels = {}

    def _expire_messages(self, channel_id):
        channel_messages = self.messages.setdefault(channel_id, [])

        if not channel_messages:
            if self.expired_channels.get(channel_id, False):
                raise Message.Expired()
            else:
                return channel_messages

        if self.max_messages and len(channel_messages) > self.max_messages:
            channel_messages = channel_messages[-self.max_messages:]

        if self.message_timeout:
            while channel_messages and len(channel_messages) > self.min_messages:
                if channel_messages[0].last_modified + self.message_timeout >= int(time.time()):
                    break
                channel_messages = channel_messages[1:]

        self.messages[channel_id] = channel_messages

        if not self.messages[channel_id]:
            self.expired_channels[channel_id] = True
            raise Message.Expired()
        else:
            return channel_messages

    def get(self, channel_id, last_modified, etag, callback, errback):
        channel_messages = self._expire_messages(channel_id)

        msg = Message(last_modified, etag)
        try:
            callback(channel_messages[bisect(channel_messages, msg)])
        except IndexError:
            errback(Message.DoesNotExist())

    def get_last(self, channel_id, callback, errback):
        channel_messages = self._expire_messages(channel_id)

        if channel_messages:
            callback(channel_messages[-1])
        else:
            errback(Message.DoesNotExist())

    def post(self, channel_id, message, callback, errback):
        self.messages.setdefault(channel_id, []).append(message)
        self.expired_channels[channel_id] = False
        callback(message)

    def flush(self, channel_id, callback, errback):
        del self.messages[channel_id]
        del self.expired_channels[channel_id]
        callback(True)

    def flushall(self, callback, errback):
        self.messages = {}
        self.expired_channels = {}
        callback(True)
