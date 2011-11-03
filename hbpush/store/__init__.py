class Store(object):
    def __init__(self, *args, **kwargs):
        pass

    def get(self, channel_id, last_modified, etag, callback, errback):
        raise NotImplementedError("")

    def get_last(self, channel_id, callback, errback):
        raise NotImplementedError("")
        
    def post(self, channel_id, message, callback, errback):
        raise NotImplementedError("")

    def flush(self, channel_id, callback, errback):
        raise NotImplementedError("")

    def flushall(self, callback, errback):
        raise NotImplementedError("")
