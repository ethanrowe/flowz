
class AbstractChannelAccessor(object):
    channel = None

    def get(self):
        if self.channel:
            return self.get_existing()
        return self.get_new()


    def get_existing(self):
        return self.channel.tee()


    def get_new(self):
        raise NotImplementedError("get_new() needs to be defined.")



class ChannelAccessor(AbstractChannelAccessor):
    def __init__(self, buildfunc):
        self.builder = buildfunc


    def get_new(self):
        self.channel = self.builder()
        return self.channel


class ChannelManager(object):
    def __init__(self):
        self.channels = {}

    def add_builder(self, name, buildfunc):
        bld = ChannelAccessor(buildfunc)
        return self.add_accessor(name, bld)

    def add_accessor(self, name, accessor):
        self.channels[name] = accessor
        return self

    def add_channel(self, name, channel):
        self.add_builder(name, lambda: channel)
        return self

    def __getitem__(self, key):
        try:
            accessor = self.channels[key]
            return accessor.get()
        except KeyError:
            raise KeyError("No such key %s" % repr(key))

