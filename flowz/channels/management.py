
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

