from flowz.app import Flo
from flowz.channels import *

@gen.coroutine
def starter(chan):
    print("Starting channel", chan)
    for i in range(5):
        print("Producer sleeping.")
        yield gen.sleep(0.1)
        print("Putting value", i)
        yield chan.put(i)
    print("Done writing to channel.")
    chan.close()


def reader(app, chan, name):
    def _reader(val):
        print("Reader", name, "received:", val)
        # Uncomment for proof that the app stops when a channel fails.
        #if len(name) > 3:
        #    raise Exception("Boom! from %s" % str(name))
        c = MapChannel(TeeChannel(chan), lambda v: v * -1)
        c = FlatMapChannel(c, reader(app, c, name + (val,)))
        app.add_targets([c])
        return ()
    return _reader


import random
@gen.coroutine
def sleeper(i):
    yield gen.sleep(random.random() * 3)
    raise gen.Return(i)

#channel = IterChannel(sleeper(x) for x in range(5))
channel = ProducerChannel(starter)
channel = ReadyFutureChannel(channel)

chans = (
        ReadChannel(TeeChannel(channel))
        for n in ('a', 'b', 'c')
        )

app = Flo([])

chans = [
    FlatMapChannel(c, reader(app, c, (n,)))
    for n, c in zip(('a', 'b', 'c'), chans)]

app.add_targets(chans)
print("Starting app.")
app.run()
print("App done.")
