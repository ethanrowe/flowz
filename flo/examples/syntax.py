from __future__ import print_function
from flowz import app
from flowz import channels


# 0 to 9.
numbers = channels.IterChannel(range(10))

# We'll have multiple readers of numbers, so tee it each time or they
# won't see all the values.
doubles = numbers.tee().map(lambda val: (val, val * 2))
triples = numbers.tee().map(lambda val: (val, val * 3))

# Use flat_map to filter things out.
even_triples = triples.tee().flat_map(
        lambda val: (x for x in (val,) if x[1] % 2 == 0))

odd_triples = triples.tee().flat_map(
        lambda val: (x for x in (val,) if x[1] % 2 == 1))

# Now we'll cogroup them as an interesting representation.
together = doubles.cogroup(triples, even_triples, odd_triples)

# And convert each to a dictionary
dicts = together.map(lambda (dbls, trpls, eventrp, oddtrp): {
    'key': dbls[0], 'double': dbls[1], 'triple': trpls[1],
    'last_even_triple': None if eventrp is None else eventrp[1],
    'last_odd_triple': None if oddtrp is None else oddtrp[1]})

# print it and run.
app.Flo([dicts.map(print)]).run()

