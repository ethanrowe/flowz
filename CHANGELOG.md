# Version 0.3.0

* Windowing and grouping: WindowChannel, GroupChannel, windowby(), groupby()
* Chaining: ChainChannel, chain()
* Observing channels for side-effects: ObserveChannel, observe()
* Improved exception handling within channels, to ensure traceback is properly captures.
  Note that this is python-2 oriented, and will need to be revisited for python 3.
* Added some channel utility functions: channel_join, merge_keyed_channels, incremental_assembly
* Some additional unit testing improvements

# Version 0.2.0

* Artifact classes added.
* Channel management tools (channelproperty decorator, ChannelManager class, etc.)
* Rearranged flowz.channels from a module to a subpackage.

