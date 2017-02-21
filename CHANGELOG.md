# Version 1.0.0

* `flowz.util.channel_inner_join` takes any number of channels.

# Version 0.7.0

* Adds introspection tools for channel management objects:
    * `get_channelmethods`
    * `get_channelproperties`
* Changes logger names for artifacts and downgrades some messages from INFO level to DEBUG
    * This change in logger names could break deployed logging configurations, and for this
      reason, we incremented the minor version; otherwise this would be a micro bump.

# Version 0.6.0

* Adds the `channelmethod` decorator as a callable-oriented version of `channelproperty` (Issue #41)
* Includes ipython notebook user guide (thanks to Patrick Rusk!)
* Suppresses the "future exception not handled" warnings from tornado for channels. (Issue #30)

# Version 0.5.0

* Adds the new `flowz.channels.tools` module for utility functions/helpers
  that extend the flexibility of channels without expanding the core interface.
* Adds rolling window functionality therein.

# Version 0.4.0

* Removes (silently) deprecated targets classes and tests
    * Including support within the `flowz.app.Flo` class
* Downgrades some logging messages to DEBUG level
* Changes some keyword parameters on windowby/groupby (from keyfunc on both
  to keys_func and key_func, respectively).

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

