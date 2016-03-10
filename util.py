class Minimum(object):
    """
    Less than all other objects other than itself.
    """
    def __gt__(self, other):
        return False
    
    def __lt__(self, other):
        return self is not other

    def __eq__(self, other):
        return self is other
    
    def __ne__(self, other):
        return self is not other
    
    def __ge__(self, other):
        return self is other
    
    def __le__(self, other):
        return True

class Maximum(object):
    """
    Greater than all other objects other than itself.
    """
    def __gt__(self, other):
        return self is not other

    def __lt__(self, other):
        return False
    
    def __eq__(self, other):
        return self is other
    
    def __ne__(self, other):
        return self is not other
    
    def __ge__(self, other):
        return True
    
    def __le__(self, other):
        return self is other

MINIMUM = Minimum()
MAXIMUM = Maximum()

