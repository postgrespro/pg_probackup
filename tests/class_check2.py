class Base(object):
    def __init__(self):
        self.a = 10
        self.b = 1
#    def func(self, arg1, arg2):
#        print 'Child {0}, a = {1}'.format(arg1, arg2)


class ChildA(Base):
    def __init__(self):
        Base.__init__(self)
        self.b = self.b + 1


class ChildB(ChildA):
    def __init__(self):
        ChildA.__init__(self)
        print 'b = {0}'.format(self.b)
#        c = b + self.a


#ChildA()
ChildB()
