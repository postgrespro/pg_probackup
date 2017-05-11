class Base(object):
    def __init__(self):
        self.a = 10
    def func(self, arg1, arg2):
        print 'Child {0}, a = {1}'.format(arg1, arg2)


class ChildA(Base):
    def __init__(self):
        Base.__init__(self)
        b = 5
        c = b + self.a
        print 'Child A, a = {0}'.format(c)


class ChildB(Base):
    def __init__(self):
        super(ChildB, self).__init__()
        b = 6
        c = b + self.a
        self.func('B', c)

#ChildA()
ChildB()
