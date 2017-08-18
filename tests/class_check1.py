class Foo(object):
    def __init__(self, *value1, **value2):
# do something with the values
        print 'I think something is being called here'
#        print value1, value2


class MyFoo(Foo):
    def __init__(self, *args, **kwargs):
# do something else, don't care about the args
        print args, kwargs
        super(MyFoo, self).__init__(*args, **kwargs)


foo = MyFoo('Python', 2.7, stack='overflow', ololo='lalala')