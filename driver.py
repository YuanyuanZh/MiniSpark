import pickle
import StringIO

import zerorpc
import cloudpickle

from rdd import *

r = TextFile('myfile')
m = Map(r, lambda s: s.split())
f = Filter(m, lambda a: int(a[1]) > 2)

output = StringIO.StringIO()
pickler = cloudpickle.CloudPickler(output)
pickler.dump(f)
objstr = output.getvalue()

c = zerorpc.Client()
c.connect("tcp://127.0.0.1:4242")

print c.hello(objstr)

