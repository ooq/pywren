import pywren
from six.moves import cPickle as pickle
import cloudpickle

def f(key):
    return key

futures = pywren.default_executor().map(f, range(2))
#pickle.dump(futures, open("future.pickle", "wb"))

#futures = pickle.load(open("future.pickle", "rb"))

res = [f.result() for f in futures]
print(res)
