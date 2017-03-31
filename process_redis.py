import sys
import os
import numpy as np
import cPickle as pickle

prefix = sys.argv[1]
for i in os.listdir("./"):
	if i.startswith(prefix):
		testcase = i[len(prefix):]
		a = pickle.load(open(i,"rb"))
		b = [c[3] for c in a]
		print testcase + "  " + str(np.mean(b)) + "  " + str(np.median(b)) + "  " + str(sum(b))
