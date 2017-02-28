import numpy as np
import time
import pywren

def original_code():
	def wait_10x_sec_and_plus_one(x):
	    time.sleep(10*x)
	    return x + 1

	N = 3
	x = np.arange(N)

	# FIXME: for stability, we should proabbly use the local dummy executor
	# but currently the dummy invoker impl does not support wait()
	futures = pywren.default_executor().map(wait_10x_sec_and_plus_one, x)

	fs_notdones = futures
	for xi in x:
		fs_dones, fs_notdones = pywren.wait(fs_notdones,
	                                    return_when=pywren.wren.ANY_COMPLETED,
	                                    WAIT_DUR_SEC=1)
		res =np.array([f.result() for f in fs_dones])
		print res
		#np.testing.assert_array_equal(res, [xi+1])

def test_all_complete(self):
    def wait_x_sec_and_plus_one(x):
        time.sleep(x)
        return x + 1

    N = 10
    x = np.arange(N)

    futures = pywren.default_executor().map(wait_x_sec_and_plus_one, x)

    fs_dones, fs_notdones = pywren.wait(futures,
                                    return_when=pywren.wren.ALL_COMPLETED)
    res = np.array([f.result() for f in fs_dones])
    np.testing.assert_array_equal(res, x+1)

def test_any_complete():
    def wait_x_sec_and_plus_one(x):
        time.sleep(x)
        return x + 1

    N = N
    x = np.arange(N)

    futures = pywren.default_executor().map(wait_10x_sec_and_plus_one, x)

    fs_notdones = futures
    while (len(fs_notdones) > 0):
        fs_dones, fs_notdones = pywren.wait(fs_notdones,
                                        return_when=pywren.wren.ANY_COMPLETED,
                                        WAIT_DUR_SEC=1)
        self.assertTrue(len(fs_dones) > 0)
    res = np.array([f.result() for f in futures])
    np.testing.assert_array_equal(res, x+1)
