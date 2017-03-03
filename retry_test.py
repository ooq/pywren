#from gevent import monkey

#monkey.patch_socket()
#monkey.patch_ssl()

import pywren
import random


if __name__ == "__main__":
    import logging
    #logging.basicConfig(level=logging.DEBUG)

    # fh = logging.FileHandler('simpletest.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(pywren.wren.formatter)
    # pywren.wren.logger.addHandler(fh)

    def test_add(x):
        if random.random() < 0.5:
            raise Exception("killed intentionally")
        return x

    wrenexec = pywren.default_executor()
    fut = wrenexec.map_sync_with_rate_and_retries(test_add, range(10), rate=100)

    res = [f.result() for f in fut]
    print(res)


