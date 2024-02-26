import time
import unittest

from .gracefull_killer import GracefulKiller


class TestGracefulKiller(unittest.TestCase):
    def test_exit_gracefully(self):
        graceful_killer = GracefulKiller()
        while not graceful_killer.kill_now:
            time.sleep(1)
            print("doing something in a loop ...")

        print("End of the program. I was killed gracefully :)")


if __name__ == '__main__':
    unittest.main()
