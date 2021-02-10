import unittest
import time
import sys
from tmtc.tmtc_py4j import *
import contet

# Path to jar file that contains the TMTC Library, use contextlib and getattr??
classpath = '/create/a/classpath/'

# Path to the spacecraft database, use context lib and getattr?
scdbpath = '/create/a/scbdpath/'

# Configuration of the connection to the onboard software
obsw_connection = TCPClient(configuration='AUTH SCID1 CCSDS_TM_DATALINK')


class TestCommandExecution(unittest.TestCase):
    
    def setUp(self):
        pass

    # HK_BEACON Methods
    def test_beacon_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            #tmtc.invoke('chd.tmtc.HKBeacon.reset')
            self.assertEqual('a' * 4, 'aaaa')

    def test_beacon_send(self):
        frame = 5
        #tmtc.invoke('chd.tmtc.HKBeacon.send', frame)


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


if __name__ == '__main__':
    with open('MOCI_CMD_EXE.log', 'w') as f:
        main(f)

