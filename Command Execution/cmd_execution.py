import unittest
import time
import sys
from tmtc.tmtc_py4j import *
import context

# Path to jar file that contains the TMTC Library, use contextlib and getattr??
classpath = '/create/a/classpath/'

# Path to the spacecraft database, use context lib and getattr?
scdbpath = '/create/a/scbdpath/'

# Configuration of the connection to the onboard software
obsw_connection = TCPClient(configuration='AUTH SCID1 CCSDS_TM_DATALINK')


class TestCommandExecution(unittest.TestCase):

    def setUp(self):
        pass

    # HK_BEACON Actions
    def test_hkbeacon_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.HKBeacon.reset')
            self.assertEqual(result, 'pass')

    def test_hkbeacon_send(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.HKBeacon.send')
            self.assertEqual(result, 'pass')

    # TMDebug Actions
    def test_tmdebug_reset(self):
        with TMTCPy4j(classpath,obsw_connection,scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.TMDebug.reset')
            self.assertEqual(result, 'pass')

    # TMTCEvent Actions
    def test_tmtcevent_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.TMTCEvent.reset')
            self.assertEqual(result, 'pass')

    def test_tmtcevent_forwardEvent(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.TMTCEvent.forwardEvent')
            self.assertEqual(result, 'pass')

    # TMTCTransfer Actions
    def test_tmtctransfer_clearGetTransfer(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.TMTCTransfer.clearGetTransfer')
            self.assertEqual(result, 'pass')

    def test_tmtctransfer_clearSetTransfer(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.tmtc.TMTCTransfer.clearSetTransfer')
            self.assertEqual(result, 'pass')

    # WatchdogPeriodicAction Actions
    def test_watchdogperiodicaction_clear(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.WatchdogPeriodicAction.clear')
            self.assertEqual(result, 'pass')

    def test_watchdogperiodicaction_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('chd.WatchdogPeriodicAction.reset')
            self.assertEqual(result, 'pass')

    # Storage Actions
    def test_storage_wipe(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.Storage.wipe')
            self.assertEqual(result, 'pass')

    def test_storage_getParamToChannel(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.Storage.getParamToChannel')
            self.assertEqual(result, 'pass')

    def test_storage_getParamFromChannel(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.Storage.getParamFromChannel')
            self.assertEqual(result, 'pass')

    # ConfigurationManager Actions
    def test_configurationmanager_resetAll(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.resetAll')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_loadAll(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.loadAll')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_storeAll(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.storeAll')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_loadProfile(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.loadProfile')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_load(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.load')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_store(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.store')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_erase(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.erase')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_eraseConfig(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.eraseConfig')
            self.assertEqual(result, 'pass')

    def test_configurationmanager_eraseAll(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.ConfigurationManager.eraseAll')
            self.assertEqual(result, 'pass')

    # OBT Actions
    def test_obt_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.OBT.reset')
            self.assertEqual(result, 'pass')

    def test_obt_update(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.OBT.update')
            self.assertEqual(result, 'pass')

    # EventDispatcher Actions
    def test_eventdispatcher_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('core.EventDispatcher.reset')
            self.assertEqual(result, 'pass')

    # OBC Actions
    def test_obc_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.reset')
            self.assertEqual(result, 'pass')

    def test_obc_kickWatchdog(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.kickWatchdog')
            self.assertEqual(result, 'pass')

    def test_obc_markCurrentImageStable(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.markCurrentImageStable')
            self.assertEqual(result, 'pass')

    def test_obc_clearImage(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.clearImage')
            self.assertEqual(result, 'pass')

    def test_obc_updateImageCrc(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.updateImageCrc')
            self.assertEqual(result, 'pass')

    def test_obc_resetGPS(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.resetGPS')
            self.assertEqual(result, 'pass')

    def test_obc_resetGyros(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.OBC.resetGyros')
            self.assertEqual(result, 'pass')

    # Time Actions
    def test_time_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.Time.reset')
            self.assertEqual(result, 'pass')

    def test_time_refresh(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.Time.refresh')
            self.assertEqual(result, 'pass')

    # GPIO Actions
    def test_gpio_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.GPIO.reset')
            self.assertEqual(result, 'pass')

    # PlatformI2C Actions
    def test_platformi2c_resetStatistics(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.PlatformI2c.resetStatistics')
            self.assertEqual(result, 'pass')

    # PlatformSPI Actions
    def test_platformspi_resetErrors(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.obc.PlatformSPI.resetErrors')
            self.assertEqual(result, 'pass')

    # STX Actions
    def test_stx_reset(self):
        with TMTCPy4j(classpath, obsw_connection, scdbpath) as tmtc:
            result = tmtc.invoke('platform.STX.reset')
            self.assertEqual(result, 'pass')


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


if __name__ == '__main__':
    with open('MOCI_CMD_EXE.log', 'w') as f:
        main(f)