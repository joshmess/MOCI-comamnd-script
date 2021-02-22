import unittest
import sys
from tmtc.tmtc_py4j import *
import context

# FIND IN GITLAB or on BA_BOI
classpath = '/create/a/classpath/'

# FIND IN GITLAB or on BA_BOI
scdbpath = '/create/a/scbdpath/'

# Configuration of the connection to the onboard software
obsw_connection = TCPClient(configuration='AUTH SCID1 CCSDS_TM_DATALINK')

# Global connection
tmtc = TMTCPy4j(classpath, obsw_connection, scdbpath)


class TestCommandExecution(unittest.TestCase):

    def setUp(self):
        pass

    # HK_BEACON Actions
    def test_hkbeacon_reset(self):
        result = tmtc.invoke('chd.tmtc.HKBeacon.reset')
        print(type(result))

    def test_hkbeacon_send(self):
        result = tmtc.invoke('chd.tmtc.HKBeacon.send')
        print(type(result))

    # TMDebug Actions
    def test_tmdebug_reset(self):
        result = tmtc.invoke('chd.tmtc.TMDebug.reset')
        print(type(result))

    # TMTCEvent Actions
    def test_tmtcevent_reset(self):
        result = tmtc.invoke('chd.tmtc.TMTCEvent.reset')
        print(type(result))

    def test_tmtcevent_forwardEvent(self):
        result = tmtc.invoke('chd.tmtc.TMTCEvent.forwardEvent')
        print(type(result))

    # TMTCTransfer Actions
    def test_tmtctransfer_clearGetTransfer(self):
        result = tmtc.invoke('chd.tmtc.TMTCTransfer.clearGetTransfer')
        print(type(result))

    def test_tmtctransfer_clearSetTransfer(self):
        result = tmtc.invoke('chd.tmtc.TMTCTransfer.clearSetTransfer')
        print(type(result))

    # WatchdogPeriodicAction Actions
    def test_watchdogperiodicaction_clear(self):
        result = tmtc.invoke('chd.WatchdogPeriodicAction.clear')
        print(type(result))

    def test_watchdogperiodicaction_reset(self):
        result = tmtc.invoke('chd.WatchdogPeriodicAction.reset')
        print(type(result))

    # Storage Actions
    def test_storage_wipe(self):
        result = tmtc.invoke('core.Storage.wipe')
        print(type(result))

    def test_storage_getParamToChannel(self):
        result = tmtc.invoke('core.Storage.getParamToChannel')
        print(type(result))

    def test_storage_getParamFromChannel(self):
        result = tmtc.invoke('core.Storage.getParamFromChannel')
        print(type(result))

    # ConfigurationManager Actions
    def test_configurationmanager_resetAll(self):
        result = tmtc.invoke('core.ConfigurationManager.resetAll')
        print(type(result))

    def test_configurationmanager_loadAll(self):
        result = tmtc.invoke('core.ConfigurationManager.loadAll')
        print(type(result))

    def test_configurationmanager_storeAll(self):
        result = tmtc.invoke('core.ConfigurationManager.storeAll')
        print(type(result))

    def test_configurationmanager_loadProfile(self):
        result = tmtc.invoke('core.ConfigurationManager.loadProfile')
        print(type(result))

    def test_configurationmanager_load(self):
        result = tmtc.invoke('core.ConfigurationManager.load')
        print(type(result))

    def test_configurationmanager_store(self):
        result = tmtc.invoke('core.ConfigurationManager.store')
        print(type(result))

    def test_configurationmanager_erase(self):
        result = tmtc.invoke('core.ConfigurationManager.erase')
        print(type(result))

    def test_configurationmanager_eraseConfig(self):
        result = tmtc.invoke('core.ConfigurationManager.eraseConfig')
        print(type(result))

    def test_configurationmanager_eraseAll(self):
        result = tmtc.invoke('core.ConfigurationManager.eraseAll')
        print(type(result))

    # OBT Actions
    def test_obt_reset(self):
        result = tmtc.invoke('core.OBT.reset')
        print(type(result))

    def test_obt_update(self):
        result = tmtc.invoke('core.OBT.update')
        print(type(result))

    # EventDispatcher Actions
    def test_eventdispatcher_reset(self):
        result = tmtc.invoke('core.EventDispatcher.reset')
        print(type(result))

    # OBC Actions
    def test_obc_reset(self):
        result = tmtc.invoke('platform.obc.OBC.reset')
        print(type(result))

    def test_obc_kickWatchdog(self):
        result = tmtc.invoke('platform.obc.OBC.kickWatchdog')
        print(type(result))

    def test_obc_markCurrentImageStable(self):
        result = tmtc.invoke('platform.obc.OBC.markCurrentImageStable')
        print(type(result))

    def test_obc_clearImage(self):
        result = tmtc.invoke('platform.obc.OBC.clearImage')
        print(type(result))

    def test_obc_updateImageCrc(self):
        result = tmtc.invoke('platform.obc.OBC.updateImageCrc')
        print(type(result))

    def test_obc_resetGPS(self):
        result = tmtc.invoke('platform.obc.OBC.resetGPS')
        print(type(result))

    def test_obc_resetGyros(self):
        result = tmtc.invoke('platform.obc.OBC.resetGyros')
        print(type(result))

    # Time Actions
    def test_time_reset(self):
        result = tmtc.invoke('platform.obc.Time.reset')
        print(type(result))

    def test_time_refresh(self):
        result = tmtc.invoke('platform.obc.Time.refresh')
        print(type(result))

    # GPIO Actions
    def test_gpio_reset(self):
        result = tmtc.invoke('platform.obc.GPIO.reset')
        print(type(result))

    # PlatformI2C Actions
    def test_platformi2c_resetStatistics(self):
        result = tmtc.invoke('platform.obc.PlatformI2c.resetStatistics')
        print(type(result))

    # PlatformSPI Actions
    def test_platformspi_resetErrors(self):
        result = tmtc.invoke('platform.obc.PlatformSPI.resetErrors')
        print(type(result))

    # STX Actions
    def test_stx_reset(self):
        result = tmtc.invoke('platform.STX.reset')
        print(type(result))


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


if __name__ == '__main__':
    with open('MOCI_CMD_EXE.log', 'w') as f:
        main(f)