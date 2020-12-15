import unittest
import time
import sys


class TestCommandExecution(unittest.TestCase):

    def setUp(self):
        pass

    # ---------------------------------------------DATA DOWNLINK MODE---------------------------------------------

    # -------------DATA PREP SUBMODE-------------

    # Parameters: Identifier of rcvd trans., time rcvd, size of trans., whether trans. wholly intact
    # Outcome: Satellite should rcv ACK specifying what was rcvd by GS, whether content should be resent
    def test_acknowledge_rx(self):
        self.assertEqual('a' * 4, 'aaaa')

    # -------------DATA TRANSMISSION SUBMODE-------------

    # Parameters: N/A
    # Outcome:
    def test_reboot_obc(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters: Requested file, file size
    # Outcome: Telemitry list downlinked over s-band as opposed to UHF
    def test_sband_tlm(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters: Requested file, file size
    # Outcome: Issuer can schedule a variable number of timed commands
    def test_schedule(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters: Time frame
    # Outcome: Return scheduled operations fot a given time frame
    def test_get_operations(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return MOCI battery charge
    def test_get_battery_charge(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return internal temperature of the MOCI
    def test_get_internal_temp(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return last packet received by MOCI over UHF
    def test_get_last_uhf_pkt(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return last packet received by MOCI over UHF
    def test_get_last_sband_pkt(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return remaining obc storage
    def test_get_remaining_obc_storage(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return remaining obc storage
    def test_get_obc_storage_in_use(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return remaining tx2i storage
    def test_get_remaining_tx2i_storage(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return remaining tx2i storage
    def test_get_tx2i_storage_in_use(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return last mode occupied by MOCI
    def test_get_last_mode(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return last submode occupied by MOCI
    def test_get_last_submode(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return current mode occupied by MOCI
    def test_get_current_mode(self):
        self.assertEqual('a' * 4, 'aaaa')

    # Parameters:
    # Outcome: Return current submode occupied by MOCI
    def test_get_current_submode(self):
        self.assertEqual('a' * 4, 'aaaa')


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


if __name__ == '__main__':
    with open('MOCI_CMD_EXE.log', 'w') as f:
        main(f)

