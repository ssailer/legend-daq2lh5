import pytest
from daq2lh5.orca import orca_packet

@pytest.fixture(scope="module")
def fc_packets(orca_stream):
    packets = []
    packets.append(orca_stream.load_packet(2).copy()) # config
    packets.append(orca_stream.load_packet(3).copy()) # waveform
    orca_stream.close_stream()  # avoid warning that file is still open
    return packets


def test_orfc_config_decoding(fc_packets):
    config_packet = fc_packets[0]
    assert config_packet is not None

def test_orfc_waveform_decoding(fc_packets):
    wf_packet = fc_packets[1]
    assert wf_packet is not None
