from daq2lh5.orca import orca_packet

def test_orca_packet_funcs(orca_stream):
    packet = orca_stream.load_packet()
    # These values are particular to the test orca file in legend-testdata and
    # may need to be changed if that file is changed
    assert orca_packet.is_short(packet) == False
    assert orca_packet.get_data_id(packet) == 7
    assert orca_packet.get_n_words(packet) == 4
    assert orca_packet.hex_dump(packet, return_output=True)[-1] == '3 0x6286930b'
    orca_stream.close_stream()  # avoid warning that file is still open
