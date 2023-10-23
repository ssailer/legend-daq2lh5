import pytest

from daq2lh5.orca.orca_streamer import OrcaStreamer


@pytest.fixture(scope="module")
def orca_stream(lgnd_test_data):
    orstr = OrcaStreamer()
    orstr.open_stream(
        lgnd_test_data.get_path("orca/fc/l200-p02-r008-phy-20230113T174010Z.orca")
    )
    return orstr
