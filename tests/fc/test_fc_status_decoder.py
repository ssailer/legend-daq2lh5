
import lgdo
from fcio import FCIOTag
import numpy as np
import pytest
from pytest import approx

from daq2lh5.fc.fc_status_decoder import FCStatusDecoder
from daq2lh5.raw_buffer import RawBuffer


@pytest.fixture(scope="module")
def status_rb(fcio_obj):
    decoder = FCStatusDecoder()
    rb = RawBuffer(lgdo=decoder.make_lgdo(size=1))

    # just get the first status record in the file and exit
    # while True:
    #     pid = fcio_obj.get_record()
    #     assert pid is True  # make sure there is at least one status record in the file
    for status in fcio_obj.statuses:
        # if fcio_obj == FCIOTag.Status:
        decoder.decode_packet(fcio=fcio_obj, status_rb=rb, packet_id=420)
        break

    return rb


def test_decoding(status_rb):
    assert status_rb.is_full() is True


def test_data_types(status_rb):
    tbl = status_rb.lgdo

    assert isinstance(tbl["status"], lgdo.Array)
    assert isinstance(tbl["statustime"], lgdo.Array)
    assert isinstance(tbl["cputime"], lgdo.Array)
    assert isinstance(tbl["startoffset"], lgdo.Array)
    assert isinstance(tbl["cards"], lgdo.Array)
    assert isinstance(tbl["size"], lgdo.Array)
    assert isinstance(tbl["environment"], lgdo.ArrayOfEqualSizedArrays)
    assert isinstance(tbl["totalerrors"], lgdo.Array)
    assert isinstance(tbl["linkerrors"], lgdo.Array)
    assert isinstance(tbl["ctierrors"], lgdo.Array)
    assert isinstance(tbl["enverrors"], lgdo.Array)
    assert isinstance(tbl["othererrors"], lgdo.ArrayOfEqualSizedArrays)


def test_values(status_rb, fcio_obj):
    fc = fcio_obj
    tbl = status_rb.lgdo
    i = status_rb.loc - 1

    assert i == 0
    assert tbl["status"].nda[i] == fc.status.status
    assert tbl["statustime"].nda[i] == approx(fc.status.statustime[0] + fc.status.statustime[1] / 1e6)
    assert tbl["cputime"].nda[i] == approx(fc.status.statustime[2] + fc.status.statustime[3] / 1e6)
    assert tbl["startoffset"].nda[i] == approx(
        fc.status.statustime[5] + fc.status.statustime[6] / 1e6
    )
    assert tbl["cards"].nda[i] == fc.status.cards
    assert tbl["size"].nda[i] == fc.status.size
    # assert np.array_equal(tbl["environment"].nda[i], fc.status.environment)
    # assert tbl["totalerrors"].nda[i] == fc.status.totalerrors
    # assert tbl["linkerrors"].nda[i] == fc.status.linkerrors
    # assert tbl["ctierrors"].nda[i] == fc.status.ctierrors
    # assert tbl["enverrors"].nda[i] == fc.status.enverrors
    # assert np.array_equal(tbl["othererrors"].nda[i], fc.status.othererrors)
