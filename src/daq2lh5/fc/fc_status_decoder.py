from __future__ import annotations

from fcio import FCIO

from ..data_decoder import DataDecoder
from ..raw_buffer import RawBuffer


class FCStatusDecoder(DataDecoder):
    """Decode FlashCam digitizer status data."""

    def __init__(self, *args, **kwargs) -> None:
        self.decoded_values = {
            # 0: Errors occurred, 1: no errors
            "status": {"dtype": "int32"},
            # fc250 seconds, microseconds, dummy, startsec startusec
            "statustime": {"dtype": "float32", "units": "s"},
            # CPU seconds, microseconds, dummy, startsec startusec
            "cputime": {"dtype": "float64", "units": "s"},
            # fc250 seconds, microseconds, dummy, startsec startusec
            "startoffset": {"dtype": "float32", "units": "s"},
            # Total number of cards (number of status data to follow)
            "cards": {"dtype": "int32"},
            # Size of each status data
            "size": {"dtype": "int32"},
            # FC card-wise environment status
            "environment": {
                # Array contents:
                # [0-4] Temps in mDeg
                # [5-10] Voltages in mV
                # 11 main current in mA
                # 12 humidity in o/oo
                # [13-14] Temps from adc cards in mDeg
                # FIXME: change to a table?
                "dtype": "uint32",
                "datatype": "array_of_equalsized_arrays<1,1>{real}",
                "length": 16,
            },
            # FC card-wise list DAQ errors during data taking
            "totalerrors": {"dtype": "uint32"},
            "enverrors": {"dtype": "uint32"},
            "ctierrors": {"dtype": "uint32"},
            "linkerrors": {"dtype": "uint32"},
            "othererrors": {
                "dtype": "uint32",
                "datatype": "array_of_equalsized_arrays<1,1>{real}",
                "length": 5,
            },
        }
        """Default FlashCam status decoded values.

        Warning
        -------
        This configuration can be dynamically modified by the decoder at runtime.
        """
        super().__init__(*args, **kwargs)

    def decode_packet(
        self, fcio: FCIO, status_rb: RawBuffer, packet_id: int
    ) -> bool:
        # aliases for brevity
        tbl = status_rb.lgdo
        ii = status_rb.loc

        # status -- 0: Errors occurred, 1: no errors
        tbl["status"].nda[ii] = fcio.status.status

        # times
        tbl["statustime"].nda[ii] = fcio.status.statustime[0] + fcio.status.statustime[1] / 1e6
        tbl["cputime"].nda[ii] = fcio.status.statustime[2] + fcio.status.statustime[3] / 1e6
        tbl["startoffset"].nda[ii] = fcio.status.statustime[5] + fcio.status.statustime[6] / 1e6

        # Total number of cards (number of status data to follow)
        tbl["cards"].nda[ii] = fcio.status.cards

        # Size of each status data
        tbl["size"].nda[ii] = fcio.status.size

        # TODO: map card values
        # conceptually the following must happen
        # but still to be decided which way cards are entered into the tbl

        # # FC card-wise environment status (temp., volt., hum., ...)
        # tbl["environment"].nda[ii][:] = fcio.status.data[:].environment

        # # FC card-wise list DAQ errors during data taking
        # tbl["totalerrors"].nda[ii][:] = fcio.status.data[:].totalerrors
        # tbl["linkerrors"].nda[ii][:] = fcio.status.data[:].linkerrors
        # tbl["ctierrors"].nda[ii][:] = fcio.status.data[:].ctierrors
        # tbl["enverrors"].nda[ii][:] = fcio.status.data[:].enverrors
        # tbl["othererrors"].nda[ii][:] = fcio.status.data[:].othererrors

        status_rb.loc += 1

        return status_rb.loc >= len(tbl)
