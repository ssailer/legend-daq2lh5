from __future__ import annotations

import logging

from fcio import FCIO
import lgdo
import numpy as np

from ..data_decoder import DataDecoder

log = logging.getLogger(__name__)


class FCConfigDecoder(DataDecoder):
    """Decode FlashCam config data.

    Note
    ----
    Derives from :class:`~.data_decoder.DataDecoder` in anticipation of
    possible future functionality. Currently the base class interface is not
    used.

    Example
    -------
    >>> from fcio import fcio_open
    >>> from daq2lh5.fc.fc_config_decoder import FCConfigDecoder
    >>> fc = fcio_open('file.fcio')
    >>> decoder = FCConfigDecoder()
    >>> config = decoder.decode_config(fc)
    >>> type(config)
    lgdo.types.struct.Struct
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.config = lgdo.Struct()

    def decode_config(self, fcio: FCIO) -> lgdo.Struct:
        config_names = [
            "nsamples",  # samples per channel
            "nadcs",  # number of adc channels
            "ntriggers",  # number of triggertraces
            "telid",  # id of telescope
            "adcbits",  # bit range of the adc channels
            "sumlength",  # length of the fpga integrator
            "blprecision",  # precision of the fpga baseline
            "mastercards",  # number of attached mastercards
            "triggercards",  # number of attached triggercards
            "adccards",  # number of attached fadccards
            "gps",  # gps mode (0: not used, 1: external pps and 10MHz)
        ]
        fcio_attr_names = [
            "eventsamples",
            "adcs",
            "triggers",
            "telid",
            "adcbits",
            "sumlength",
            "blprecision",
            "mastercards",
            "triggercards",
            "adccards",
            "gps"
        ]
        for name, fcio_attr_name in zip(config_names, fcio_attr_names):
            if name in self.config:
                log.warning(f"{name} already in self.config. skipping...")
                continue
            value = np.int32(getattr(fcio, fcio_attr_name))  # all config fields are int32
            self.config.add_field(name, lgdo.Scalar(value))
        self.config.add_field("tracemap", lgdo.Array())
        return self.config

    def make_lgdo(self, key: int = None, size: int = None) -> lgdo.Struct:
        return self.config
