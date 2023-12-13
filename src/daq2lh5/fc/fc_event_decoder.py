from __future__ import annotations

import copy
import logging
from typing import Any

from fcio import FCIO
import lgdo

from ..data_decoder import DataDecoder

log = logging.getLogger(__name__)

# put decoded values here where they can be used also by the orca decoder
fc_decoded_values = {
    # packet index in file
    "packet_id": {"dtype": "uint32"},
    # index of event
    "eventnumber": {"dtype": "int32"},
    # time since epoch
    "timestamp": {"dtype": "float64", "units": "s"},
    # time since beginning of file
    "runtime": {"dtype": "float64", "units": "s"},
    # number of triggered adc channels
    "numtraces": {"dtype": "int32"},
    # list of triggered adc channels
    "tracelist": {
        "dtype": "uint16",
        "datatype": "array<1>{array<1>{real}}",  # vector of vectors
        "length_guess": 16,
    },
    # fpga baseline
    "baseline": {"dtype": "uint16"},
    # fpga energy
    "daqenergy": {"dtype": "uint16"},
    # right now, index of the trigger (trace)
    "channel": {"dtype": "uint32"},
    # PPS timestamp in sec
    "ts_pps": {"dtype": "int32"},
    # clock ticks
    "ts_ticks": {"dtype": "int32"},
    # max clock ticks
    "ts_maxticks": {"dtype": "int32"},
    # the offset in sec between the master and unix
    "mu_offset_sec": {"dtype": "int32"},
    # the offset in usec between master and unix
    "mu_offset_usec": {"dtype": "int32"},
    # the calculated sec which must be added to the master
    "to_master_sec": {"dtype": "int32"},
    # the delta time between master and unix in usec
    "delta_mu_usec": {"dtype": "int32"},
    # the abs(time) between master and unix in usec
    "abs_delta_mu_usec": {"dtype": "int32"},
    # startsec
    "to_start_sec": {"dtype": "int32"},
    # startusec
    "to_start_usec": {"dtype": "int32"},
    # start pps of the next dead window
    "dr_start_pps": {"dtype": "int32"},
    # start ticks of the next dead window
    "dr_start_ticks": {"dtype": "int32"},
    # stop pps of the next dead window
    "dr_stop_pps": {"dtype": "int32"},
    # stop ticks of the next dead window
    "dr_stop_ticks": {"dtype": "int32"},
    # maxticks of the dead window
    "dr_maxticks": {"dtype": "int32"},
    # current dead time calculated from deadregion (dr) fields.
    # Give the total dead time if summed up.
    "deadtime_nsec": {"dtype": "int64"},
    # channel range which are affected
    "dr_ch_idx" : {"dtype": "uint16"},
    "dr_ch_len" : {"dtype": "uint16"},
    # waveform data
    "waveform": {
        "dtype": "uint16",
        "datatype": "waveform",
        "wf_len": 65532,  # max value. override this before initializing buffers to save RAM
        "dt": 16,  # override if a different clock rate is used
        "dt_units": "ns",
        "t0_units": "ns",
    },
}
"""Default FlashCam Event decoded values.

Re-used by :class:`~.orca.orca_flashcam.ORFlashCamWaveformDecoder`.

Warning
-------
This configuration can be dynamically modified by the decoder at runtime.
"""


class FCEventDecoder(DataDecoder):
    """Decode FlashCam digitizer event data."""

    def __init__(self, *args, **kwargs) -> None:
        # these are read for every event (decode_event)
        self.decoded_values = copy.deepcopy(fc_decoded_values)
        super().__init__(*args, **kwargs)
        self.skipped_channels = {}
        self.fc_config = None

    def set_file_config(self, fc_config: lgdo.Struct) -> None:
        """Access ``FCIOConfig`` members once when each file is opened.

        Parameters
        ----------
        fc_config
            extracted via :meth:`~.fc_config_decoder.FCConfigDecoder.decode_config`.
        """
        self.fc_config = fc_config
        self.decoded_values["waveform"]["wf_len"] = self.fc_config["nsamples"].value
        self.decoded_values["tracelist"]["length_guess"] = self.fc_config["nadcs"].value

    def get_key_lists(self) -> range:
        return [list(range(self.fc_config["nadcs"].value))]

    def get_decoded_values(self, channel: int = None) -> dict[str, dict[str, Any]]:
        # FC uses the same values for all channels
        return self.decoded_values

    def get_max_rows_in_packet(self) -> int:
        return self.fc_config["nadcs"].value

    def decode_packet(
        self,
        fcio: FCIO,
        evt_rbkd: lgdo.Table | dict[int, lgdo.Table],
        packet_id: int,
    ) -> bool:
        """Access ``FCIOEvent`` members for each event in the DAQ file.

        Parameters
        ----------
        fcio
            The interface to the ``fcio`` data. Enters this function after a
            call to ``fcio.get_record()`` so that data for `packet_id` ready to
            be read out.
        evt_rbkd
            A single table for reading out all data, or a dictionary of tables
            keyed by channel number.
        packet_id
            The index of the packet in the `fcio` stream. Incremented by
            :class:`~.fc.fc_streamer.FCStreamer`.

        Returns
        -------
        n_bytes
            (estimated) number of bytes in the packet that was just decoded.
        """
        any_full = False

        # a list of channels is read out simultaneously for each event
        # for iwf in fcio.event.trace_list:
        for idx in range(fcio.event.num_traces):
        
            iwf = fcio.event.trace_list[idx]
            if iwf not in evt_rbkd:
                if iwf not in self.skipped_channels:
                    # TODO: should this be a warning instead?
                    log.debug(f"skipping packets from channel {iwf}...")
                    self.skipped_channels[iwf] = 0
                self.skipped_channels[iwf] += 1
                continue
            tbl = evt_rbkd[iwf].lgdo
            if fcio.config.eventsamples != tbl["waveform"]["values"].nda.shape[1]:
                log.warning(
                    "event wf length was",
                    fcio.config.eventsamples,
                    "when",
                    self.decoded_values["waveform"]["wf_len"],
                    "were expected",
                )
            ii = evt_rbkd[iwf].loc

            # fill the table
            tbl["channel"].nda[ii] = iwf
            tbl["packet_id"].nda[ii] = packet_id
              # the eventnumber since the beginning of the file
            tbl["eventnumber"].nda[ii] = fcio.event.timestamp[0]
            # number of triggered adcs
            tbl["numtraces"].nda[ii] = fcio.event.num_traces
            tbl["tracelist"]._set_vector_unsafe(
                ii, fcio.event.trace_list
            )  # list of triggered adcs
            tbl["ts_pps"].nda[ii] = fcio.event.timestamp[1]
            tbl["ts_ticks"].nda[ii] = fcio.event.timestamp[2]
            tbl["ts_maxticks"].nda[ii] = fcio.event.timestamp[3]
            tbl["mu_offset_sec"].nda[ii] = fcio.event.timeoffset[0]
            tbl["mu_offset_usec"].nda[ii] = fcio.event.timeoffset[1]
            tbl["to_master_sec"].nda[ii] = fcio.event.timeoffset[2]
            tbl["delta_mu_usec"].nda[ii] = fcio.event.timeoffset[3]
            tbl["abs_delta_mu_usec"].nda[ii] = fcio.event.timeoffset[4]
            tbl["to_start_sec"].nda[ii] = fcio.event.timeoffset[5]
            tbl["to_start_usec"].nda[ii] = fcio.event.timeoffset[6]
            tbl["dr_start_pps"].nda[ii] = fcio.event.deadregion[0]
            tbl["dr_start_ticks"].nda[ii] = fcio.event.deadregion[1]
            tbl["dr_stop_pps"].nda[ii] = fcio.event.deadregion[2]
            tbl["dr_stop_ticks"].nda[ii] = fcio.event.deadregion[3]
            tbl["dr_maxticks"].nda[ii] = fcio.event.deadregion[4]
            # the dead-time affected channels
            if fcio.event.deadregion_size == 7:
                tbl["dr_ch_idx"].nda[ii] = fcio.event.deadregion[5]
                tbl["dr_ch_len"].nda[ii] = fcio.event.deadregion[6]
            else:
                tbl["dr_ch_idx"].nda[ii] = 0
                tbl["dr_ch_len"].nda[ii] = fcio.config.adcs

            # The following values are derived values by fcio-py
            # the time since epoch in seconds
            tbl["timestamp"].nda[ii] = fcio.event.utc_unix
            tbl["deadtime_nsec"].nda[ii] = fcio.event.dead_time_ns[iwf]
            # the time since the beginning of the file in seconds
            tbl["runtime"].nda[ii] = fcio.event.run_time
            # the fpga baseline values for each channel in LSB
            tbl["baseline"].nda[ii] = fcio.event.fpga_baseline[iwf]
            # the fpga energy values for each channel in LSB
            tbl["daqenergy"].nda[ii] = fcio.event.fpga_energy[iwf]

            # if len(traces[iwf]) != fcio.nsamples: # number of sample per trace check
            tbl["waveform"]["values"].nda[ii][:] = fcio.event.trace[idx]

            evt_rbkd[iwf].loc += 1
            any_full |= evt_rbkd[iwf].is_full()

        return bool(any_full)
