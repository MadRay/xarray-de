import os
from typing import Tuple
from struct import pack
import logging
import numpy as np
import multiprocessing as mp
from dataclasses import dataclass, astuple
import asyncio

import xarray as xr

from downloader import download_files

# Setup Logging
logger = logging.getLogger('dwd.de::tp')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


@dataclass
class GrbDimensions:
    """ Dimensions for headers from parsed grib file """
    lat_min: int = None
    lat_max: int = None
    lon_min: int = None
    lon_max: int = None
    step_lat: int = None
    step_lon: int = None
    multi: int = None
    empty: float = -100500.0


class TpConverter:
    def __init__(self):
        """ """
        pass

    def write_wgf4(self, subdirname: str, headers: GrbDimensions, data: np.array):
        """ Write data to target file (with creating dir logic)
        :param subdirname: str - directory name for result
        :param headers: object - headers for file write
        :param data: bytes  - payload """
        if not os.path.isdir('icon_d2'):
            os.mkdir('icon_d2')
        dirname = f'icon_d2/{subdirname}'
        if not os.path.isdir(dirname):
            os.mkdir(dirname)
        with open(f'{dirname}/PDATA.wgf4', 'wb') as file:
            # Writing header, it is static in this task
            header_bytes = pack('<7if', *astuple(headers))
            file.write(header_bytes)
            body_bytes = pack(f'<{len(data)}f', *data)
            file.write(body_bytes)

    def load_grib(self, filename: str) -> Tuple[GrbDimensions, np.array]:
        """ Open and load pygrib file
        :param filename: str filepath to open"""

        def m_int(value: float) -> int:
            """ Get int value with multiplication """
            return int(value*multi)

        multi = 100  # may be it is better to calculate it (via lat/lon format_float_positional)

        ds = xr.open_dataset(filename, engine="cfgrib")
        data = ds.tp[0].to_numpy()
        lats = ds.latitude.to_numpy()  # FIXME: maybe we can use xarray dataFrames (but lose cmpblty with other pkgs)
        lons = ds.longitude.to_numpy()

        step_lat = m_int(lats[1]) - m_int(lats[0])
        step_lon = m_int(lons[1]) - m_int(lons[0])
        headers = GrbDimensions(m_int(lats[0]), m_int(lats[-1]), m_int(lons[0]), m_int(lons[-1]),
                                     step_lat, step_lon, multi)
        logger.debug(f'{filename} dim is {headers}')
        return headers, data

    def process_file(self, filename: str) -> Tuple[GrbDimensions, np.array]:
        """ Process single file """
        headers, tp_values = self.load_grib(f'download/{filename}.grib2')

        try:
            lats, lons = tp_values.shape
            point_cnt = lats*lons
            result_arr = np.full(point_cnt, -100500., dtype=np.float64)  # Allocating memory

            pos = -1
            for x in range(0, lats):
                for y in range(0, lons):
                    pos += 1
                    value = tp_values[x][y]
                    if type(value) is np.float64:
                        result_arr[pos] = value

            return headers, result_arr
        except Exception as error:
            logger.error(f'Error on {filename}: {type(error)} {error}')
            return None, None

    def main(self):
        """ Entry point """
        filenames = asyncio.run(download_files())

        with mp.Pool(processes=mp.cpu_count()) as pool:
            pool_out: tuple = pool.map_async(self.process_file, filenames).get()  # We will get results in tasks order

        prev_arr = None
        for num, result in enumerate(pool_out):
            headers, arr = result
            if arr is None:
                logger.error(f'Failed to create result for {filenames[num]}')
                continue
            if prev_arr is not None:
                arr_to_write = np.array([arr[x] - prev_arr[x] if arr[x]!=-100500. else -100500. for x in range(0, len(arr))])
            else:
                arr_to_write = arr

            self.write_wgf4(filenames[num], headers=headers, data=arr_to_write)
            prev_arr = arr


if __name__ == '__main__':
    ins = TpConverter()
    ins.main()
