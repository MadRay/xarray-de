import os
import sys
import re
import logging
from typing import Tuple
from datetime import datetime, timedelta
from bz2 import decompress
import asyncio

import httpx

logger = logging.getLogger('dwd.de::tp.downloader')


async def download_files() -> list:
    """ Download each file and place it in some directory
    :return list of downloaded filenames (in our format)"""

    def transform_filenames(original: str) -> str:
        """ Convert filenames to internal format
        :param original: filename of original file"""
        input_info = re.search(r'(20\d{8})_(\d{3})', original)
        input_dt = datetime.strptime(input_info.group(1), '%Y%m%d%H')
        hour_offset = int(input_info.group(2))
        res_dt = input_dt + timedelta(hours=hour_offset)
        return f"{datetime.strftime(res_dt, '%d.%m.%Y_%H:%M')}_{int(res_dt.timestamp())}"

    if not os.path.isdir('download'):
        os.mkdir('download')

    base_url, filenames = await _get_source_urls()
    result_filenames = [transform_filenames(fl) for fl in filenames]

    tasks = [_download_file(base_url, filenames[x], result_filenames[x]) for x in range(0, len(filenames))]
    await asyncio.gather(*tasks)

    return result_filenames


async def _get_source_urls() -> Tuple[str, list]:
    """ Get list of files and base_url from source """
    index_url = 'https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/tot_prec/'
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(index_url)
            data = response.text
            real_base_url = str(response.url)  # In case if site return 3xx to another directory
    except Exception as error:
        logger.fatal(f'Failed to get index directory list! {type(error)} {error}')
        sys.exit(1)

    filenames = re.findall(r'href="(icon-d2_germany_regular-lat-lon_single[^"\s]+)', data)
    return real_base_url, filenames


async def _download_file(base_url: str, fl: str, output_fl: str):
    """ Download and unzip file
    :param base_url: str remoter HTTP directory URI
    :param fl: str remote filename (bz2)
    :param output_fl: str output filename (w/o extension) """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f'{base_url}{fl}')
            data_bz = response.content
    except Exception as error:
        logger.error(f'Failed to download {fl}: {type(error)} {error}')
        return

    with open(f'download/{output_fl}.grib2', 'wb') as fp:
        data = decompress(data_bz)
        fp.write(data)
