# create lookup zip for g3halowhymsie 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "g3halowhymsie"
provider_path = "g3halowhymsie/"
file_type = "HDF-5"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.get_hdf5_metadata(filename, file_obj_stream)


    def get_hdf5_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from HDF-5 files
        """
        #sample file: whymsie_apex_HALO_G3_20241031_R0_F1.h5
        h5 = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        navgrp = h5.groups['Nav_Data']
        lat = navgrp.variables['gps_lat'][:]
        lon = navgrp.variables['gps_lon'][:]

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        #data_collection_start_date: 20241031
        utc_ref = datetime.strptime(h5.data_collection_start_date,'%Y%m%d')
        utc_sec = navgrp.variables['gps_time'][:]  #hours
        start_time = utc_ref + timedelta(hours=utc_sec.min())
        end_time = utc_ref + timedelta(hours=utc_sec.max())

        h5.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }


    def main(self):
        # start_time = time.time()
        self.process_collection(short_name, provider_path)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
