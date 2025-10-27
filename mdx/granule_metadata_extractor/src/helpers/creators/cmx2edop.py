# create lookup zip for cmx2edop 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "cmx2edop"
provider_path = "cmx2edop/"
file_type = "netCDF-4"

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
        return self.get_nc_metadata(filename, file_obj_stream)


    def get_nc_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        ref_time = datetime(1970,1,1)
        start_time = min([ref_time + timedelta(seconds=x) for x in data['Products/TimeUTC'][:]])
        end_time = max([ref_time + timedelta(seconds=x) for x in data['Products/TimeUTC'][:]])
        lats = np.array(data['Navigation/Latitude'][:])
        lons = np.array(data['Navigation/Longitude'][:])
        north, south, east, west = [np.nanmax(lats),
                                    np.nanmin(lats),
                                    np.nanmax(lons),
                                    np.nanmin(lons)]
        data.close()
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
