# create lookup zip for gpmceiluconn
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "gpmceiluconn"
provider_path = "gpmceiluconn/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-4"

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
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        sec = np.array(datafile['time'][:])
        ref_time = datetime(1970,1,1)

        #Extract metadata from netCDF-4 data files
        start_time, end_time = [ref_time+timedelta(seconds=int(sec.min())),
                                ref_time+timedelta(seconds=int(sec.max()))]

        # the latitude and longitude values are swapped within the data.
        # and incorrect values (-75, 37)
        #lat = np.array(datafile['Location_latitude'][:])
        #lon = np.array(datafile['Location_longitude'][:])

        #north, south, east, west = [np.max(lat), np.min(lat),
        #                            np.max(lon), np.min(lon)]

        lat = 41.808
        lon = -72.294
        north, south, east, west = [lat+0.01, lat-0.01, lon+0.01, lon-0.01]
        datafile.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
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
