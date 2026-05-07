# create lookup zip for arowhymsie 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "arowhymsie"
provider_path = "arowhymsie/"

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
        #sample file: whymsie_aro_ER2_20241112203015_R0.nc
        start_time = datetime.strptime(filename.split('_')[3],'%Y%m%d%H%M%S') 

        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        utc_sec = datafile.end_time - datafile.start_time #seconds
        end_time = start_time+timedelta(seconds=utc_sec.item())

        lat = np.array(datafile['Lat'][:])
        lon = np.array(datafile['Lon'][:])
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]
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
