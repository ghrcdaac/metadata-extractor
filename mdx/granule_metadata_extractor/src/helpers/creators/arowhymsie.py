# Create lookup zip for arowhymsie 
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

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        self.file_type = 'netCDF-4'
        return self.read_metadata_nc(filename, file_obj_stream)


    def read_metadata_nc(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF files
        """
        print(filename)
        #whymsie_aro_ER2_20241104213603_R0.nc
        utc_date = filename.split('_')[3] #i.e., 20241104213603
        start_time = datetime.strptime(utc_date,'%Y%m%d%H%M%S')
        end_time = start_time

        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = data['Lat'][:].flatten()
        lon = data['Lon'][:].flatten()
        north, south, east, west = [lat.max(), lat.min(), lon.max(), lon.min()]

        data.close()

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
