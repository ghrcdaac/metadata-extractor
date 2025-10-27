# create lookup zip for smapcpex 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "smapcpex"
provider_path = "smapcpex/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-3"

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
        Extract temporal and spatial metadata from netCDF-3 files
        """
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        #Extract time info from filename
        tkn = filename.split('/')[-1].split('.nc')[0].split('_')
        start_time = datetime.strptime(tkn[-2],'%Y%m%d%H%M')
        end_time = datetime.strptime(tkn[-1],'%Y%m%d%H%M')

        #Extract metadata from netCDF-3 data files
        lat = datafile['lat'][:].flatten()
        lon = datafile['lon'][:].flatten()

        #Remove fill values
        lat0 = []
        lon0 = []
        for i in range(0,len(lat)):
            if lat[i] != -9999.0 and lon[i] != -9999.0:
                lat0.append(lat[i])
                lon0.append(lon[i])

        north, south, east, west = [max(lat0),min(lat0),max(lon0),min(lon0)]

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
