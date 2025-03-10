# create lookup zip for gpmmrr2uconn 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "gpmmrr2uconn"
provider_path = "gpmmrr2uconn/MRR2/"

#MRR2: (41.80778N, -72.29389E), 2021-2022.
#MRR_PRO: (41.81778N, -72.2575E), 2022-2023 and 2023-2024
MRR2={'lat':41.80778,'lon':-72.29389}
MRR_PRO={'lat':41.81778,'lon':-72.2575'}

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
        starttime = datetime(1970,1,1) + timedelta(seconds=int(datafile['time'][:].min()))
        endtime = datetime(1970,1,1) + timedelta(seconds=int(datafile['time'][:].max()))

        #Extract metadata from netCDF-3 data files
        lat = MRR2['lat']
        lon = MRR2['lon']

        north, south, east, west = [lat+0.01, lat-0.01,
                                    lon+0.01, lon-0.01]

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
