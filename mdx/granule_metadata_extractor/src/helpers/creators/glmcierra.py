# create lookup zip for glmcierra 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "glmcierra"
provider_path = "glmcierra/"
file_type = "netCDF-4"

file_excluded = ['OR_GLM-L2-CIERRA-DB_GOES-EAST_s20192931845000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-EAST_s20193132345000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203590215000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203591600000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20210122000000.nc']

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
        print(filename)
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        ftype = datafile.file_format
        if ftype.startswith('NETCDF3'):
            file_type = "netCDF-3"
        else:
            file_type = "netCDF-4"

        lats = np.array(datafile['FLASH_LAT'][:])
        lons = np.array(datafile['FLASH_LON'][:])

        north, south, east, west = [np.nanmax(lats),
                                    np.nanmin(lats),
                                    np.nanmax(lons),
                                    np.nanmin(lons)]
        if filename.split('/')[-1] in file_excluded:
            #assign summary metadata 
            #"north": 57.267, "south": -57.312, "east": 180.0, "west": -180.0
            north, south, east, west = [57.267, -57.312, 180.0, -180.0]

        start_time = datetime.strptime(datafile.TIME_COVERAGE_START,'%Y-%m-%d %H:%M:%SZ')
        end_time = datetime.strptime(datafile.TIME_COVERAGE_END,'%Y-%m-%d %H:%M:%SZ')
        datafile.close()
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
