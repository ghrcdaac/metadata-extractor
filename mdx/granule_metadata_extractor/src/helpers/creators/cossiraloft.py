# create lookup zip for cossiraloft
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "cossiraloft"
provider_path = "aloftcossir/"
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
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['Latitude'])
        lon = np.array(nc['Longitude'])
        year = np.array(nc['Year'])
        mon = np.array(nc['Month'])
        day = np.array(nc['DayOfMonth'])
        hr = np.array(nc['Hour'])
        mn = np.array(nc['Minute'])
        sec = np.array(nc['Second'])

        #set missing values in lat and lon to np.nan
        lat[lat==-999.0] = np.nan
        lon[lon==-999.0] = np.nan
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time, end_time = [datetime(2100, 1, 1), datetime(1900, 1, 1)]
        for i in range(sec.shape[0]):
            for j in range(sec.shape[1]):
                if int(year[i,j]) != -999.0:
                    total_sec = int(hr[i,j]*3600 + mn[i,j]*60 + sec[i,j])
                    dt = datetime(int(year[i,j]), int(mon[i,j]), int(day[i,j])) + \
                         timedelta(seconds=total_sec)
                    start_time = min(dt, start_time)
                    end_time = max(dt, end_time)

        nc.close()
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
