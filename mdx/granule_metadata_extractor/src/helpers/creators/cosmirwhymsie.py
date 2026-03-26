# create lookup zip for cosmirwhymsie 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "cosmirwhymsie"
provider_path = "cosmirwhymsie/"

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

        year = np.array(datafile['Year'][:])
        month = np.array(datafile['Month'][:])
        day_of_month = np.array(datafile['DayOfMonth'][:])
        hour = np.array(datafile['Hour'][:])
        minute = np.array(datafile['Minute'][:])
        second = np.array(datafile['Second'][:])
        millisecond = np.array(datafile['MilliSecond'][:])
        lat0 = np.array(datafile['Latitude'][:])
        lon0 = np.array(datafile['Longitude'][:])
        utc = []
        lat = []
        lon = []
        for i in range(0,year.shape[0]):
            for j in range(0, year.shape[1]):
                if -999 not in [lat0[i,j], lon0[i,j]]:
                   lat.append(lat0[i,j])
                   lon.append(lon0[i,j])

                   if -999 not in [year[i,j],month[i,j],day_of_month[i,j],hour[i,j],minute[i,j],second[i,j],millisecond[i,j]]:
                      utc.append(datetime(year[i,j],month[i,j],day_of_month[i,j],hour[i,j],minute[i,j],second[i,j],millisecond[i,j]))

        north, south, east, west = [np.max(lat), np.min(lat),
                                    np.max(lon), np.min(lon)]        

        if len(utc) > 0:
           start_time = min(utc)
           end_time = max(utc)
        else:
           #whymsie_cosmirh_ER2_20241113_S184902_E191905_R0_crosstrack.nc
           #All time vlues are missing in this file
           tkn = filename.split('_')
           start_time = datetime.strptime(tkn[3]+tkn[4],'%Y%m%dS%H%M%S')
           end_time = datetime.strptime(tkn[3]+tkn[5],'%Y%m%dE%H%M%S')
           if end_time < start_time:
              end_time = end_time + timedelta(days=1)

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
