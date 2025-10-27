# create lookup zip for sbumetimpacts
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "sbumetimpacts"
provider_path = "sbumetimpacts/fieldCampaigns/impacts/SBU_MetStation/data/"

#The met station (the SBU AirMar Weather) was on the radar truck. The coordinates are the same as the radar truck locations.
#In 2020:
#2020/01/18-2020/01/19: Cedar Beach 40.965N, -73.030E
#2020/02/13: Cedar Beach 40.965N, -73.030E
#For the other days, Stony Brook University South Parking Lot 40.897N, -73.127E
 
#In 2022:
#January 17, 2022: Elmira airport 42.1742 N -76.8719 E
#January 29, 2022: Smith Point 40.7339 N -72.8615E
#February 25, 2022: Fort Edward 43.246 N -73.559 E
#For the other days, Stony Brook University South Parking Lot 40.897N, -73.127E
 
#In 2023:
#2023/01/25: Fort Edward 43.245 N, -73.559 E
#2023/01/28: Montauk Point, 41.065 N, -71.8627 E
#For the other days, Stony Brook University South Parking Lot 40.897N, -73.127E
site_loc = {'Cedar Beach': [-73.030,40.965],
            'Elmira airport': [-76.8719, 42.1742],
            'Smoth Point': [-72.8615, 40.7339],
            'Fort Edward 2022': [-73.559, 43.246],
            'Fort Edward 2023': [-73.559, 43.245],
            'Montauk Point': [-71.8627, 41.065],
            'SBU': [-73.127, 40.897]}
RT_track ={'2020-01-18':'Cedar Beach',
           '2020-01-19':'Cedar Beach',
           '2020-02-13':'Cedar Beach',
           '2022-01-17':'Elmira airport',
           '2022-01-29':'Smith Point',
           '2022-02-25':'Fort Edward 2022',
           '2023-01-25':'Fort Edward 2023',
           '2023-01-28':'Montauk Point'}


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
        if filename.endswith('.nc'):
           self.file_type = "netCDF-4"
           return self.get_nc_metadata(filename, file_obj_stream)
        else:
           self.file_type = "CSV"
           return self.get_csv_metadata(filename, file_obj_stream)


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

        date1 = start_time.strftime('%Y-%m-%d')
        date2 = end_time.strftime('%Y-%m-%d')
        if date1 != date2:
           print(filename,date1,date2)
        if date1 in RT_track.keys():
           site = RT_track[date1]
        else:
           site = 'SBU'

        lat = site_loc[site][1]
        lon = site_loc[site][0]
        north,south,east,west = [lat+0.01,lat-0.01,lon+0.01,lon-0.01]

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

    def get_csv_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from CSV files
        """
        #lat and lon for *MAN.csv file is provided in datainfo sheet
        #(40.7282N, 74.0068W)
        south, north, west, east = [40.7182, 40.7382, -74.0168, -73.9968]

        #Initializing 2 parameters
        minTime, maxTime = [datetime(2100, 1, 1),datetime(1900, 1, 1)]

        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        for ii in range(1,len(file_lines)): #skip the first line
            line = file_lines[ii]
            tkn = line.split(";")
            cTime = datetime.strptime(line.split(";")[0], '%Y-%m-%d %H:%M:%S')
            minTime = min(minTime, cTime)
            maxTime = max(maxTime, cTime)

        return {
            "start": minTime, 
            "end": maxTime,
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
