# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re
import gzip
 
from math import radians, degrees, acos, cos, sin

short_name = "dclma"
provider_path = "lma/malma/fullrate/"
file_type = "ASCII"

#DCLMA network cap: dclma_range = 200km
#dclma_range = 200.
#Earth radius ~ 6371 km
#dclma_lat = 38.8894 
#dclma_lon = -77.03517
#mlat = radians(dclma_lat)
#mlon = radians(dclma_lon)
#plat = mlat
#dlon = degrees(acos((cos(dclma_range/6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
#dlat = degrees(dclma_range/6371.)
#north0, south0, east0, west0 = [dclma_lat+dlat, dclma_lat-dlat,
#                            dclma_lon+dlon, dclma_lon-dlon]

north0, south0, east0, west0 = [40.688, 37.091, -74.724, -79.346]


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
        if 'DCLMA' in filename:
            return self.read_metadata_dclma(filename, file_obj_stream)


    def read_metadata_dclma(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from dclma files
        """
        with gzip.GzipFile(fileobj=file_obj_stream, mode='rb') as gzipfile:
             encoded_lines = gzipfile.readlines()
           
        lines = []
        for encoded_line in encoded_lines:
            line = encoded_line.decode("utf-8")
            line = line.replace('\n','')
            lines.append(line)
        tmp_str = [x for x in lines if 'Data start time:' in x]
        utc_date_str = tmp_str[0].split()[3] #i.e., 12/21/25
        utc_date = datetime.strptime(utc_date_str,'%m/%d/%y')

        tmp_str = [x for x in lines if '*** data ***' in x]
        index = lines.index(tmp_str[0]) 

        if index == len(lines)-1: #'*** data ***' is the last line; no data lines
           tkn = filename.split('_')
           utc_str = ''.join([tkn[1],tkn[2]]) #i.e., 251218235000
           start_time = datetime.strptime(utc_str,'%y%m%d%H%M%S')
           end_time = start_time + timedelta(seconds = 600)
           north, south, east, west = [north0, south0, east0, west0]
        else:
           utc = []
           lats = []
           lons = []
           for line in lines[index+1:]:
               tkn = line.split()
               utc.append(float(tkn[0]))
               lats.append(float(tkn[1]))
               lons.append(float(tkn[2]))

           start_time = utc_date + timedelta(seconds = int(min(utc))) 
           end_time = utc_date + timedelta(seconds = int(max(utc)))

           north, south, east, west = [max(lats),min(lats),max(lons),min(lons)]

           #Exclude all data located more than 200 km from DCLMA center point
           north = min(north0,north)
           south = max(south0,south)
           east = min(east0,east)
           west = max(west0,west)
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
