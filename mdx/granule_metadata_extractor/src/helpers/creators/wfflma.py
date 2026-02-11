# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re
import gzip
 
from math import radians, degrees, acos, cos, sin

short_name = "wfflma"
provider_path = "lma/malma/fullrate/"
file_type = "ASCII"

#WFFLMA network cap: lma_range = 200km
#WFFLMA Coordinate center (lat,lon,alt): 38.0926323 -75.5895615 -2000.00
#lma_range = 200.
#Earth radius ~ 6371 km
#lma_lat = 38.0926 
#lma_lon = -75.5896
#mlat = radians(lma_lat)
#mlon = radians(lma_lon)
#plat = mlat
#dlon = degrees(acos((cos(lma_range/6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
#dlat = degrees(lma_range/6371.)
#north0, south0, east0, west0 = [lma_lat+dlat, lma_lat-dlat,
#                            lma_lon+dlon, lma_lon-dlon]
#print(north0, south0, east0, west0)
north0, south0, east0, west0 = [39.891, 36.294, -73.304, -77.875]


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
        if 'WFF' in filename:
            return self.read_metadata_wfflma(filename, file_obj_stream)


    def read_metadata_wfflma(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from wfflma files
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

        #Sample file: WFF_251218_235000_0600.dat
        tkn = filename.split('_')
        utc_str = ''.join([tkn[1],tkn[2]]) #i.e., 251218235000
        start_time = datetime.strptime(utc_str,'%y%m%d%H%M%S')
        end_time = start_time + timedelta(seconds = 600)
        north, south, east, west = [north0, south0, east0, west0]

        tmp_str = [x for x in lines if '*** data ***' in x]
        index = lines.index(tmp_str[0]) 

        if index < len(lines)-1: #'*** data ***' is not the last line
           utc = []
           lats = []
           lons = []
           for line in lines[index+1:]:
               tkn = line.split()
               lat = float(tkn[1])
               lon = float(tkn[2])
               #Exclude all data located more than 200 km from WFFLMA center point
               if lat >= south0 and lat <= north0 and lon >= west0 and lon <= east0:
                  utc.append(float(tkn[0]))
                  lats.append(lat)
                  lons.append(lon)

           if len(utc) > 0:
              start_time = utc_date + timedelta(seconds = int(min(utc))) 
              end_time = utc_date + timedelta(seconds = int(max(utc)))
              north, south, east, west = [max(lats),min(lats),max(lons),min(lons)]

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
