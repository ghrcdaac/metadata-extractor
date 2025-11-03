# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmwxuasuconn"
provider_path = "gpmwxuasuconn/"

#Raxpol radar range ~ 55.56 km
#radar_range = 55.56.
#Earth radius ~ 6371 km
#radar_lat = 41.807945 
#radar_lon = -72.294595 
#mlat = radians(radar_lat)
#mlon = radians(radar_lon)
#plat = mlat
#dlon = degrees(acos((cos(radar_range/6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
#dlat = degrees(radar_range/6371.)
#north, south, east, west = [radar_lat+dlat, radar_lat-dlat,
#                            radar_lon+dlon, radar_lon-dlon]
north0, south0, east0, west0 = [42.308, 41.308, -71.624, -72.965]


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
        if filename.endswith('.txt'):
           self.file_type = 'ASCII'
        else:
           self.file_type = 'CSV'

        if '_TempRH_' in filename:
           return self.get_TempRH_metadata(filename, file_obj_stream)
        elif '_ParticleCount_' in filename:
           return self.get_ParticleCount_metadata(filename)
        elif '_Radar_' in filename:
           return self.get_Radar_metadata(filename, file_obj_stream)

    def get_ParticleCount_metadata(self, filename):
        """
        Extract temporal and spatial metadata from CSV files
        """
        #UConn_WxUAS_ParticleCount_20240311_06_04_40.csv
        utc_str = ''.join(filename.split('.')[0].split('_')[-4:]) #i.e., 20240311060440
        minTime = datetime.strptime(utc_str,'%Y%m%d%H%M%S') 
        maxTime = minTime 
        return {
            "start": minTime,
            "end": maxTime,
            "north": north0,
            "south": south0,
            "east": east0,
            "west": west0,
            "format": self.file_type
        }

              
    def get_Radar_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from TXT files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        utc = []
        for ii in range(1,len(file_lines)): #skip the first line
            tkn = file_lines[ii].rstrip('\n').split(',')
            cTime = datetime.strptime(tkn[0],'%d-%b-%Y %H:%M:%S') #i.e., 11-Mar-2024 05:24:46
            utc.append(cTime)

        minTime = min(utc)
        maxTime = max(utc)
        return {
            "start": minTime,
            "end": maxTime,
            "north": north0,
            "south": south0,
            "east": east0,
            "west": west0,
            "format": self.file_type
        }


    def get_TempRH_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from CSV files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        utc = []
        for ii in range(1,len(file_lines)): #skip the first line
            tkn = file_lines[ii].rstrip('\n').split(',')
            cTime = datetime.strptime(tkn[0],'%Y-%m-%d %H:%M:%S.%f')
            utc.append(cTime)

        minTime = min(utc)
        maxTime = max(utc)
        return {
            "start": minTime,
            "end": maxTime,
            "north": north0,
            "south": south0,
            "east": east0,
            "west": west0,
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
