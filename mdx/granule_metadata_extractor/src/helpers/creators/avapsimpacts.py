# Create lookup zip for avapsimpacts
# for all future collections
from datetime import datetime, timedelta
from netCDF4 import Dataset
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "avapsimpacts"
provider_path = "avapsimpacts/fieldCampaigns/impacts/AVAPS/data/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "ASCII"

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if filename.endswith('.ict'):
           self.file_type = "ASCII"
           return self.read_metadata_ascii(filename, file_obj_stream)
        else: #netCDF-3
           self.file_type = "netCDF-3"
           return self.read_metadata_nc(filename, file_obj_stream)

    def read_metadata_nc(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-3 files
        """
        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        reftime_str = 'T'.join(data['time'].units.split()[2:4]) #i.e., '2022-01-06T17:05:09'
        dt_base = datetime.strptime(reftime_str,'%Y-%m-%dT%H:%M:%S')
        dt0 = data['time'][:].flatten() #seconds since reftime
        lat0 = data['lat'][:].flatten()
        lon0 = data['lon'][:].flatten()

        #get indices for vaid lat and lon values
        idx = [i for i in range(0,len(lat0)) if lat0.mask[i] == False and lon0.mask[i] == False]
        dt = dt0[idx]
        lat = lat0[idx]
        lon = lon0[idx]

        start_time = dt_base+timedelta(seconds=dt.min())
        end_time = dt_base+timedelta(seconds=dt.max())
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


    def read_metadata_ascii(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            err_flag = 0
            try:
                decoded_line = encoded_line.decode("utf-8")
            except:
                err_flag = 1

            if err_flag == 0:
               file_lines.append(decoded_line) 

        sec0 = []
        lat0 = []
        lon0 = []
        for line in file_lines:
            matches = re.search(r'^([\d.-]*),(([\d.-]*),){7}([\d.-]*),.*$', line)
            if matches and all([(x != "-9999") for x in [matches[3], matches[4]]]):
               lat0.append(float(matches[3]))
               lon0.append(float(matches[4]))
               sec0.append(float(matches[1]))
        dt_base = datetime.strptime(filename.split('/')[-1].split('_')[3][0:8], '%Y%m%d')
        minTime = dt_base+timedelta(seconds=min(sec0))
        maxTime = dt_base+timedelta(seconds=max(sec0))
        north = max(lat0)
        south = min(lat0)
        west = min(lon0)
        east = max(lon0)

        return {
            "start": minTime,
            "end": maxTime,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format":self. file_type
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
