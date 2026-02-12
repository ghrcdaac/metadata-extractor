# Create lookup zip for avapswhymsie 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

from netCDF4 import Dataset
import numpy as np

short_name = "avapswhymsie"
provider_path = "avapswhymsie/"

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
        if filename.endswith('.nc'):
           self.file_type = 'netCDF-4'
           return self.read_metadata_nc(filename, file_obj_stream)
        else: #.ict
           self.file_type = 'ASCII'
           return self.read_metadata_ascii(filename, file_obj_stream)

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

        num_of_header_lines = int(file_lines[0].split(',')[0])
        sec0 = []
        lat0 = []
        lon0 = []
        for line in file_lines[num_of_header_lines:]:
            tkn = line.split(',')
            if tkn[7] != '-9999' and tkn[8] != '-9999':
               lat0.append(float(tkn[7]))
               lon0.append(float(tkn[8]))
               sec0.append(float(tkn[0]))
        launch_time_string =list(filter(lambda x: 'Launch Time:' in x, file_lines))[0]
        utc_date_str = launch_time_string.split()[2]  #i.e., '2024-10-30'
        utc_date = datetime.strptime(utc_date_str,'%Y-%m-%d')
        minTime = utc_date+timedelta(seconds=min(sec0))
        maxTime = utc_date+timedelta(seconds=max(sec0))
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
            "format":self.file_type
        }


    def read_metadata_nc(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF files
        """
        print(filename)
        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        dt0 = data['time'][:].flatten() #seconds since reftime
        lat0 = data['lat'][:].flatten()
        lon0 = data['lon'][:].flatten()

        #get indices for vaid lat and lon values
        idx = [i for i in range(0,len(lat0)) if lat0.mask[i] == False and lon0.mask[i] == False]
        dt = dt0[idx]
        lat = lat0[idx]
        lon = lon0[idx]

        t0 = datetime.strptime('T'.join(data['time'].units.split()[2:4]),'%Y-%m-%dT%H:%M:%S')
        start_time = t0+timedelta(seconds=dt.min())
        end_time = t0+timedelta(seconds=dt.max())
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
