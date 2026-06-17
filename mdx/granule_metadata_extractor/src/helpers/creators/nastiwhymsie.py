# create lookup zip for nastiwhymsie
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "nastiwhymsie"
provider_path = "nastiwhymsie/"
file_type = "netCDF-3"

rad_utc = {'20241023': {'start': '2024-10-23T18:12:16Z', 'end': '2024-10-23T22:08:23Z'}, '20241113': {'start': '2024-11-13T18:54:45Z', 'end': '2024-11-13T23:02:11Z'}, '20241030': {'start': '2024-10-30T19:15:28Z', 'end': '2024-10-30T23:38:04Z'}, '20241107': {'start': '2024-11-07T18:30:15Z', 'end': '2024-11-07T23:12:34Z'}, '20241104': {'start': '2024-11-04T17:28:53Z', 'end': '2024-11-04T21:41:11Z'}, '20241022': {'start': '2024-10-22T17:26:47Z', 'end': '2024-10-22T22:31:23Z'}, '20241025': {'start': '2024-10-25T17:06:26Z', 'end': '2024-10-25T23:45:41Z'}, '20241112': {'start': '2024-11-12T18:16:19Z', 'end': '2024-11-12T21:49:27Z'}, '20241031': {'start': '2024-10-31T17:11:34Z', 'end': '2024-10-31T22:37:38Z'}}

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
        if '_RAD_' in filename:
           file_type = "netCDF-4"
           return self.get_rad_metadata(filename, file_obj_stream)
        else: #L2
           file_type = "netCDF-3"
           return self.get_L2_metadata(filename, file_obj_stream)


    def get_rad_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['Latitude'][:])
        lon = np.array(nc['Longitude'][:])

        yy = np.array(nc['Year'][:])
        mm = np.array(nc['Month'][:])
        dd = np.array(nc['Day'][:])
        utc = np.array(nc['UTC'][:])

        tt = []
        for i in range(0,len(yy)):
            tmp_str = ''.join([str.zfill(str(int(yy[i])),4),
                               str.zfill(str(int(mm[i])),2),
                               str.zfill(str(int(dd[i])),2),
                               str.zfill(str(int(utc[i])),4)])
            if tmp_str.endswith('60'): 
               tt.append(datetime.strptime(tmp_str[:-2],'%Y%m%d%H%M')+timedelta(seconds=60))       
            else:
               tt.append(datetime.strptime(tmp_str,'%Y%m%d%H%M%S'))

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time = min(tt) 
        end_time = max(tt) 

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

    def get_L2_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['lat_ir'][:])
        lon = np.array(nc['lon_ir'][:])

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        key = filename.split('_')[4]
        start_time = datetime.strptime(rad_utc[key]['start'],'%Y-%m-%dT%H:%M:%SZ') 
        end_time = datetime.strptime(rad_utc[key]['end'],'%Y-%m-%dT%H:%M:%SZ')  

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
