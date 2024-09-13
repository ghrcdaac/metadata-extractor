# Create lookup zip for dlhimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re
from zipfile import ZipFile
import numpy as np
import json
import os

short_name = "dlhimpacts"
provider_path = "dlhimpacts__1/"
file_type = "ASCII"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        gps_path = '../P3_Nav_impacts.zip'
        with ZipFile(os.path.join(os.path.dirname(__file__),gps_path), 'r') as p3zip:
            with p3zip.open('P3_Nav_impacts.json') as p3object:
                P3_Nav = json.load(p3object)
        self.nav_time = np.array([datetime.strptime(x,'%Y%m%d%H%M%S') for x in P3_Nav['time'][:]])
        self.nav_lat = np.array(P3_Nav['lat'][:])
        self.nav_lon = np.array(P3_Nav['lon'][:])

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.read_metadata_ascii(filename, file_obj_stream)

    def read_metadata_ascii(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        print(filename)
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        num_header_lines = int(file_lines[0].split(',')[0])
        lines = file_lines[num_header_lines:]
        minTime, maxTime, minlat, maxlat, minlon, maxlon = [datetime(2100,1,1),
                                                            datetime(1900,1,1),
                                                            90.0,-90.0,180.0,-180.0]
        dt0_str = filename.split('/')[-1].split('_')[-2] #i.e., 20230113
        dt0 = datetime.strptime(dt0_str,'%Y%m%d')
        sec = []
        for i in range(len(lines)):
            tkn = lines[i].split(',')
            if len(tkn) > 1:
               sec.append(float(tkn[0]))

        minTime = dt0 + timedelta(seconds=min(sec))
        maxTime = dt0 + timedelta(seconds=max(sec))

        idx = np.where((self.nav_time >= minTime) & (self.nav_time <= maxTime))[0]
        maxlat, minlat, maxlon, minlon = [self.nav_lat[idx].max(),
                                          self.nav_lat[idx].min(),
                                          self.nav_lon[idx].max(),
                                          self.nav_lon[idx].min()]

        return {
            "start": minTime,
            "end": maxTime,
            "north": maxlat,
            "south": minlat,
            "east": maxlon,
            "west": minlon,
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
