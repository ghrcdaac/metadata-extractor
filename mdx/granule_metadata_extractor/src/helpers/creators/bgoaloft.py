# Create lookup zip for bgoaloft
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re
from zipfile import ZipFile
import numpy as np
import json
import os

short_name = "bgoaloft"
provider_path = "bgoaloft/"
file_type = "ASCII"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        gps_path = '../Nav_aloft.zip'
        print(os.path.join(os.path.dirname(__file__),gps_path))
        with ZipFile(os.path.join(os.path.dirname(__file__),gps_path), 'r') as p3zip:
            with p3zip.open('nav_aloft.json') as p3object:
                Nav_aloft = json.load(p3object)
        self.nav_time = np.array([datetime.strptime(x,'%Y%m%d%H%M%S') for x in Nav_aloft['time'][:]])
        self.nav_lat = np.array(Nav_aloft['lat'][:])
        self.nav_lon = np.array(Nav_aloft['lon'][:])

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
        """
        :return:
        """
        utc = []
        for encoded_line in file_obj_stream.iter_lines():
            #Sample line: 2023-07-12 23:00:00     178
            file_lines = encoded_line.decode("utf-8")
            tkn = file_lines.split()
            utc.append(datetime.strptime(tkn[0]+tkn[1],'%Y-%m-%d%H:%M:%S'))

        minTime, maxTime = [min(utc), max(utc)]
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
