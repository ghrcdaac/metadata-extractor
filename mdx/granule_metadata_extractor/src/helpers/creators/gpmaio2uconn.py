# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmaio2uconn"
provider_path = "gpmaio2uconn/"
file_type = "CSV"

#In 2021-2022 the AIO2 at 41.80778, -72.29389 was 4m above ground level
#In 2022-2023 the AIO2 at 41.81778, -72.2575 (D3R file) was at 2m above ground level and the the AIO2 at 41.80778, -72.29389 (GAIL file) was 4m above ground level
#In 2023-2024 both AIO2 were at 41.80778, -72.29389 with one being at 2m (Ground file) and the other at 4m (Roof file).
site_loc = {'2122':[41.80778, -72.29389],
            '2223_D3R': [41.81778, -72.2575],
            '2223_GAIL': [41.80778, -72.29389],
            '2324_GROUND': [41.80778, -72.29389],
            '2324_ROOF': [41.80778, -72.29389]}

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
        return self.get_csv_metadata(filename, file_obj_stream)

    def get_csv_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from CSV files
        """
        tkn = filename.split('/')[-1].split('.csv')[0]
        for site_name in site_loc.keys():
            if tkn.endswith(site_name):
               lat = site_loc[site_name][0]
               lon = site_loc[site_name][1]

        south, north, west, east = [lat-0.01,lat+0.01,lon-0.01,lon+0.01]

        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        utc = []
        for ii in range(1,len(file_lines)): #skip the first line
            line = file_lines[ii]
            #i.e., 2022-12-23 20:48:00
            cTime = datetime.strptime(line.split(",")[1], '%Y-%m-%d %H:%M:%S')
            utc.append(cTime)

        minTime = min(utc)
        maxTime = max(utc)

        return {
            "start": minTime,
            "end": maxTime,
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
