# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmrmyounguconn"
provider_path = "gpmrmyounguconn/"
file_type = "CSV"

#2 sites for 2 different deployments:
#22_23:   N: 41.8178 E: -72.2575 S: 41.8178 W: -72.2575
#23_24:   N: 41.8078 E: -72.2939 S: 41.8078 W: -72.2939

#Sample files:
#UConn_RMYOUNG_D3R_22_23.csv (i.e., 0,2022-12-07 20:27:20,111,000.0)
#UConn_RMYOUNG_APU28_23_24.csv (i.e., 0,2023-12-01 17:31:37,031,000.0)

site_loc = {'22_23': [41.8178, -72.2575],
            '23_24': [41.8078, -72.2939]
           } 

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
        for line in file_lines[1:]: #skip the first line    
            if '1999-12-31' not in line:
               tkn = line.split(',')[1] #i.e., 2023-12-01 17:31:37
               cTime = datetime.strptime(tkn, '%Y-%m-%d %H:%M:%S')
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
