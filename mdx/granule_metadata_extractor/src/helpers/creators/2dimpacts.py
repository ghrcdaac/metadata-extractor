# 2dimpacts is being used to template new approach of creating lookup zip
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re
import os

short_name = "2dimpacts"
provider_path = "2dimpacts/fieldCampaigns/impacts/2DVD/data/"
file_type = "ASCII"


class MDXProcessing(MDX):

    def __init__(self):
        self.loc = {"sn70": [37.93450, -75.47081], "sn25": [37.93715, -75.46622],
                    "sn38": [37.92938, -75.47314], "sn35": [37.94432, -75.48116],
                    "sn37": [37.93764, -75.45618]}

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if 'diameter020' in filename:
            return self.read_metadata_diameter020()
        elif 'raintotalhour' in filename:
            return self.read_metadata_raintotalhour(filename, file_obj_stream)
        else:
            return self.read_metadata_regular(filename, file_obj_stream)

    def read_metadata_diameter020(self):
        """
        Extracts temporal and spatial metadata from diameter020 files
        """
        start_time = datetime(2020, 1, 15, 1, 37, 6)
        end_time = datetime(2020,2,28,20,41,44)
        north, south, east, west = [37.954319999999996, 37.919380000000004,
                                    -75.44618, -75.49116000000001]
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }

    def read_metadata_raintotalhour(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from raintotalhour files
        """
        start_time, end_time = [datetime(2100, 1, 1), datetime(1900, 1, 1)]
        stn_id = re.search(
            '^impacts_2dvd_(sn.*)_raintotalhour.txt$', filename)[1]

        for encoded_line in file_obj_stream.iter_lines():
            line = encoded_line.decode("utf-8")
            tkn = line.split()
            start_time_split=tkn[2].split(':')
            end_time_split=tkn[4].split(':')
            granule_start = datetime(int(tkn[0]), 1, 1, int(start_time_split[0]), int(start_time_split[1])) + timedelta(int(tkn[1]) - 1)
            granule_end = datetime(int(tkn[0]), 1, 1, int(end_time_split[0]), int(end_time_split[1])) + timedelta(int(tkn[3]) - 1)

            start_time = min(start_time, granule_start)
            end_time = max(end_time, granule_end)

            north, south, east, west = [self.loc[stn_id][0] + 0.01,
                                        self.loc[stn_id][0] - 0.01,
                                        self.loc[stn_id][1] + 0.01,
                                        self.loc[stn_id][1] - 0.01]
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }

    def read_metadata_regular(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from the following files:
        eachdrop, largedrop, dropcounts, raindsd_ter, raindsd, rainparameter_ter, rainparameter
        """
        start_time, end_time = [datetime(2100, 1, 1), datetime(1900, 1, 1)]
        stn_id = re.search('^impacts_2dvd_.*(sn\\d{2})_.*$', filename)[1]

        for encoded_line in file_obj_stream.iter_lines():
            line = encoded_line.decode("utf-8")
            tkn = line.split()
            if 'eachdrop' in filename or 'largedrop' in filename:
                seconds_fraction, seconds = math.modf(float(tkn[4]))
                microseconds = int(seconds_fraction*1000000)
                dt = datetime(int(tkn[0]), 1, 1, int(tkn[2]), int(tkn[3]),
                                int(seconds), microseconds) + timedelta(int(tkn[1]) - 1)
            else:
                dt = datetime(int(tkn[0]), 1, 1, int(tkn[2]), int(tkn[3])) + timedelta(int(tkn[1]) -1)

            start_time = min(start_time, dt)
            end_time = max(end_time, dt)

            north, south, east, west = [self.loc[stn_id][0] + 0.01,
                                        self.loc[stn_id][0] - 0.01,
                                        self.loc[stn_id][1] + 0.01,
                                        self.loc[stn_id][1] - 0.01]

        if start_time == datetime(2100, 1, 1) or end_time == datetime(1900, 1, 1):
            start_time = datetime(2020, 1, 15, 1, 37, 6)
            end_time = datetime(2020, 2, 28, 20, 41, 44)

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
