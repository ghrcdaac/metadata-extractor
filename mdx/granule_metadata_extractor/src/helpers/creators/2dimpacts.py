# 2dimpacts is being used to template new approach of creating lookup zip
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import time
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
        start_time = datetime.strptime(
            '2020-01-15T01:37:06Z', '%Y-%m-%dT%H:%M:%SZ')
        end_time = datetime.strptime(
            '2020-02-28T20:41:44Z', '%Y-%m-%dT%H:%M:%SZ')
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
            granule_start = datetime.strptime(
                f"{tkn[0]},{tkn[1]},{tkn[2]}", '%Y,%j,%H:%M')
            granule_end = datetime.strptime(
                f"{tkn[0]},{tkn[3]},{tkn[4]}", '%Y,%j,%H:%M')

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
                dt = datetime.strptime(f"{tkn[0]},{tkn[1]},{tkn[2]},{tkn[3]},{tkn[4]}",
                                       "%Y,%j,%H,%M,%S.%f")
            else:
                dt = datetime.strptime(
                    f"{tkn[0]},{tkn[1]},{tkn[2]},{tkn[3]}", "%Y,%j,%H,%M")

            start_time = min(start_time, dt)
            end_time = max(end_time, dt)

            north, south, east, west = [self.loc[stn_id][0] + 0.01,
                                        self.loc[stn_id][0] - 0.01,
                                        self.loc[stn_id][1] + 0.01,
                                        self.loc[stn_id][1] - 0.01]

        if start_time == datetime(2100, 1, 1) or end_time == datetime(1900, 1, 1):
            start_time = datetime.strptime(
                '2020-01-15T01:37:06Z', '%Y-%m-%dT%H:%M:%SZ')
            end_time = datetime.strptime(
                '2020-02-28T20:41:44Z', '%Y-%m-%dT%H:%M:%SZ')

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
        start_time = time.time()
        self.process_collection(short_name, provider_path)
        elapsed_time = time.time() - start_time
        print(f"Elapsed time in seconds: {elapsed_time}")


if __name__ == '__main__':
    MDXProcessing().main()
