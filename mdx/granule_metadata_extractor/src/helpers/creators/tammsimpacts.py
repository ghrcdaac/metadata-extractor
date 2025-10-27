# Create lookup zip for tammsimpacts
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "tammsimpacts"
provider_path = "tammsimpacts/fieldCampaigns/impacts/TAMMS/data/"
file_type = "ASCII"

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
        return self.read_metadata_ascii(filename, file_obj_stream)

    def read_metadata_ascii(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        sec0 = []
        lat0 = []
        lon0 = []
        num_header_lines = int(file_lines[0].split(',')[0])
        for line in file_lines[num_header_lines:]:
            tkn = line.split(',')
            if tkn[1] != '-9999.000000' and tkn[2] != '-9999.000000':
               sec, lat, lon = [float(tkn[0]), float(tkn[1]), float(tkn[2])]
               sec0.append(sec)
               lat0.append(lat)
               lon0.append(lon)
        
        utc_date = datetime.strptime(filename.split('/')[-1].split('_')[-2],'%Y%m%d')
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
