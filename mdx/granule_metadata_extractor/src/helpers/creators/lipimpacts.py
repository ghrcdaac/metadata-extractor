# Create lookup zip for lipimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "lipimpacts"
provider_path = "lipimpacts/fieldCampaigns/impacts/LIP/data/"
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
        lines = []
        for encoded_line in file_obj_stream.iter_lines():
            lines.append(encoded_line.decode("utf-8"))

        dt = []
        lat = []
        lon = []

        #First line is the header beginning with 'Time'
        for i in range(1,len(lines)):
            tkn = lines[i].split() #tkn[0] i.e., '23-Feb-2020T13:16:00.000''
            if 'NaN' in [tkn[8], tkn[9]]:
               continue #ignore this data line
            else:
               dt.append(datetime.strptime(tkn[0],'%d-%b-%YT%H:%M:%S.%f'))
               lat.append(float(tkn[8]))
               lon.append(float(tkn[9]))

        minTime = min(dt)
        maxTime = max(dt)
        maxlat = max(lat)
        minlat = min(lat)
        maxlon = max(lon)
        minlon = min(lon)

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
