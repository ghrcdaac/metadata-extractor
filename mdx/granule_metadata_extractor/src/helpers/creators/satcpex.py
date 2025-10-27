# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "satcpex"
provider_path = "satcpex/"
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


    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        print(filename)
        utc = []
        lats = []
        lons = []
        for encoded_line in file_obj_stream.iter_lines():
            #print(encoded_line)
            line = encoded_line.decode("utf-8")
            #print(line)
            #Sample: IWG110hz,2017-06-01T13:46:32.101, 26.078796, -80.154362,,
            tkn = line.split(',')
            if len(tkn[2]) != 0 and len(tkn[3]) != 0:
               utc.append(datetime.strptime(tkn[1],'%Y-%m-%dT%H:%M:%S.%f'))
               lats.append(float(tkn[2]))
               lons.append(float(tkn[3]))

        start_time, end_time = [min(utc), max(utc)]
        north, south, east, west = [max(lats),min(lats),max(lons),min(lons)]  

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
