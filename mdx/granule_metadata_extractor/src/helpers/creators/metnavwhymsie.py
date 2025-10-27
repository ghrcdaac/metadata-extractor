# Create lookup zip for metnavwhymsie 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "metnavwhymsie"
provider_path = "metnavwhymsie/"
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
        print(filename)
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        num_header_lines = int(file_lines[0].split(',')[0])
        databuf = file_lines[num_header_lines:]
        minTime, maxTime, minlat, maxlat, minlon, maxlon = [datetime(2100,1,1),
                                                            datetime(1900,1,1),
                                                            90.0,-90.0,180.0,-180.0]
        year = int(filename.split('/')[-1].split('_')[3][0:4])
        for i in range(len(databuf)):
            tkn = databuf[i].split(',')
            sec, doy, lat, lon = [int(tkn[0]), int(tkn[1]), float(tkn[2]), float(tkn[3])]
            dt = datetime(year,1,1) + timedelta(seconds=sec) + timedelta(days=doy-1)
            minTime = min(minTime, dt)
            maxTime = max(maxTime, dt)
            if lat != -9999.0 and lon != -9999.0:
                maxlat = max(maxlat, lat)
                minlat = min(minlat, lat)
                maxlon = max(maxlon, lon)
                minlon = min(minlon, lon)

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
