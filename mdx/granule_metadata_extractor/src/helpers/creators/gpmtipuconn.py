# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmtipuconn"
provider_path = "gpmtipuconn/"
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
        lines = []
        for encoded_line in file_obj_stream.iter_lines():
            lines.append(encoded_line.decode("utf-8"))

        for line in lines[2:]: #skip the first two lines
            #Year Mon Day Jday  Hr Min Rain[mm/h]  Lat        Lon
            #2024  01  01  001  00  00   0.000  37.93452  -75.47104
            tkn = line.split()
            lat = float(tkn[-2])
            lon = float(tkn[-1])
            if lat >= -90. and lat <= 90. and lon >=-180. and lon <= 180.:
                lats.append(lat)
                lons.append(lon)
                utc.append(datetime.strptime(''.join([tkn[0],tkn[1],tkn[2],tkn[4],tkn[5]]),'%Y%m%d%H%M'))

        #x0 = [x for x in lats if x < -90.]
        #y0 = [y for y in lons if y < -180.]
        #if len(x0) > 0:
        #    print(filename, x0)
        #if len(y0) > 0:
        #    print(filename, y0)

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
