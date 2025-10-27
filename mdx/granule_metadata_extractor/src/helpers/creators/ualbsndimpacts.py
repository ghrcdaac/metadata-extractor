# Create lookup zip for ualbsndimpacts
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "ualbsndimpacts"
provider_path = "ualbsndimpacts/"
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
            file_lines.append(encoded_line.decode("ISO-8859-1"))

        num_header_lines = 3 
        databuf = file_lines[num_header_lines:]
        minTime, maxTime, minlat, maxlat, minlon, maxlon = [datetime(2100,1,1),
                                                            datetime(1900,1,1),
                                                            90.0,-90.0,180.0,-180.0]

        for i in range(len(databuf)):
            tkn = databuf[i].split()
            #tkn[0]: 1/19/2023 
            #tkn[1]: 5:40:53
            #tkn[2]: PM
            #tkn[13]: 073°47'57.4"W 
            #tkn[14]: 42°41'54.1"N

            dt = tkn[0].split('/') #i.e., 1, 19, 2023
            tt = tkn[1].split(':') #i.e., 5, 40, 53
            utc_time_str = ''.join([dt[2],dt[0].zfill(2),dt[1].zfill(2),'T',tt[0].zfill(2),tt[1].zfill(2),tt[2].zfill(2)])
            utc_time = datetime.strptime(utc_time_str,'%Y%m%dT%H%M%S')
            if tkn[2] == 'PM':
               utc_time = utc_time + timedelta(hours=12)
            minTime = min(minTime, utc_time)
            maxTime = max(maxTime, utc_time)

            lon_str = tkn[13] #i.e.,'072°47\'35.1"W'
            deg = float(lon_str.split('°')[0]) #i.e., 72
            minu = float(lon_str.split('°')[1].split('\'')[0]) #i.e.,47
            sec = float(lon_str.split('°')[1].split('\'')[1].split('"')[0]) #i.e.,35.1  
            lon = (sec/60.+minu)/60.+deg
            if lon_str.endswith('W'):
               lon = -1.*lon

            lat_str = tkn[14] #i.e., '43°07\'30.8"N'
            deg = float(lat_str.split('°')[0]) #i.e., 43
            minu = float(lat_str.split('°')[1].split('\'')[0]) #i.e.,7
            sec = float(lat_str.split('°')[1].split('\'')[1].split('"')[0]) #i.e.,30.8
            lat = (sec/60.+minu)/60.+deg
            if lat_str.endswith('S'):
               lat = -1.*lat

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
