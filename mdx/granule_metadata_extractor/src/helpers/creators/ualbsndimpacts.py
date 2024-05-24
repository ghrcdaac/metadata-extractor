# Create lookup zip for ualbsndimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "ualbsndimpacts"
provider_path = "ualbsndimpacts/"
file_type = "ASCII"

# with open('IMPACTS_UAlb_Soundings_UAETEC_20230119_1713_TSPOTINT.txt','r',encoding = 'ISO-8859-1') as fp:
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
            #print(encoded_line.decode("ISO-8859-1"))
            file_lines.append(encoded_line.decode("ISO-8859-1"))

        utc = []
        lat = []
        lon = []
        for line in file_lines[3:]:
            tkn = line.split()
            utc_str = [] #%m%d%Y%H%M%S
            #date
            dt_str = tkn[0].split('/')
            utc_str.append(dt_str[0].zfill(2))
            utc_str.append(dt_str[1].zfill(2))
            utc_str.append(dt_str[2].zfill(4))
            #hour,minute,second
            tt_str = tkn[1].split(':')
            utc_str.append(tt_str[0].zfill(2))
            utc_str.append(tt_str[1].zfill(2))
            utc_str.append(tt_str[2].zfill(2))

            utc0 = datetime.strptime(''.join(utc_str),'%m%d%Y%H%M%S')
            if tkn[2] == 'PM':
               utc0 = utc0 + timedelta(hours=12)
            utc.append(utc0)

            a=tkn[13] #longitude
            b=a.split('°')
            c=b[1].split('\'')
            d=c[1].split('"')
            lon0=float(b[0])+(float(c[0])+float(d[0])/60.)/60.
            if d[1] == 'W':
               lon0 = -1.*lon0

            a=tkn[14] #latitude
            b= a.split('°')
            c=b[1].split('\'')
            d=c[1].split('"')
            lat0=float(b[0])+(float(c[0])+float(d[0])/60.)/60.
            if d[1] == 'S':
               lat0 = -1.*lat0    
               
            lat.append(lat0)
            lon.append(lon0)

        minTime = min(utc)
        maxTime = max(utc)
        minlat = min(lat)
        maxlat = max(lat)
        minlon = min(lon)
        maxlon = max(lon)

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
