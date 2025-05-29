# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmparsuconn"
provider_path = "gpmparsuconn/"
file_type = "ASCII"

#2021-2022: apu18 located at 41.808, -72.294
#2022-2023: apu27 located at  41.818, -72.258  and  apu28 located at: 41.808, -72.294
#2023-2024: Both apu27 and apu28 located at 41.808, -72.294
site_loc = {'impacts2022':{'apu18':{'lat':41.808,'lon':-72.294}},
            'impacts2023':{'apu27':{'lat':41.818,'lon':-72.258},'apu28':{'lat':41.808,'lon':-72.294}},
            'uconn2024':{'apu27':{'lat':41.808,'lon':-72.294},'apu28':{'lat':41.808,'lon':-72.294}}
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
        if 'UConn_parsivel_diameter.txt' in filename or 'UConn_parsivel_matrix.txt' in filename:
           return self.read_metadata_special()
        else: 
           return self.read_metadata_ascii(filename, file_obj_stream)

    def read_metadata_special(self):
        #Assign collection metadata to these files
        start_time = datetime(2021,12,8,21,44)
        end_time = datetime(2024,5,19,10,5)
        north,south,east,west = [41.828,41.798,-72.248,-72.304]
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }

    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        utc = []
        for encoded_line in file_obj_stream.iter_lines():
            line = encoded_line.decode("utf-8")
            tkn = line.split()
            if ":" in tkn[2] and ":" in tkn[4]:
               #Sample: 2022    2   01:57    2   20:12
               utc0 = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2]]) #i.e., 2022-002-01:57
               utc1 = '-'.join([tkn[0],tkn[3].zfill(3),tkn[4]]) #i.e., 2022-002-20:12
               utc.append(datetime.strptime(utc0,'%Y-%j-%H:%M'))
               utc.append(datetime.strptime(utc1,'%Y-%j-%H:%M'))
            else: #Sample: 2023 349  0  0  ......
               utc0 = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2].zfill(2),tkn[3].zfill(2)])#i.e.,2023-349-00-00
               utc.append(datetime.strptime(utc0,'%Y-%j-%H-%M'))
        start_time, end_time = [min(utc), max(utc)]
   
        fn = filename.split('/')[-1] #i.e., UConn_apu18_pluvio200_202204_precip_impacts2022.txt 
        period = fn.split('_')[-1].split('.')[0] #i.e., impact2022
        site = fn.split('_')[1] #i.e., apu18
        lat = site_loc[period][site]['lat']
        lon = site_loc[period][site]['lon']
        north, south, east, west = [lat+0.01,lat-0.01,lon+0.01,lon-0.01]  

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
