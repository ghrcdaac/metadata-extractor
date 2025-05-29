# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmpluviouconn"
provider_path = "gpmpluviouconn/"
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
        return self.read_metadata_ascii(filename, file_obj_stream)


    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        #print(filename)
        #Sample file name: UConn_apu18_pluvio200_raintotal_impact2022.txt
        utc = []
        for encoded_line in file_obj_stream.iter_lines():
            line = encoded_line.decode("utf-8")
            tkn = line.split()
            if ":" in tkn[2] and ":" in tkn[4]:
               #Sample: 2022    2   01:57    2   20:12
               if tkn[1] in ['0','00','000']:
                  tkn[1] = '001'
                  utc0_str = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2]]) #i.e., 2022-002-01:57
                  utc0 = datetime.strptime(utc0_str,'%Y-%j-%H:%M')-timedelta(hours=24)
               else:
                  utc0_str = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2]]) #i.e., 2022-002-01:57
                  utc0 = datetime.strptime(utc0_str,'%Y-%j-%H:%M')

               if tkn[3] in ['0','00','000']:
                  tkn[3] = '001'
                  utc1_str = '-'.join([tkn[0],tkn[3].zfill(3),tkn[4]]) #i.e., 2022-002-01:57
                  utc1 = datetime.strptime(utc1_str,'%Y-%j-%H:%M')-timedelta(hours=24)
               else:
                  utc1_str = '-'.join([tkn[0],tkn[3].zfill(3),tkn[4]]) #i.e., 2022-002-01:57
                  utc1 = datetime.strptime(utc1_str,'%Y-%j-%H:%M')

               utc.append(utc0)
               utc.append(utc1)
            else: #Sample: 2023 349  0  0  ......
               if tkn[1] in ['0','00','000']: #i.e., 2022,000,0,0
                  tkn[1] = '001'
                  utc0_str = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2].zfill(2),tkn[3].zfill(2)])#i.e.,2023-001-00-00
                  utc0 = datetime.strptime(utc0_str,'%Y-%j-%H-%M') - timedelta(hours=24)
               else:   
                  utc0_str = '-'.join([tkn[0],tkn[1].zfill(3),tkn[2].zfill(2),tkn[3].zfill(2)])#i.e.,2023-349-00-00
                  utc0 = datetime.strptime(utc0_str,'%Y-%j-%H-%M')
               utc.append(utc0)

        start_time, end_time = [min(utc), max(utc)]
    
        fn = filename.split('/')[-1] 
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
