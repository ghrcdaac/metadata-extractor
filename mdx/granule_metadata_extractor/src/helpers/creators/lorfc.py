# create lookup zip for lorfc (LIS/OTD reprocessed flash climatology) 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import re

short_name = "lorfc"
provider_path = "lis_climatologies_2025/"

#OTD: (1995 Apr 13 – 2000 Mar 23)
#TRMM LIS: (1998 Jan 01 – 2015 Apr 8)
#ISS LIS: (2017 Mar 01 – 2023 Nov 16)
obs_period = {'OTD':['19950413','20000323'],
              'TRMM':['19980101','20150408'],
              'ISS':['20170301','20231116'],
              'COMB_OTD_TRMM_ISS':['19950413','20231116'],
              'COMB_TRMM_ISS':['19980101','20231116']
             }
   
class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "HDF-4"

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.get_hdf4_metadata(filename)


    def get_hdf4_metadata(self, filename):
        for key in obs_period.keys():
            if filename.startswith(key):
               start_time = datetime.strptime(obs_period[key][0],'%Y%m%d')
               end_time = datetime.strptime(obs_period[key][1],'%Y%m%d')
               end_time = end_time + timedelta(seconds=86399) 
            north,south,east,west=[90.,-90.,180.,-180.]
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
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
