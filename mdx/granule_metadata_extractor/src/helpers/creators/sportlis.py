# create lookup zip for sportlis 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

import json

short_name = "sportlis"
#provider_path = "sportlis/climatologies/gridded/"#sportlis_SM_0_40cm_CLIMO_1981_2013_2013364.dat
#provider_path = "sportlis/climatologies/county/" #sportlis_Yuma_County_CO_percentileSoil_1230.out
#provider_path = "sportlis/percentiles/" #sportlis_percentile_2023123112.grb2 or
                                         #sportlis_vsm_percentile_20231230.grb2
#provider_path = "sportlis/rawoutput/" #sportlis_RST_NOAH33_202212010000_d01.nc or
                                      #sportlis_HIST_202212300300_d01.grb
class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-4"
        with open('../sportlis_county_bounding_box.json','r') as fp:
            self.county_latlon = json.load(fp)

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.get_sportlis_metadata(filename)


    def get_sportlis_metadata(self, filename):
        #assign bounding box
        north, south, east, west = [52.915,25.075,-67.085,-124.925]
        #extract time info from file name
        if filename.endswith('.dat'): #sportlis_SM_0_40cm_CLIMO_1981_2013_2013364.dat
           self.file_type = "Binary"
           start_time = datetime.strptime(filename.split('.dat')[0].split('_')[-1],'%Y%j') 
           end_time = start_time + timedelta(seconds=86399)
        elif filename.endswith('.out'): #sportlis_Yuma_County_CO_percentileSoil_1230.out
           self.file_type = "ASCII"
           countyName = '_'.join(filename.split('_')[1:-2]) #i.e., Yuma_County_CO

           utc_date = filename.split('_')[-1].split('.out')[0] #i.e., 1230
           north = self.county_latlon[countyName]['north']
           south = self.county_latlon[countyName]['south']
           east = self.county_latlon[countyName]['east']
           west = self.county_latlon[countyName]['west']
           start_time = datetime.strptime('1981'+utc_date,'%Y%m%d')
           end_time = datetime.strptime('2013'+utc_date+'235959','%Y%m%d%H%M%S')
        elif filename.endswith('.grb2'):
           self.file_type = "GRIB2"
           utc_str = filename.split('.grb2')[0].split('_')[-1]
           if len(utc_str) == 10: #i.e., sportlis_percentile_2023123112.grb2
              start_time = datetime.strptime(utc_str,'%Y%m%d%H')
           else: #i.e., sportlis_vsm_percentile_20231230.grb2
              start_time = datetime.strptime(utc_str,'%Y%m%d')
           end_time = start_time                    
        elif filename.endswith('grb'):#i.e., sportlis_HIST_202212300300_d01.grb
           self.file_type = "GRIB1"
           utc_str = filename.split('_')[-2] 
           start_time = datetime.strptime(utc_str,'%Y%m%d%H%M')
           end_time = start_time

        elif filename.endswith('.nc'): #i.e., sportlis_RST_NOAH33_202212010000_d01.nc
           self.file_type = "netCDF-4"
           utc_str = filename.split('_')[-2] #i.e., 202212010000
           start_time = datetime.strptime(utc_str,'%Y%m%dYHYM')
           end_time = start_time + timedelta(seconds = 86399)

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
