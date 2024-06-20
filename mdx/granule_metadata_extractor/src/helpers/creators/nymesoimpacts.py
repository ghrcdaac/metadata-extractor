# create lookup zip for nymesoimpacts images
# for all future collections
from datetime import datetime
from utils.mdx import MDX
import cProfile
import time
import math
import re

import json
import os
import pathlib
from zipfile import ZipFile

short_name = "nymesoimpacts"
provider_path = "nymesoimpacts/fieldCampaigns/impacts/NY_Mesonet/browse/20200103/"
file_type = "PNG"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

        #Get lookup dataset's metadata attributes from lookup zip
        self.nysm_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                         f"../meta_nysm_latlon.csv")
        self.prof_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                         f"../meta_prof_latlon.csv")
        [self.nysm, self.prof] = self.get_site_lat_lon()

    def get_site_lat_lon(self):
        """
        :return:
        """
        nysm = {}
        prof = {}
        #read meta_nysm_latlon.csv
        with open(self.nysm_path,'r') as fp:
             lines = fp.readlines()
        for i in range(2,len(lines)):
            #Sample line:
            #WBOU,94,Woodbourne,41.7451,-74.5883,432.999,Woodbourne,BGM
            if not lines[i].startswith(',,,'):
               tkn = lines[i].split(',')
               nysm[tkn[0].lower()] = {'lat':float(tkn[3]),'lon':float(tkn[4])}

        #read meta_prof_latlon.csv
        with open(self.prof_path,'r') as fp:
             lines = fp.readlines()
        for line in lines:
            if 'PROF_' in line:
               tkn = line.split(',')
               prof[tkn[0].split('_')[1].lower()] = {'lat':float(tkn[3]),'lon':float(tkn[4])}
        return nysm, prof


    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.get_png_metadata(filename)

    def get_png_metadata(self, filename):
        """
        Extract temporal and spatial metadata for image files
        """
        fn=filename.split('/')[-1]
        #Sample file
        #IMPACTS_NYS_mwr_cloud_202001032300_redh.png
        tkn = fn.split('.')[0].split('_')
        minTime = datetime.strptime(tkn[-2],'%Y%m%d%H%M')
        maxTime = minTime + timedelta(hours=24)

        if 'lidar' in tkn:
           stn_lat = self.prof[tkn[-1]]['lat']
           stn_lon = self.prof[tkn[-1]]['lon']

        else: #'mwr' in tkn or 'ground' in tkn (MESONET ground observation)
           if tkn[-1] in self.nysm.keys():
              stn_lat = self.nysm[tkn[-1]]['lat']
              stn_lon = self.nysm[tkn[-1]]['lon']
           else:
              stn_lat = self.prof[tkn[-1]]['lat']
              stn_lon = self.prof[tkn[-1]]['lon']

        maxlat = stn_lat+0.01
        minlat = stn_lat-0.01
        maxlon = stn_lon+0.01
        minlon = stn_lon-0.01

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
