# create lookup zip for cplimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "cplimpacts"
provider_path = "cplimpacts/fieldCampaigns/impacts/CPL/data/"
file_type = "HDF-5"


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
        return self.get_nc_metadata(filename, file_obj_stream)

        if '_ATB_' in filename:
            return self.get_ATB_metadata(filename, file_obj_stream)
        else:
            return self.get_01kmPro_01kmLay_metadata(filename, file_obj_stream)


    def get_ATB_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from '*_ATB_*' HDF-5 files
        """
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lats = datafile['Latitude'][:].flatten()
        lons = datafile['Longitude'][:].flatten()
        obs_date = filename.split('/')[-1].split('_')[4][0:8] #i.e., 20220208
        ref_time = datetime(datetime.strptime(obs_date,'%Y%m%d').year-1,12,31)

        start_time, end_time = [ref_time+timedelta(days=datafile['Start_JDay'][:].item()),
                            ref_time+timedelta(days=datafile['End_JDay'][:].item())]

        """ Due to calibration on 2020-02-23, files on this date contains several invalid
           latitude values; We decide to skip these data points
        """
        north, south, east, west = [lats[np.where(lats>0)].max(),
                                    lats[np.where(lats>0)].min(),
                                    lons[np.where(lats>0)].max(),
                                    lons[np.where(lats>0)].min()]
        datafile.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }


    def get_01kmPro_01kmLay_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from *_01kmPro_* and *_01kmLay_* HDF-5 files
        """
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lats = datafile['geolocation']['CPL_Latitude'][:].flatten()
        lons = datafile['geolocation']['CPL_Longitude'][:].flatten()
        ref_time = datetime(int(datafile['metadata_parameters']['File_Year'][:])-1,12,31)

        if '_01kmPro_' in filename:
            key = 'profile'
        elif '_01kmLay_' in filename:
            key = 'layer_descriptor'

        start_time = ref_time + timedelta(datafile[key]['Profile_Decimal_Julian_Day'][:].min())
        end_time = ref_time + timedelta(datafile[key]['Profile_Decimal_Julian_Day'][:].max())
        north, south, east, west = [lats[np.where(lats>0)].max(),
                                    lats[np.where(lats>0)].min(),
                                    lons[np.where(lats>0)].max(),
                                    lons[np.where(lats>0)].min()]

        datafile.close()
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
