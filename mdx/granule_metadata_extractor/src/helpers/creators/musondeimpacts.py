# create lookup zip for musondeimpacts
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "musondeimpacts"
provider_path = "musondeimpacts/fieldCampaigns/impacts/MU_sondes/data/"
file_type = "HDF-5"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.fileformat = 'ASCII'

        self.utf8_list = ['IMPACTS_upperair_UMILL_radiosonde_202201291800_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202201292000_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202201292200_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202202191500_QC.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202202191800_QC.txt']

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if filename.endswith('.cdf'): #HDF-5
            file_type = 'HDF-5'
            return self.get_hdf_metadata(filename, file_obj_stream)
        else: #ASCII
            file_type = 'ASCII'
            return self.get_ascii_metadata(filename, file_obj_stream)


    def get_hdf_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from HDF-5 files
        """
        datafile = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        if '_windsonde_' in filename:
            lat = float(datafile.latitude)
            lon = float(datafile.longitude)
            north, south, east, west = [lat+0.01, lat-0.01, lon+0.01, lon-0.01]
            tkn = filename.split('.cdf')[0].split('_')
            start_time = datetime.strptime(tkn[-2]+tkn[-1],'%Y%m%d%H%M%S')
            end_time = start_time
        else:
            lats = datafile['lat'][:].flatten()
            lons = datafile['lon'][:].flatten()
            sec = datafile['time'][:].flatten()
            ref_time_str = datafile['time'].units #'seconds since 2023-01-25 20:16:35'
            ref_time = datetime.strptime(ref_time_str, 'seconds since %Y-%m-%d %H:%M:%S')

            start_time, end_time = [ref_time+timedelta(seconds=sec.min().item()),
                                    ref_time+timedelta(seconds=sec.max().item())]

            north, south, east, west = [lats.max(),
                                        lats.min(),
                                        lons.max(),
                                        lons.min()]
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


    def get_ascii_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        file_lines = []
        fn = filename.split('/')[-1]
        if '_windsonde1_' in fn: #wind sonde file
           #sample file:
           #IMPACTS_upperair_UMILL_windsonde1_202201162100_QCTeare.txt
           for encoded_line in file_obj_stream.iter_lines():
               file_lines.append(encoded_line.decode("utf-8"))

           for line in file_lines:
               line = line.strip() # remove all the leading and trailing spaces from a string
               if line.startswith('XXX '):
                  start_time_str = '20'+line.split()[-1] #i.e., 220116/1958
                  minTime = datetime.strptime(start_time_str,'%Y%m%d/%H%M')
               elif line.startswith('Site'):
                  tkn = line.split()
                  lat0 = float(tkn[1].split(',')[0].split('=')[-1])
                  lon0 = float(tkn[2].split('=')[-1])
                  maxlat, minlat, maxlon, minlon = [lat0+0.01,
                                                    lat0-0.01,
                                                    lon0+0.01,
                                                    lon0-0.01]
               elif line.startswith('Saved by user: '):
                  maxTime = datetime.strptime(line,'Saved by user: User on %Y%m%d/%H%M UTC')
                  break
        else: #radio sonde file, either utf-8 or utf-16-be (big endian)
           print('fn=',fn)
           endian_type = 'utf-16-be'
           if fn in self.utf8_list:
              endian_type = 'utf-8'

           #read lines and save into 'file_lines' list 
           for encoded_line in file_obj_stream.iter_lines():
               line = encoded_line.decode(endian_type,errors='ignore').strip()
               file_lines.append(line)
          
           count = 0 #account number of header lines for later use
           for line in file_lines:
               count = count + 1
               if line.startswith('Balloon release date and time'):
                  minTime = datetime.strptime(line.split()[-1],'%Y-%m-%dT%H:%M:%S') #i.e.,2022-01-29T13:07:23
               elif line.startswith('s hh:mm:ss'):
                  num_header_lines = count
                  break

           elap_sec = []
           lat = []
           lon = []
           for line in file_lines[num_header_lines:]:
               if len(line) < 20 or 'row' in line:
                  continue
               tkn = line.split()
               elap_sec.append(float(tkn[1]))
               lat.append(float(tkn[-2]))
               lon.append(float(tkn[-1]))
           maxTime = minTime + timedelta(seconds = max(elap_sec))
           maxlat, minlat, maxlon, minlon = [max(lat),min(lat),max(lon),min(lon)]

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
