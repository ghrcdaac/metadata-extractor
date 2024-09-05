# create lookup zip for asosimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "asosimpacts"
provider_path = "asosimpacts/fieldCampaigns/impacts/ASOS/data/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-4"
        self.stn_info = {'k1v4': {'lat': 44.42, 'lon': -72.019}, 'kabe': {'lat': 40.649, 'lon': -75.447}, 'kack': {'lat': 41.253, 'lon': -70.06}, 'kacy': {'lat': 39.452000000000005, 'lon': -74.566}, 'kadg': {'lat': 41.867000000000004, 'lon': -84.07900000000001}, 'kafn': {'lat': 42.805, 'lon': -72.003}, 'kagc': {'lat': 40.354, 'lon': -79.921}, 'kakq': {'lat': 36.982, 'lon': -77.001}, 'kakr': {'lat': 41.037, 'lon': -81.464}, 'kalb': {'lat': 42.747, 'lon': -73.799}, 'kanj': {'lat': 46.479, 'lon': -84.357}, 'kaoh': {'lat': 40.707, 'lon': -84.027}, 'kaoo': {'lat': 40.296, 'lon': -78.32000000000001}, 'kapn': {'lat': 45.071000000000005, 'lon': -83.56400000000001}, 'kart': {'lat': 43.988, 'lon': -76.02600000000001}, 'kaug': {'lat': 44.315000000000005, 'lon': -69.79700000000001}, 'kavp': {'lat': 41.333000000000006, 'lon': -75.726}, 'kazo': {'lat': 42.234, 'lon': -85.551}, 'kbdl': {'lat': 41.937000000000005, 'lon': -72.68100000000001}, 'kbed': {'lat': 42.47, 'lon': -71.289}, 'kbeh': {'lat': 42.125, 'lon': -86.42800000000001}, 'kbfd': {'lat': 41.800000000000004, 'lon': -78.63300000000001}, 'kbgm': {'lat': 42.206, 'lon': -75.98}, 'kbgr': {'lat': 44.797000000000004, 'lon': -68.818}, 'kbiv': {'lat': 42.746, 'lon': -86.096}, 'kbjj': {'lat': 40.873000000000005, 'lon': -81.88600000000001}, 'kbkw': {'lat': 37.783, 'lon': -81.123}, 'kblf': {'lat': 37.297000000000004, 'lon': -81.203}, 'kbmg': {'lat': 39.133, 'lon': -86.616}, 'kbml': {'lat': 44.576, 'lon': -71.17800000000001}, 'kbos': {'lat': 42.36, 'lon': -71.009}, 'kbtl': {'lat': 42.307, 'lon': -85.251}, 'kbtv': {'lat': 44.468, 'lon': -73.149}, 'kbuf': {'lat': 42.940000000000005, 'lon': -78.735}, 'kbwg': {'lat': 36.964000000000006, 'lon': -86.423}, 'kbwi': {'lat': 39.173, 'lon': -76.68400000000001}, 'kcar': {'lat': 46.870000000000005, 'lon': -68.01700000000001}, 'kcho': {'lat': 38.137, 'lon': -78.455}, 'kckb': {'lat': 39.295, 'lon': -80.22800000000001}, 'kcle': {'lat': 41.405, 'lon': -81.852}, 'kcmh': {'lat': 39.99, 'lon': -82.87700000000001}, 'kcmx': {'lat': 47.168, 'lon': -88.488}, 'kcon': {'lat': 43.204, 'lon': -71.50200000000001}, 'kcrw': {'lat': 38.379000000000005, 'lon': -81.59}, 'kdan': {'lat': 36.572, 'lon': -79.33500000000001}, 'kdaw': {'lat': 43.278000000000006, 'lon': -70.92200000000001}, 'kday': {'lat': 39.906, 'lon': -84.218}, 'kdca': {'lat': 38.847, 'lon': -77.034}, 'kdet': {'lat': 42.409, 'lon': -83.01}, 'kdfi': {'lat': 41.337, 'lon': -84.42800000000001}, 'kdkk': {'lat': 42.493, 'lon': -79.272}, 'kdsv': {'lat': 42.57, 'lon': -77.71300000000001}, 'kduj': {'lat': 41.179, 'lon': -78.893}, 'kdxr': {'lat': 41.371, 'lon': -73.482}, 'kdyl': {'lat': 40.330000000000005, 'lon': -75.122}, 'kekn': {'lat': 38.885000000000005, 'lon': -79.852}, 'kelm': {'lat': 42.159, 'lon': -76.891}, 'kelz': {'lat': 42.109, 'lon': -77.991}, 'keri': {'lat': 42.080000000000005, 'lon': -80.182}, 'kevv': {'lat': 38.044000000000004, 'lon': -87.52000000000001}, 'kewr': {'lat': 40.682, 'lon': -74.16900000000001}, 'kfdy': {'lat': 41.013000000000005, 'lon': -83.668}, 'kfft': {'lat': 38.184000000000005, 'lon': -84.903}, 'kfig': {'lat': 41.046, 'lon': -78.411}, 'kfit': {'lat': 42.551, 'lon': -71.75500000000001}, 'kfnt': {'lat': 42.966, 'lon': -83.74900000000001}, 'kfrg': {'lat': 40.734, 'lon': -73.41600000000001}, 'kfve': {'lat': 47.285000000000004, 'lon': -68.313}, 'kfwa': {'lat': 40.97, 'lon': -85.206}, 'kfzy': {'lat': 43.349000000000004, 'lon': -76.384}, 'kged': {'lat': 38.689, 'lon': -75.36200000000001}, 'kgez': {'lat': 39.578, 'lon': -85.80300000000001}, 'kgfl': {'lat': 43.338, 'lon': -73.61}, 'kgkj': {'lat': 41.626000000000005, 'lon': -80.215}, 'kglr': {'lat': 45.013000000000005, 'lon': -84.70100000000001}, 'kgnr': {'lat': 45.462, 'lon': -69.595}, 'kgrr': {'lat': 42.882000000000005, 'lon': -85.52300000000001}, 'kgsh': {'lat': 41.533, 'lon': -85.783}, 'khao': {'lat': 39.364000000000004, 'lon': -84.524}, 'khgr': {'lat': 39.706, 'lon': -77.73}, 'khie': {'lat': 44.367000000000004, 'lon': -71.545}, 'khlg': {'lat': 40.176, 'lon': -80.647}, 'khpn': {'lat': 41.066, 'lon': -73.70700000000001}, 'khtl': {'lat': 44.359, 'lon': -84.673}, 'khts': {'lat': 38.365, 'lon': -82.554}, 'khuf': {'lat': 39.451, 'lon': -87.308}, 'khul': {'lat': 46.118, 'lon': -67.792}, 'khzy': {'lat': 41.778000000000006, 'lon': -80.69500000000001}, 'kiad': {'lat': 38.934000000000005, 'lon': -77.447}, 'kiag': {'lat': 43.108000000000004, 'lon': -78.938}, 'kijd': {'lat': 41.741, 'lon': -72.183}, 'kilg': {'lat': 39.674, 'lon': -75.605}, 'kiln': {'lat': 39.43, 'lon': -83.77600000000001}, 'kimt': {'lat': 45.818000000000005, 'lon': -88.114}, 'kind': {'lat': 39.725, 'lon': -86.281}, 'kipt': {'lat': 41.243, 'lon': -76.921}, 'kiwi': {'lat': 43.963, 'lon': -69.711}, 'kisp': {'lat': 40.793, 'lon': -73.101}, 'kizg': {'lat': 43.99, 'lon': -70.947}, 'kjfk': {'lat': 40.638000000000005, 'lon': -73.762}, 'kjkl': {'lat': 37.591, 'lon': -83.31400000000001}, 'kjst': {'lat': 40.316, 'lon': -78.833}, 'kjxn': {'lat': 42.266000000000005, 'lon': -84.46600000000001}, 'klaf': {'lat': 40.412, 'lon': -86.936}, 'klan': {'lat': 42.776, 'lon': -84.599}, 'kleb': {'lat': 43.626000000000005, 'lon': -72.304}, 'klex': {'lat': 38.04, 'lon': -84.605}, 'klga': {'lat': 40.779, 'lon': -73.88000000000001}, 'klhq': {'lat': 39.755, 'lon': -82.65700000000001}, 'klns': {'lat': 40.120000000000005, 'lon': -76.29400000000001}, 'kloz': {'lat': 37.087, 'lon': -84.07600000000001}, 'klpr': {'lat': 41.346000000000004, 'lon': -82.179}, 'klyh': {'lat': 37.32, 'lon': -79.206}, 'kmbs': {'lat': 43.533, 'lon': -84.07900000000001}, 'kmdt': {'lat': 40.196000000000005, 'lon': -76.772}, 'kmfd': {'lat': 40.82, 'lon': -82.51700000000001}, 'kmgj': {'lat': 41.509, 'lon': -74.265}, 'kmgw': {'lat': 39.642, 'lon': -79.91600000000001}, 'kmgy': {'lat': 39.593, 'lon': -84.226}, 'kmht': {'lat': 42.932, 'lon': -71.435}, 'kmie': {'lat': 40.234, 'lon': -85.393}, 'kmiv': {'lat': 39.366, 'lon': -75.077}, 'kmkg': {'lat': 43.171, 'lon': -86.236}, 'kmlt': {'lat': 45.647000000000006, 'lon': -68.69200000000001}, 'kmmk': {'lat': 41.509, 'lon': -72.827}, 'kmnn': {'lat': 40.616, 'lon': -83.063}, 'kmpv': {'lat': 44.203, 'lon': -72.56200000000001}, 'kmrb': {'lat': 39.403000000000006, 'lon': -77.94500000000001}, 'kmss': {'lat': 44.935, 'lon': -74.845}, 'kmtp': {'lat': 41.073, 'lon': -71.923}, 'kmvl': {'lat': 44.534, 'lon': -72.614}, 'kmvy': {'lat': 41.393, 'lon': -70.61500000000001}, 'kore': {'lat': 42.57, 'lon': -72.29100000000001}, 'korf': {'lat': 36.903000000000006, 'lon': -76.19200000000001}, 'korh': {'lat': 42.27, 'lon': -71.873}, 'koxb': {'lat': 38.308, 'lon': -75.123}, 'kp58': {'lat': 44.021, 'lon': -82.793}, 'kp59': {'lat': 47.466, 'lon': -87.88300000000001}, 'kpah': {'lat': 37.056000000000004, 'lon': -88.774}, 'kpbg': {'lat': 44.650000000000006, 'lon': -73.46600000000001}, 'kpeo': {'lat': 42.642, 'lon': -77.05600000000001}, 'kphd': {'lat': 40.471000000000004, 'lon': -81.423}, 'kphf': {'lat': 37.131, 'lon': -76.49300000000001}, 'kphl': {'lat': 39.873000000000005, 'lon': -75.226}, 'kpia': {'lat': 40.664, 'lon': -89.693}, 'kpit': {'lat': 40.484, 'lon': -80.214}, 'kpkb': {'lat': 39.339000000000006, 'lon': -81.443}, 'kpln': {'lat': 45.564, 'lon': -84.792}, 'kpou': {'lat': 41.625, 'lon': -73.881}, 'kpsf': {'lat': 42.426, 'lon': -73.289}, 'kptk': {'lat': 42.665, 'lon': -83.418}, 'kptw': {'lat': 40.238, 'lon': -75.554}, 'kpwm': {'lat': 43.642, 'lon': -70.304}, 'krdg': {'lat': 40.373000000000005, 'lon': -75.959}, 'kric': {'lat': 37.511, 'lon': -77.32300000000001}, 'krme': {'lat': 43.233000000000004, 'lon': -75.411}, 'kroa': {'lat': 37.316, 'lon': -79.974}, 'kroc': {'lat': 43.116, 'lon': -77.676}, 'ksbn': {'lat': 41.707, 'lon': -86.316}, 'ksby': {'lat': 38.34, 'lon': -75.513}, 'ksdf': {'lat': 38.181000000000004, 'lon': -85.739}, 'kseg': {'lat': 40.82, 'lon': -76.864}, 'kslk': {'lat': 44.385000000000005, 'lon': -74.206}, 'ksmq': {'lat': 40.624, 'lon': -74.66900000000001}, 'ksyr': {'lat': 43.111000000000004, 'lon': -76.10300000000001}, 'ktdz': {'lat': 41.563, 'lon': -83.476}, 'kthv': {'lat': 39.918, 'lon': -76.87400000000001}, 'ktol': {'lat': 41.587, 'lon': -83.805}, 'kttn': {'lat': 40.276, 'lon': -74.815}, 'ktvc': {'lat': 44.74, 'lon': -85.58200000000001}, 'kvpz': {'lat': 41.452000000000005, 'lon': -87.00500000000001}, 'kvsf': {'lat': 43.343, 'lon': -72.51700000000001}, 'kvta': {'lat': 40.022000000000006, 'lon': -82.462}, 'kwal': {'lat': 37.937000000000005, 'lon': -75.46600000000001}, 'kyng': {'lat': 41.254000000000005, 'lon': -80.673}, 'kzzv': {'lat': 39.944, 'lon': -81.89200000000001}}

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if filename.endswith('.nc'):
           self.file_type = "netCDF-4"
           return self.get_nc_metadata(filename, file_obj_stream)
        else:
           self.file_type = "CSV"
           return self.get_csv_metadata(filename, file_obj_stream)

    def get_csv_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from CSV files
        """
        print(filename)
        #Initializing 2 parameters
        minTime, maxTime = [datetime(2100, 1, 1),datetime(1900, 1, 1)]

        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        for ii in range(1,len(file_lines)): #skip the first line
            line = file_lines[ii]
            cTime = datetime.strptime(line.split(",")[0], '%Y-%m-%d %H:%M:%S')
            minTime = min(minTime, cTime)
            maxTime = max(maxTime, cTime)


        stn_name = filename.split('.csv')[0].split('_')[3]
        lat = self.stn_info[stn_name]['lat']
        lon = self.stn_info[stn_name]['lon']
        north, south, east, west  = [round(lat+0.001, 3), round(lat-0.001, 3),
                                     round(lon+0.001, 3), round(lon-0.001, 3)]
        
        return {
            "start": minTime,
            "end": maxTime,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
        }


    def get_nc_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        print(filename)
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = float(nc.getncattr('site_latitude'))
        lon = float(nc.getncattr('site_longitude'))
        sec = nc['time'][:]
        ref_ymd = nc.variables['time'].getncattr('long_name').split()[2]
        start_time = datetime.strptime(ref_ymd, "%Y-%m-%d") + timedelta(seconds=float(min(sec)))
        end_time = datetime.strptime(ref_ymd, "%Y-%m-%d") + timedelta(seconds=float(max(sec)))
        north, south, east, west  = [round(lat+0.001, 3), round(lat-0.001, 3),
                                     round(lon+0.001, 3), round(lon-0.001, 3)]

        nc.close()
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
