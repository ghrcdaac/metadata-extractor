# Create lookup table for SHIS WHyMSIE collection
from datetime import datetime, timedelta
import re
from netCDF4 import Dataset
import numpy as np

from utils.mdx import MDX

short_name = "shiswhymsie"
provider_path = "shiswhymsie/"

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
        if re.search(r'whymsie_shis_rtv_ER2_.*\.h5$', filename):
            file_type = "HDF-5"
            return self.get_rtv_metadata(file_obj_stream, file_type)
        # elif re.search(r'whymsie_shis_rad_ER2_.*\.nc$', filename):
        else:
            file_type = "netCDF-3"
            return self.get_rad_metadata(file_obj_stream, file_type)


    def get_rtv_metadata(self, file_obj_stream, file_type) -> dict:
        """
        Extract temporal and spatial metadata from RTV files
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        :param file_type: type of file to process
        :type file_type: str
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        
        lat = nc['Latitude'][:]
        lon = nc['Longitude'][:]
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        dd = [datetime.strptime(str(int(d)), "%y%m%d").date() for d in nc['dateYYMMDD'][:]]
        tt = [datetime.strptime(str(int(t)).zfill(6), "%H%M%S").time() for t in nc['timeHHMMSS'][:]]
        timestamps = [datetime.combine(d, t) for d, t in zip(dd, tt)]
        start_time = min(timestamps)
        end_time = max(timestamps)

        nc.close()

        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
        }



    def get_rad_metadata(self, file_obj_stream, file_type) -> dict:
        """
        Extract temporal and spatial metadata from RAD files
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        :param file_type: type of file to process
        :type file_type: str
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
                
        lat = nc['Latitude'][:]
        lon = nc['Longitude'][:]
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        dd = np.array([datetime.strptime(str(int(d)), "%y%m%d") for d in nc['date'][:]])
        tt = nc['timeUTC'][:]
        t_mask = ~np.isnan(nc['date'][:]) & ~np.isnan(nc['timeUTC'][:])
        ts = [d + timedelta(hours=t.item()) for d, t in zip(dd[t_mask], tt[t_mask])]
        start_time = min(ts)
        end_time = max(ts)

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
        self.process_collection(short_name, provider_path, max_concurrent=5)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()