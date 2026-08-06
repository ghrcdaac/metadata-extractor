# Create lookup table for SHIS WHyMSIE collection
import datetime as dt
import numpy as np
from netCDF4 import Dataset

from utils.mdx import MDX

short_name = "cossirv2impacts"
provider_path = "cossirimpacts/"
file_type = "netCDF-4"

class MDXProcessing(MDX):
    
    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction.
        Note: The netCDF4 library reads the missing value (-999) as a string which 
        causes type casting warnings, so automasking is manually disabled to avoid these.
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        nc.set_auto_mask(False)

        lat = np.array(nc['Latitude'][:])
        lon = np.array(nc['Longitude'][:])
        yy = np.array(nc['Year'][:])
        mm = np.array(nc['Month'][:])
        dd = np.array(nc['DayOfMonth'][:])
        hh = np.array(nc['Hour'][:])
        mi = np.array(nc['Minute'][:])
        ss = np.array(nc['Second'][:])
        mask = (lat != -999) & (lon != -999) & (yy != -999) & (mm != -999) & (dd != -999) \
            & (hh != -999) & (mi != -999) & (ss != -999)

        north, south, east, west = [np.nanmax(lat[mask]), np.nanmin(lat[mask]),
                                    np.nanmax(lon[mask]), np.nanmin(lon[mask])]

        timestamps = (
            (yy[mask] - 1970).astype('datetime64[Y]') +
            (mm[mask] - 1).astype('timedelta64[M]') +
            (dd[mask] - 1).astype('timedelta64[D]') +
            hh[mask].astype('timedelta64[h]') +
            mi[mask].astype('timedelta64[m]') +
            ss[mask].astype('timedelta64[s]')
        )
        start_time = (np.min(timestamps)).astype(dt.datetime)
        end_time = (np.max(timestamps)).astype(dt.datetime)

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

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()