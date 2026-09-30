from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from ..src.helpers.creators.utils.streams import as_seekable_binary_stream
import os
from datetime import datetime, timedelta
from netCDF4 import Dataset
from pathlib import Path
import h5netcdf
import numpy as np
from pyproj import Geod

class ExtractKwajaleinMetadata(ExtractNetCDFMetadata):
    """
    A class to extract metadata from kwajalein.
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from .mat file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        gzipped = True
        if isinstance(self.file_path, Path):
            if self.file_path.suffix == ".cf":
                gzipped = False
        elif self.file_path.endswith(".cf"):
            gzipped = False
        file_buffer = as_seekable_binary_stream(self.file_path, gzipped=gzipped)

        with h5netcdf.File(file_buffer, "r") as nc:
            attrs = nc.attrs

            # Get the radar position
            longitude = nc.variables["longitude"][()]
            latitude = nc.variables["latitude"][()]

            # Get radar range
            radar_range_gates = nc["range"].shape[0]
            # First gate is 600 m; each increments by 200 m
            radar_range_max_m = 600 + (radar_range_gates - 1) * 200

            # Get the geode
            geod = Geod(ellps="WGS84")

            # Calculate the lat and lon extents
            azimuths = np.linspace(0.0, 360.0, 3600, endpoint=False)

            lons, lats, _ = geod.fwd(
                np.full_like(azimuths, longitude),
                np.full_like(azimuths, latitude),
                azimuths,
                np.full_like(azimuths, radar_range_max_m)
            )

            north = lats.max()
            south = lats.min()
            east = lons.max()
            west = lons.min()

            start = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_start"][:]).decode("ascii"))
            end = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_end"][:]).decode("ascii"))

            maxlat, minlat, maxlon, minlon = north, south, east, west
            minTime, maxTime = start, end

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.NLat, self.SLat, self.ELon, self.WLon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0,
                     offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        gzipped = True
        if isinstance(self.file_path, Path):
            if self.file_path.suffix == ".cf":
                gzipped = False
        elif self.file_path.endswith(".cf"):
            gzipped = False
        file_buffer = as_seekable_binary_stream(self.file_path, gzipped=gzipped)

        with h5netcdf.File(file_buffer, "r") as nc:
            start = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_start"][:]).decode("ascii")).strftime(date_format)
            end = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_end"][:]).decode("ascii")).strftime(date_format)
        return start, end

    def get_metadata(self, ds_short_name, format='netCDF-3', version='1', **kwargs):
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal()
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.get_checksum()
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data
