from datetime import datetime, timedelta, timezone
import os
import re
import json

from ..src.extract_ascii_metadata import ExtractASCIIMetadata


class ExtractMalmaRawMetadata(ExtractASCIIMetadata):
    """
    Processing class for MALMA raw collection. 
    Metadata for raw LMA data is extracted from file names instead of the binary files.
    """
    start_time = datetime.max.replace(tzinfo=timezone.utc)
    end_time = datetime.min.replace(tzinfo=timezone.utc)
    north, south, east, west = [float('-inf'), float('inf'), float('-inf'), float('inf')]


    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        with open(os.path.join(os.path.dirname(__file__), "../src/helpers/malmarawRefData.json"), 'r') as f:
            self.station_dict = json.load(f)
        self.get_variables_min_max()


    def get_variables_min_max(self):
        """
        Extracts temporal and spatial bounds of granule via filename and station lookup
        """
        match = re.search(r"L([A-Z])_([A-Z]+)_.*_(\d{6})_(\d{6})", self.file_name)
        site_id = match.group(1)
        array_name = match.group(2)
        date_str = match.group(3)
        time_str = match.group(4)

        # MALMA files cover 10 minute intervals
        timestamp = datetime.strptime(f"{date_str}_{time_str}", "%y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        self.start_time = min(timestamp, self.start_time)
        self.end_time = max(self.start_time + timedelta(seconds=599), self.end_time)

        # LMA sites are technically point data, so a delta is used to create small bounding box
        if array_name in self.station_dict and site_id in self.station_dict[array_name]:
            site_locs = self.station_dict[array_name][site_id]
            time_float = float(f"20{date_str}.{time_str}")
            for loc in site_locs:
                if loc["start"] <= time_float <= loc["end"]:
                    lat, lon = [loc["lat"] , loc["lon"] ]
                    break
            if not lat:
                raise ValueError(f'Granule {self.file_name} does not correspond to a valid station and date combination.')
        else:
            raise ValueError(f'Station {site_id} from LMA array {array_name} not found in station lookup file.')

        self.north = lat + 0.001
        self.south = lat - 0.001
        self.east = lon + 0.001
        self.west = lon - 0.001


    def get_wnes_geometry(self, scale_factor=1.0, offset=0) -> tuple[float]:
        """
        Returns bounding box coordinates
        :param scale_factor In case it is not CF compliant we will need scale factor
        :param offset data offset if the netCDF not CF compliant
        :return list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.north, self.south, self.east, self.west]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]


    def get_temporal(self, date_format='%Y-%m-%dT%H:%M:%SZ') -> tuple[str, str]:
        """
        Returns temporal bounds as formatted strings
        :param date_format datetime format string 
        :return tuple of formatted start and end timestamp strings
        """
        return self.start_time.strftime(date_format), self.end_time.strftime(date_format)


    def get_metadata(self, ds_short_name, format='Binary', version='1') -> dict:
        """
        Creates granule metadata dict.
        :param ds_short_name Collection shortname
        :param format Granule file format
        :param version Collection version 
        :return Granule metadata dict
        """
        md = {}
        md['ShortName'] = ds_short_name
        md['GranuleUR'] = self.file_name

        start, end = self.get_temporal()
        md['BeginningDateTime'], md['EndingDateTime'] = start, end

        bounds = self.get_wnes_geometry()
        md['WestBoundingCoordinate'], md['NorthBoundingCoordinate'], \
        md['EastBoundingCoordinate'], md['SouthBoundingCoordinate'] = list(str(x) for x in bounds)

        md['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        md['checksum'] = self.get_checksum()
        md['DataFormat'] = format
        md['VersionId'] = version
        
        return md
