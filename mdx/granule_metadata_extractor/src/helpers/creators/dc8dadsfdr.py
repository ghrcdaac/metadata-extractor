"""Lookup creator for DC-8 DADS FDR (dc8dadsfdr)."""
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
from utils.streams import as_text_stream
import re
from datetime import datetime, timedelta

short_name = "dc8dadsfdr"
provider_path = "dc8dadsfdr/"

# For mission director logs, use same bounds as aligned flight data recorder logs
date_lookup = {
    'CAMEX3_DC8FDR_98215_md_log.txt':
        {"start": "1998-08-03T18:23:02Z",
         "end": "1998-08-03T22:02:58Z",
         "north": 36.17,
         "south": 29.485,
         "east": -116.425,
         "west": -120.22},
    'CAMEX3_DC8FDR_98218_md_log.txt':
        {"start": "1998-08-06T17:02:41Z",
         "end": "1998-08-06T23:18:48Z",
         "north": 36.2,
         "south": 27.467,
         "east": -117.115,
         "west": -122.977},
    'CAMEX3_DC8FDR_98222_md_log.txt':
        {"start": "1998-08-10T15:16:50Z",
         "end": "1998-08-10T22:28:53Z",
         "north": 36.018,
         "south": 27.403,
         "east": -78.745,
         "west": -118.215},
}

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
        self.file_type = 'ASCII'
        return self.read_metadata_ascii(filename, file_obj_stream)

    @staticmethod
    def dmm_to_decimal(degrees: float, minutes: float) -> float:
        """Convert degrees, minutes to decimal degrees."""
        sign = -1 if degrees < 0 else 1
        return sign * (abs(degrees) + minutes / 60.0)

    @staticmethod
    def parse_date_from_filename(filename: str) -> datetime|None:
        """Parse date from filename."""
        if m := re.match(r'^.*_(\d{2})(\d{3})_(?:flt|md_log)\.txt$', filename):
            year, day_of_year = m.groups()
            return datetime(1900 + int(year), 1, 1) + timedelta(days=int(day_of_year) - 1)
        return None

    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        # Field definitions (from dc8dads_dataset.pdf):
        # C day, UTC, lat, lon, pitch, roll, wind speed
        # D wind direction, true airspeed, ground speed,
        #   true heading, drift angle, pressure altitude,
        #   radar altitude, two dew/frost-point temperatures
        # E static air temp, total air temp, IR surface temp,
        #   calculated static air temp, indicated airspeed,
        #   vertical speed, distance-to-go, time-to-go, alignment status
        # F cabin altitude, pressure, Mach, cross-track distance,
        #   desired track, track-angle error, track angle, specific humidity
        # G water-vapor partial pressure, RH wrt ice, RH wrt water,
        #   saturation vapor pressures, several solar elevation/azimuth quantities
        # H GPS UTC, GPS lat/lon, GPS altitude, GPS vertical speed, spare A/D
        # I solar angles, waypoint lat/lon, potential temperature, specific humidity

        if filename.endswith('_md_log.txt'):
            # May not need to actually parse this file
            if filename in date_lookup:
                metadata = date_lookup[filename]
                for field in ['start', 'end']:
                    metadata[field] = datetime.fromisoformat(metadata[field])
            else: return {}

        file_date = self.parse_date_from_filename(filename)
        if file_date is None:
            raise ValueError(f"Failed to parse date from filename {filename}")
        year = file_date.year
        file_buffer = as_text_stream(file_obj_stream)

        minTime = datetime.max
        maxTime = datetime.min
        north = -90.0
        south = 90.0
        east = -180.0
        west = 180.0
        for line in file_buffer:
            if line.startswith('C'):
                # Has day, UTC, lat, lon
                fields = line.split()
                # Fields are described as 'day', 'UTC', 'lat', 'lon',
                #   'pitch', 'roll', 'wind speed', but in fact lat and lon
                #   are two fields each, degrees & minutes
                (line_label, day, utc, lat_deg, lat_min, lon_deg,
                 lon_min, pitch, roll, wind_speed) = fields
            elif line.startswith('H'):
                # Has GPS lat/lon
                # Fields are described as 'GPS UTC', 'GPS lat', 'GPS lon',
                #   'GPS altitude', 'GPS vertical speed', 'spare A/D'
                # Again, lat and lon are two fields each, degrees & minutes
                # But probably only need flight data GPS from C
                continue
            else:
                continue
            lon = self.dmm_to_decimal(float(lon_deg), float(lon_min))
            lat = self.dmm_to_decimal(float(lat_deg), float(lat_min))
            data_time = datetime.strptime(
                f"{year} {int(day):03d} {utc}",
                "%Y %j %H:%M:%S.%f"
            )
            if lat > north:
                north = lat
            if lat < south:
                south = lat
            if lon > east:
                east = lon
            if lon < west:
                west = lon
            if data_time < minTime:
                minTime = data_time
            if data_time > maxTime:
                maxTime = data_time

        return {
            "start": minTime,
            "end": maxTime,
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
