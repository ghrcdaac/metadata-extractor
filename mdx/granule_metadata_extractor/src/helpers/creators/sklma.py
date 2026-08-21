"""Creator file for South Korea LMA Level 1."""
from utils.mdx import MDX
from utils.lma import read_lma_file
from utils.streams import as_text_stream
from datetime import timedelta
from math import radians, degrees, asin, sin, cos

short_name = "sklma"
provider_path = "sklma/"
file_type = "ASCII"
bounds_method = 'actual_data' # Whether to select bounds of actual_data or study_area

EARTH_RADIUS_KM = 6371.0

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
        if filename.endswith(".dat.gz"):
            # Handle gzipped case
            gzipped = True
        else:
            gzipped = False
        if not filename.lower().endswith(".dat"):
            return {}

        stream = as_text_stream(file_obj_stream, gzipped=gzipped)

        header, records = read_lma_file(stream)

        # Temporal bounds
        start_time = header.start_time
        end_time = start_time + timedelta(seconds=header.analyzed_duration)

        # Get center point
        center_lat, center_lon, center_alt = header.coordinate_center
        # Calculate bounding box 200 km from center

        lat_delta = degrees(200.0 / EARTH_RADIUS_KM)
        lon_delta = degrees(
            asin(sin(200.0 / EARTH_RADIUS_KM) / cos(radians(center_lat)))
        )

        north_bound = center_lat + lat_delta
        south_bound = center_lat - lat_delta
        east_bound = center_lon + lon_delta
        west_bound = center_lon - lon_delta

        times = []
        lats = []
        lons = []

        # Even if there are no records, we have a bounding box
        for record in records:
            latitude = record.latitude
            longitude = record.longitude
            if (latitude > north_bound or latitude < south_bound
                    or longitude > east_bound or longitude < west_bound):
                continue
            times.append(record.time)
            lats.append(latitude)
            lons.append(longitude)

        # If we are selecting area of actual data
        if bounds_method == 'actual_data' and lats:
            north_bound = max(lats)
            south_bound = min(lats)
            east_bound = max(lons)
            west_bound = min(lons)
            start_time = header.start_time + timedelta(seconds=min(times))
            end_time = header.start_time + timedelta(seconds=max(times))
        # Otherwise, if study_area or no data in file,
        # keep the spatial bounds of 200 km from center point
        # and temporal bounds of header.start_time + timedelta(duration)

        return {
            "start": start_time,
            "end": end_time,
            "north": north_bound,
            "south": south_bound,
            "east": east_bound,
            "west": west_bound,
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
