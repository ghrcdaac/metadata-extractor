"""Lookup creator for CAMEX-4 ER-2 METNAV (c4metnavfdr)."""
from utils.mdx import MDX
from utils.streams import as_text_stream
import re
from datetime import datetime, timedelta, time, timezone

short_name = "c4metnavfdr"
provider_path = "c4metnavfdr/"

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
        if not filename.endswith('.f13'):
            return {}
        file_buffer = as_text_stream(file_obj_stream)
        start = datetime.max.replace(tzinfo=timezone.utc)
        end = datetime.min.replace(tzinfo=timezone.utc)
        north = -90
        south = 90
        east = -180
        west = 180

        if m := re.search(r'_metnav_(\d{4})_', filename):
            year = int(m.group(1))

        for line in file_buffer:
            if not line[0:3].isnumeric():
                continue
            day_of_year = int(line[0:3])

            d = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            t = time.fromisoformat(line[4:12])
            dt = datetime.combine(d, t, tzinfo=timezone.utc)

            if dt < start:
                start = dt
            if dt > end:
                end = dt

            lat_quad = line[16:17]
            lat = float(line[17:23])
            lon_quad = line[25:26]
            lon = float(line[26:33])

            lat = +lat if lat_quad == 'N' else -lat
            lon = +lon if lon_quad == 'E' else -lon

            if lat > north:
                north = lat
            if lat < south:
                south = lat
            if lon > east:
                east = lon
            if lon < west:
                west = lon

        return {
            "start": start,
            "end": end,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": 'ASCII'
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

