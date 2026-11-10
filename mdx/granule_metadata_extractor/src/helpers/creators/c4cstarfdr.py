"""Lookup creator for DC-8 CSTAR CAMEX-4 FDR (c4cstarfdr)."""
from utils.mdx import MDX
from utils.streams import as_text_stream
import re
from datetime import datetime, timezone

short_name = "c4cstarfdr"
provider_path = "c4cstarfdr/"

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
        start = datetime.max.replace(tzinfo=timezone.utc)
        end = datetime.min.replace(tzinfo=timezone.utc)
        north = -90.0
        south = 90.0
        east = -180.0
        west = 180.0

        stream = as_text_stream(file_obj_stream)

        for line in stream:
            fields = line.split()
            month = int(fields[1])
            day = int(fields[2])
            year = int(fields[3])
            hour = int(fields[4])
            minute = int(fields[5])
            second = int(fields[6])
            latitude = float(fields[7])
            longitude = float(fields[8])

            timestamp = datetime(year, month, day, hour, minute, second,
                                 tzinfo=timezone.utc)
            if timestamp < start:
                start = timestamp
            if timestamp > end:
                end = timestamp
            if latitude > north:
                north = latitude
            if latitude < south:
                south = latitude
            if longitude > east:
                east = longitude
            if longitude < west:
                west = longitude

        return {
            'start': start,
            'end': end,
            'north': north,
            'south': south,
            'east': east,
            'west': west,
            'format': 'ASCII',
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
