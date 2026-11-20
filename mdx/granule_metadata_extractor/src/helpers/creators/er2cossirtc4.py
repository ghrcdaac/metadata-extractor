"""Lookup creator for ER-2 COSSIR TC4 (er2cossirtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX
from utils.streams import as_text_stream
from datetime import datetime, timedelta, timezone, time
import re

short_name = "er2cossirtc4"
provider_path = "er2cossirtc4/"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    # Ames FFI 2010 has a slightly different format.
    # This quickly extracts UTC timestamps

    @staticmethod
    def chop_ames_header(stream):
        first_line = stream.readline()
        nlines, ffi = first_line.split()
        nlines = int(nlines) - 1
        for i in range(0, nlines):
            stream.readline()
        # Done with header
        return stream

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
        if m := re.search(r'_([A-Z]+)\.txt', filename):
            file_type = m.group(1)
        else:
            raise ValueError(f"Could not parse file type from filename {filename!r}")
        if m := re.search(r'_(\d{4})(\d{2})(\d{2})_', filename):
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
        else:
            raise ValueError(f"Could not parse file date from filename {filename!r}")

        stream = as_text_stream(file_obj_stream)
        stream = self.chop_ames_header(stream)

        start = datetime.max.replace(tzinfo=timezone.utc)
        end = datetime.min.replace(tzinfo=timezone.utc)
        north = -90
        south = 90
        east = -180
        west = 180

        beginning_of_day = datetime.combine(
            datetime(year, month, day),
            time.min,
            tzinfo=timezone.utc
        )

        for line in stream:
            fields = line.split()

            if file_type == 'AFT' or file_type == 'FWD' or file_type == 'NAD':
                utc = int(fields[0])
                lat = int(fields[-6]) / 1000
                lon = int(fields[-5]) / 1000
            elif file_type == 'FWDIWP':
                if len(fields) == 8:
                    utc = float(fields[0])
                    lat = float(fields[2])
                    lon = float(fields[3])
            elif file_type == 'NADIWP':
                utc = float(fields[0])
                lat = float(fields[-6])
                lon = float(fields[-5])
            else:
                raise ValueError(f"Unrecognized file type {file_type!r}")

            dt = beginning_of_day + timedelta(
                seconds=utc
            )

            if dt < start:
                start = dt
            if dt > end:
                end = dt
            if lat > north:
                north = lat
            if lat < south:
                south = lat
            if lon > east:
                east = lon
            if lon < west:
                west = lon

        return {
            'start': start,
            'end': end,
            'north': north,
            'south': south,
            'east': east,
            'west': west,
            'format': 'ASCII'
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
