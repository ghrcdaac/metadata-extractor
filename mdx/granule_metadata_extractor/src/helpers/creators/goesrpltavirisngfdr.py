"""Lookup creator for GOES-R PLT AVIRIS-NG Ferry and Dry Run (goesrpltavirisngdr)."""
from utils.mdx import MDX
from utils.streams import as_seekable_binary_stream
from datetime import datetime, timedelta, timezone
import numpy as np
import tarfile
import re
from spectral.io import envi

short_name = "goesrpltavirisngfdr"
provider_path = "goesrpltavirisngfdr/"

nav_time_lookup = {
    '20170321t194012':
        {'north': 38.8497, 'south': 37.7386, 'east': -120.8532, 'west': -121.173, 'start': '2017-03-21T19:40:28Z',
         'end': '2017-03-21T19:51:03Z'},
    '20170321t195545':
        {'north': 38.5513, 'south': 37.2427, 'east': -120.8139, 'west': -120.9719, 'start': '2017-03-21T19:56:01Z',
         'end': '2017-03-21T20:09:04Z'},
    '20170321t201259':
        {'north': 38.558, 'south': 37.2049, 'east': -120.6061, 'west': -120.893, 'start': '2017-03-21T20:13:15Z',
         'end': '2017-03-21T20:25:57Z'},
    '20170321t202746':
        {'north': 38.5006, 'south': 37.2777, 'east': -120.3423, 'west': -120.8852, 'start': '2017-03-21T20:28:02Z',
         'end': '2017-03-21T20:40:39Z'},
    '20170321t204255':
        {'north': 38.186, 'south': 37.172, 'east': -120.1879, 'west': -120.7943, 'start': '2017-03-21T20:43:10Z',
         'end': '2017-03-21T20:53:08Z'},
    '20170321t205641':
        {'north': 38.2091, 'south': 37.1135, 'east': -120.2942, 'west': -120.5802, 'start': '2017-03-21T20:56:56Z',
         'end': '2017-03-21T21:07:59Z'},
    '20170321t210951':
        {'north': 38.2892, 'south': 37.1247, 'east': -119.9801, 'west': -120.7409, 'start': '2017-03-21T21:10:06Z',
         'end': '2017-03-21T21:21:53Z'},
    '20170321t212344':
        {'north': 38.347, 'south': 37.6409, 'east': -120.0951, 'west': -120.3731, 'start': '2017-03-21T21:23:59Z',
         'end': '2017-03-21T21:31:15Z'},
    '20170321t213141':
        {'north': 37.6159, 'south': 37.386, 'east': -120.4484, 'west': -121.4874, 'start': '2017-03-21T21:31:56Z',
         'end': '2017-03-21T21:39:19Z'},
    '20170321t215736':
        {'north': 38.3465, 'south': 37.0446, 'east': -119.8307, 'west': -120.4968, 'start': '2017-03-21T21:57:51Z',
         'end': '2017-03-21T22:11:14Z'},
    '20170321t221205':
        {'north': 38.0902, 'south': 37.0165, 'east': -119.9972, 'west': -120.4003, 'start': '2017-03-21T22:12:20Z',
         'end': '2017-03-21T22:22:32Z'}
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
        filetype_info = ''
        tar_gzipped = False
        if m := re.match(r"goesrplt_avirisngfdr_(\d{4}\d{2}\d{2}t\d{2}\d{2}\d{2})_(.*)(\.tar\.gz)?$",
                    filename):
            flight = m.group(1)
        else:
            raise ValueError(f"Could not parse filename: {filename}")

        if flight in nav_time_lookup:
            metadata = nav_time_lookup[flight]
            for field in ['start', 'end']:
                metadata[field] = datetime.fromisoformat(metadata[field])
        else:
            raise KeyError(f"Flight not found in lookup: {flight}, filename {filename}")
        metadata['format'] = 'ENVI'
        return metadata

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
