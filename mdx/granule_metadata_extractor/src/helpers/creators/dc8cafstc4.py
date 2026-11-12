"""Lookup creator for DC-8 CAFS TC4 (dc8cafstc4)."""
# Create lookup zip for dc8navtc4
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8cafstc4"
provider_path = "dc8cafstc4/"

nav_lookup = {'TC4_CAFS_DC8_20070628.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070630.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070717.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070721.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070722.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070724.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070728.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070729.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070731.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070803.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070805.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070806.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070808.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAFS_DC8_20070810.txt': {'start': '2007-06-28T21:34:57+00:00',
  'end': '2007-06-29T00:15:55+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228}
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
        if filename in nav_lookup:
            metadata = nav_lookup[filename]
        else:
            raise KeyError(f"No nav lookup for file {filename}")
        for field in ['start', 'end']:
            metadata[field] = datetime.fromisoformat(metadata[field])
        metadata['format'] = self.file_type
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
