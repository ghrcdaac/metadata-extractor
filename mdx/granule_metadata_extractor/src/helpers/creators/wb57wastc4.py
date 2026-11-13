"""Lookup creator for WB-57 WAS TC4 (wb57wastc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57wastc4"
provider_path = "wb57wastc4/"

nav_lookup = {'TC4_WB57_WAS_20070803.txt': {'start': '2007-08-03T12:45:38+00:00',
  'end': '2007-08-03T16:01:43+00:00',
  'north': 27.46,
  'south': 9.8523,
  'east': -83.9989,
  'west': -93.6615},
 'TC4_WB57_WAS_20070805.txt': {'start': '2007-08-05T13:46:28+00:00',
  'end': '2007-08-05T16:57:02+00:00',
  'north': 10.0882,
  'south': 6.1546,
  'east': -79.5831,
  'west': -84.6423},
 'TC4_WB57_WAS_20070806.txt': {'start': '2007-08-06T13:26:22+00:00',
  'end': '2007-08-06T15:02:36+00:00',
  'north': 8.661,
  'south': 1.9746,
  'east': -86.1472,
  'west': -92.1919},
 'TC4_WB57_WAS_20070808.txt': {'start': '2007-08-08T12:40:13+00:00',
  'end': '2007-08-08T15:45:34+00:00',
  'north': 8.8308,
  'south': 5.9237,
  'east': -82.1461,
  'west': -84.918},
 'TC4_WB57_WAS_20070809.txt': {'start': '2007-08-09T13:16:36+00:00',
  'end': '2007-08-09T16:42:19+00:00',
  'north': 29.4135,
  'south': 10.974,
  'east': -85.1195,
  'west': -94.9166},
 'TC4_WB57_WAS_20070813.txt': {'start': '2007-08-13T15:56:37+00:00',
  'end': '2007-08-13T19:12:10+00:00',
  'north': 36.162,
  'south': 29.436,
  'east': -85.182,
  'west': -94.8796}
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
