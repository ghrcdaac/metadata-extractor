"""Lookup creator for WB-57 PANTHER TC4 (wb57panthertc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57panthertc4"
provider_path = "wb57panthertc4/"

nav_lookup = {'TC4_WB57_PANTHER_20070803_ECD.txt': {'start': '2007-08-03T12:23:39+00:00',
  'end': '2007-08-03T16:22:49+00:00',
  'north': 29.2969,
  'south': 9.8592,
  'east': -83.9922,
  'west': -94.8568},
 'TC4_WB57_PANTHER_20070805_ECD.txt': {'start': '2007-08-05T13:22:38+00:00',
  'end': '2007-08-05T16:50:18+00:00',
  'north': 10.0992,
  'south': 6.1503,
  'east': -79.5309,
  'west': -84.2658},
 'TC4_WB57_PANTHER_20070806_ECD.txt': {'start': '2007-08-06T13:03:04+00:00',
  'end': '2007-08-06T14:31:44+00:00',
  'north': 10.0408,
  'south': 3.786,
  'east': -84.496,
  'west': -91.4723},
 'TC4_WB57_PANTHER_20070808_ECD.txt': {'start': '2007-08-08T12:27:37+00:00',
  'end': '2007-08-08T16:25:37+00:00',
  'north': 9.8192,
  'south': 5.9263,
  'east': -82.1022,
  'west': -85.161},
 'TC4_WB57_PANTHER_20070809_ECD.txt': {'start': '2007-08-09T13:02:25+00:00',
  'end': '2007-08-09T16:33:35+00:00',
  'north': 28.9351,
  'south': 10.1177,
  'east': -84.3288,
  'west': -94.6918},
 'TC4_WB57_PANTHER_20070813_ECD.txt': {'start': '2007-08-13T15:43:55+00:00',
  'end': '2007-08-13T19:01:05+00:00',
  'north': 36.121,
  'south': 29.3549,
  'east': -85.1974,
  'west': -94.7634}
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
