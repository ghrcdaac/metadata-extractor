"""Lookup creator for WB-57 SP2 TC4 (wb57sp2tc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "wb57sp2tc4"
provider_path = "wb57sp2tc4/"

nav_lookup = {'TC4_WB57_SP2_20070801_SP.txt': {'start': '2007-08-01T16:28:30+00:00',
  'end': '2007-08-01T20:27:30+00:00',
  'north': 36.8231,
  'south': 29.2671,
  'east': -86.6047,
  'west': -95.1799},
 'TC4_WB57_SP2_20070803_SP.txt': {'start': '2007-08-03T12:09:00+00:00',
  'end': '2007-08-03T16:38:30+00:00',
  'north': 29.6188,
  'south': 9.8537,
  'east': -83.992,
  'west': -95.1668},
 'TC4_WB57_SP2_20070805_SP.txt': {'start': '2007-08-05T13:13:00+00:00',
  'end': '2007-08-05T17:11:00+00:00',
  'north': 10.1048,
  'south': 6.1503,
  'east': -79.5299,
  'west': -84.6667},
 'TC4_WB57_SP2_20070806_SP.txt': {'start': '2007-08-06T12:52:00+00:00',
  'end': '2007-08-06T17:05:00+00:00',
  'north': 10.0521,
  'south': 1.8826,
  'east': -84.1876,
  'west': -92.2816},
 'TC4_WB57_SP2_20070808_SP.txt': {'start': '2007-08-08T12:15:00+00:00',
  'end': '2007-08-08T16:47:30+00:00',
  'north': 10.0218,
  'south': 5.899,
  'east': -82.1013,
  'west': -85.165},
 'TC4_WB57_SP2_20070809_SP.txt': {'start': '2007-08-09T12:52:00+00:00',
  'end': '2007-08-09T16:56:00+00:00',
  'north': 29.6942,
  'south': 9.9699,
  'east': -84.1872,
  'west': -95.1594},
 'TC4_WB57_SP2_20070813_SP.txt': {'start': '2007-08-13T15:24:30+00:00',
  'end': '2007-08-13T19:21:00+00:00',
  'north': 36.1823,
  'south': 29.3416,
  'east': -85.1803,
  'west': -95.1644}
}
nav_lookup['TC4_WB57_SP2_20070801_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070801_SP.txt']
nav_lookup['TC4_WB57_SP2_20070803_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070803_SP.txt']
nav_lookup['TC4_WB57_SP2_20070805_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070805_SP.txt']
nav_lookup['TC4_WB57_SP2_20070806_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070806_SP.txt']
nav_lookup['TC4_WB57_SP2_20070808_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070808_SP.txt']
nav_lookup['TC4_WB57_SP2_20070809_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070809_SP.txt']
nav_lookup['TC4_WB57_SP2_20070813_SM.txt'] = nav_lookup['TC4_WB57_SP2_20070813_SP.txt']

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
