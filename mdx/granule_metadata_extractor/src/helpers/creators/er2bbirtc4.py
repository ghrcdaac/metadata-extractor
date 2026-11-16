"""Lookup creator for ER-2 BBIR TC4 (er2bbirtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "er2bbirtc4"
provider_path = "er2bbirtc4/"

nav_lookup = {'TC4_ER2_BBIR_20070717.txt': {'start': '2007-07-17T00:00:13+00:00',
  'end': '2007-07-17T02:46:39+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070719.txt': {'start': '2007-07-19T00:00:12+00:00',
  'end': '2007-07-19T00:00:19+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070722.txt': {'start': '2007-07-22T00:00:12+00:00',
  'end': '2007-07-22T00:00:18+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070724.txt': {'start': '2007-07-24T00:00:12+00:00',
  'end': '2007-07-24T00:00:19+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070725.txt': {'start': '2007-07-25T00:00:14+00:00',
  'end': '2007-07-25T00:00:19+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070729.txt': {'start': '2007-07-29T00:00:12+00:00',
  'end': '2007-07-29T00:00:18+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070731.txt': {'start': '2007-07-31T00:00:13+00:00',
  'end': '2007-07-31T00:00:18+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070803.txt': {'start': '2007-08-03T00:00:13+00:00',
  'end': '2007-08-03T00:00:19+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070805.txt': {'start': '2007-08-05T00:00:13+00:00',
  'end': '2007-08-05T00:00:18+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21055000000001,
  'west': -84.21055000000001},
 'TC4_ER2_BBIR_20070806.txt': {'start': '2007-08-06T00:00:12+00:00',
  'end': '2007-08-06T00:00:19+00:00',
  'north': 9.99361,
  'south': 9.99361,
  'east': -84.20952000000001,
  'west': -84.20952000000001},
 'TC4_ER2_BBIR_20070808.txt': {'start': '2007-08-08T00:00:12+00:00',
  'end': '2007-08-08T00:00:18+00:00',
  'north': 9.99327,
  'south': 9.99327,
  'east': -84.21038,
  'west': -84.21038}
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
