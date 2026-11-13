"""Lookup creator for DC-8 PALMS TC4 (dc8palmstc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8palmstc4"
provider_path = "dc8palmstc4/"

nav_lookup = {'TC4_PALMS_DC8_20070628_R2.ict': {'start': '2007-06-28T21:20:00+00:00',
  'end': '2007-06-28T23:55:00+00:00',
  'north': 39.993096,
  'south': 38.451804,
  'east': -119.411688,
  'west': -126.298052},
 'TC4_PALMS_DC8_20070630_R2.ict': {'start': '2007-06-30T19:35:00+00:00',
  'end': '2007-06-30T23:20:00+00:00',
  'north': 44.381676,
  'south': 38.752632,
  'east': -121.631348,
  'west': -127.35482},
 'TC4_PALMS_DC8_20070702_R2.ict': {'start': '2007-07-02T18:45:00+00:00',
  'end': '2007-07-02T23:55:00+00:00',
  'north': 38.618988,
  'south': 31.820172,
  'east': -117.236576,
  'west': -125.115724},
 'TC4_PALMS_DC8_20070713_R2.ict': {'start': '2007-07-13T15:10:00+00:00',
  'end': '2007-07-13T21:45:00+00:00',
  'north': 38.570423,
  'south': 9.842033,
  'east': -84.288139,
  'west': -121.400642},
 'TC4_PALMS_DC8_20070717_R2.ict': {'start': '2007-07-17T13:00:00+00:00',
  'end': '2007-07-17T20:15:00+00:00',
  'north': 9.997215,
  'south': -3.272896,
  'east': -80.066986,
  'west': -84.775486},
 'TC4_PALMS_DC8_20070721_R2.ict': {'start': '2007-07-21T12:15:00+00:00',
  'end': '2007-07-21T20:10:00+00:00',
  'north': 12.206154,
  'south': 2.653542,
  'east': -72.467709,
  'west': -85.490627},
 'TC4_PALMS_DC8_20070722_R2.ict': {'start': '2007-07-22T13:00:00+00:00',
  'end': '2007-07-22T21:00:00+00:00',
  'north': 15.73225,
  'south': 5.939655,
  'east': -78.160858,
  'west': -85.751038},
 'TC4_PALMS_DC8_20070724_R2.ict': {'start': '2007-07-24T12:50:00+00:00',
  'end': '2007-07-24T16:30:00+00:00',
  'north': 9.974041,
  'south': 5.025902,
  'east': -84.20042,
  'west': -86.03754},
 'TC4_PALMS_DC8_20070728_R2.ict': {'start': '2007-07-28T12:50:00+00:00',
  'end': '2007-07-28T19:20:00+00:00',
  'north': 16.546268,
  'south': 8.749752,
  'east': -81.318398,
  'west': -88.76627},
 'TC4_PALMS_DC8_20070729_R2.ict': {'start': '2007-07-29T12:40:00+00:00',
  'end': '2007-07-29T20:05:00+00:00',
  'north': 10.553055,
  'south': -6.420479,
  'east': -78.234673,
  'west': -85.113316},
 'TC4_PALMS_DC8_20070731_R2.ict': {'start': '2007-07-31T12:40:00+00:00',
  'end': '2007-07-31T20:15:00+00:00',
  'north': 10.618286,
  'south': 8.052292,
  'east': -82.575302,
  'west': -89.509048},
 'TC4_PALMS_DC8_20070803_R2.ict': {'start': '2007-08-03T12:55:00+00:00',
  'end': '2007-08-03T19:40:00+00:00',
  'north': 13.840027,
  'south': 5.319443,
  'east': -80.041409,
  'west': -86.221218},
 'TC4_PALMS_DC8_20070805_R2.ict': {'start': '2007-08-05T12:20:00+00:00',
  'end': '2007-08-05T19:20:00+00:00',
  'north': 9.938679,
  'south': 5.35017,
  'east': -76.731262,
  'west': -84.354057},
 'TC4_PALMS_DC8_20070806_R2.ict': {'start': '2007-08-06T11:45:00+00:00',
  'end': '2007-08-06T19:00:00+00:00',
  'north': 9.957561,
  'south': -3.000469,
  'east': -84.277153,
  'west': -92.309189},
 'TC4_PALMS_DC8_20070808_R2.ict': {'start': '2007-08-08T12:05:00+00:00',
  'end': '2007-08-08T21:15:00+00:00',
  'north': 10.007,
  'south': 1.443672,
  'east': -70.319538,
  'west': -85.054951},
 'TC4_PALMS_DC8_20070810_R2.ict': {'start': '2007-08-10T14:50:00+00:00',
  'end': '2007-08-10T21:20:00+00:00',
  'north': 39.204197,
  'south': 9.997559,
  'east': -84.140854,
  'west': -121.297474}
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
