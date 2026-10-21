"""Lookup creator for DC-8 JPL SAW FDR (dc8dadsfdr)."""
from datetime import datetime, timedelta
from utils.mdx import MDX
from utils.streams import as_text_stream

short_name = "dc8sawfdr"
provider_path = "dc8sawfdr/"

nav_lookup = {
    'CAMEX3_DC8FDR_SAW_19980810_1458_2230_980404_s1.txt':
        {'north': 28.25,
         'south': 28.25,
         'east': -80.603,
         'west': -80.603,
         'start': "1998-08-10T14:58:00.480000",
         'end': "1998-08-10T22:29:48.120000"
        },
    'CAMEX3_DC8FDR_SAW_19980810_1500_2229_980404_s2.txt':
        {'north': 28.25,
         'south': 28.25,
         'east': -80.603,
         'west': -80.603,
         'start': "1998-08-10T14:59:51.720000",
         'end': "1998-08-10T22:29:18.240000",
        },
    'CAMEX3_DC8FDR_SAW_19980813_1852_2356_980405_s1.txt':
        {'north': 28.929,
         'south': 24.068,
         'east': -77.283,
         'west': -82.18,
         'start': "1998-08-13T18:52:48",
         'end': "1998-08-13T23:56:00.960000"
         },
    'CAMEX3_DC8FDR_SAW_19980813_1853_2356_980405_s2.txt':
        {'north': 28.929,
         'south': 24.068,
         'east': -77.283,
         'west': -82.18,
         'start': "1998-08-13T18:52:48",
         'end': "1998-08-13T23:56:00.960000"
        },
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
        metadata = nav_lookup[filename]
        for field in ['start', 'end']:
            metadata[field] = datetime.fromisoformat(metadata[field])
        metadata['format'] = "ASCII"
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