# create lookup zip for crsimpacts
# for all future collections
from datetime import datetime, timedelta, time
from utils.mdx import MDX
import io
from os import PathLike 
import h5py
import numpy as np

short_name = "crsimpacts"
provider_path = "crsimpacts/"
file_type = "HDF-5"


def _as_seekable_hdf5_stream(source):
    """
    Return a seekable binary stream suitable for h5py.

    Accepted inputs:
      - S3/botocore StreamingBody
      - local binary file object
      - bytes / bytearray
      - local filename or pathlib.Path
    """
    if isinstance(source, (str, PathLike)):
        with open(source, "rb") as local_file:
            return io.BytesIO(local_file.read())

    if isinstance(source, (bytes, bytearray)):
        return io.BytesIO(source)

    if hasattr(source, "read"):
        return io.BytesIO(source.read())

    raise TypeError(
        "Expected a path, bytes, or binary file-like object; "
        f"got {type(source).__name__}"
    )

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
        return self.get_hdf5_metadata(filename, file_obj_stream)

    def get_hdf5_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from CRS IMPACTS HDF5 files.
        Supports both the older and newer file revisions.
        """
        # StreamingBody is not reliably seekable, while h5py needs a seekable
        # file-like object; make an in-memory buffer from its bytes.
        print(f"Processing {filename}")
        file_buffer = _as_seekable_hdf5_stream(file_obj_stream)

        with h5py.File(file_buffer, mode="r") as h5:
            lat = h5["/Navigation/Data/Latitude"][:]
            lon = h5["/Navigation/Data/Longitude"][:]
            tm = h5["/Time/Data/TimeUTC"][:]

            north = float(np.nanmax(lat))
            south = float(np.nanmin(lat))
            east = float(np.nanmax(lon))
            west = float(np.nanmin(lon))

            start_time = datetime(1970, 1, 1) + timedelta(
                seconds=float(np.nanmin(tm))
            )
            end_time = datetime(1970, 1, 1) + timedelta(
                seconds=float(np.nanmax(tm))
            )

        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type,
        }

    def main(self):
        start_time = time.time()
        self.process_collection(short_name, provider_path, max_concurrent=1)
        elapsed_time = time.time() - start_time
        print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
