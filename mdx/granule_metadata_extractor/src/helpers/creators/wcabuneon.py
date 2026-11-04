"""Creator for WCABU NEON (wcabuneon)."""
from utils.mdx import MDX
from datetime import datetime
from pathlib import PurePosixPath
from pyproj import Transformer, CRS
from zipfile import ZipFile
import sys
import re
from utils.streams import as_seekable_binary_stream

short_name = "wcabuneon"
provider_path = "wcabuneon/"

big_bbox = {
    'north': 46.849,
    'south': 31.707,
    'east': -96.474,
    'west': -122.409,
}

utm_lookup = {
    'ABBY': 10,
    'CLBJ': 14,
    'JORN': 13,
    'KONZ': 14,
    'MOAB': 12,
    'NIWO': 13,
    'NOGP': 14,
    'ONAQ': 12,
    'RMNP': 13,
    'SJER': 11,
    'SOAP': 11,
    'SRER': 12,
    'TEAK': 11,
    'WREF': 10,
    'YELL': 12,
}

def get_utm_proj(utm_zone):
    utm_crs = CRS.from_dict({
        "proj": "utm",
        "zone": utm_zone,
        "datum": "WGS84",
        "south": False,
    })

    wgs84_crs = CRS.from_epsg(4326)

    transformer = Transformer.from_crs(
        utm_crs,
        wgs84_crs,
        always_xy=True,
    )
    return transformer

def get_years(filenames):
    years = set()
    for filename in filenames:
        if not filename.endswith(".shp"):
            continue
        if m := re.search(r'^\w{4}_(\d{4})_', filename):
            year = int(m.group(1))
            years.add(year)
    return sorted(list(years))

def get_start_end(years):
    first_year = min(years)
    last_year = max(years)
    start = datetime(first_year, 1, 1)
    end = datetime(last_year, 12, 31)
    return start, end

def shapefile_bounds(filenames: list[str], transformer: Transformer) -> dict:
    """
    Calculate WGS84 bounding box from NEON shapefile filenames.

    Assumes 1000m x 1000m UTM tiles.
    """

    eastings = []
    northings = []

    for filename in filenames:
        name = PurePosixPath(filename).stem
        parts = name.split("_")

        # <site>_<year>_<easting>_<northing>
        eastings.append(int(parts[-2]))
        northings.append(int(parts[-1]))

    # Overall UTM bounding box
    west = min(eastings)
    east = max(eastings) + 1000
    south = min(northings)
    north = max(northings) + 1000

    lon_min, lat_min, lon_max, lat_max = transformer.transform_bounds(
        west, south, east, north,
        densify_pts=21,
    )

    return {
        "north": lat_max,
        "south": lat_min,
        "east": lon_max,
        "west": lon_min,
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
        if filename.endswith(".csv"):
            start, end = get_start_end([2020, 2023])
            metadata = {
                "start": start,
                "end": end,
            }
            metadata.update(big_bbox)
            return metadata
        elif not filename.endswith(".zip"):
            return {}
        site_code = None
        print(f"Processing file {filename}")
        if m := re.search(r'_(\w{4})\.zip$', filename):
            site_code = m.group(1)
        stream = as_seekable_binary_stream(file_obj_stream)
        with ZipFile(stream) as zf:
            filenames = zf.namelist()
        years = get_years(filenames)
        start, end = get_start_end(years)
        utm_zone = utm_lookup[site_code]
        transformer = get_utm_proj(utm_zone)
        bounds = shapefile_bounds(filenames, transformer)
        metadata = {
            'start': start,
            'end': end,
            'format': 'ZIP/Shapefile'
        }
        metadata.update(bounds)
        return metadata

    def main(self):
        self.process_collection(short_name, provider_path, max_concurrent=5)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()