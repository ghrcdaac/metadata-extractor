#!/usr/bin/env python
import argparse
import sys
import zipfile
import json
from datetime import datetime, timezone
from tqdm import tqdm


DEFAULT_SUMMARY = {
    'start': datetime.max.replace(tzinfo=timezone.utc),
    'end': datetime.min.replace(tzinfo=timezone.utc),
    'north': -90.0,
    'south': 90.0,
    'east': -180.0,
    'west': 180.0,
    'num_of_files': 0,
    'sizeMB': 0
}

def cast_str_summary(summary):
    """Cast summary dict to correct types."""
    for field in ['start', 'end']:
        summary[field] = datetime.fromisoformat(summary[field])
    for field in ['north', 'south', 'east', 'west', 'sizeMB']:
        summary[field] = float(summary[field])
    for field in ['num_of_files']:
        summary[field] = int(summary[field])
    return summary

def cast_summary_to_str(summary):
    """Cast summary dict backl to str."""
    for field in ['start', 'end']:
        summary[field] = summary[field].strftime("%Y-%m-%dT%H:%M:%S")
    for field in ['north', 'south', 'east', 'west', 'sizeMB', 'num_of_files']:
        summary[field] = str(summary[field])
    return summary

def extract_file(zip_path, filename):
    """Extract a file from the zip archive.

    Args:
        zip_path (str): Path to the zip file.
        filename (str): Name of the file to extract.

    Returns:
        """
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(filename, 'r') as file_in:
            return file_in.read()

def compare_metadata(metadata1, metadata2):
    """Compare two metadata dicts.

    Args:
        metadata1 (dict): First metadata dict.
        metadata2 (dict): Second metadata dict.

    Returns:
        bool: True if both metadata dicts are equal, False otherwise.
    """
    for field in metadata1.keys():
        if metadata1[field] != metadata2[field]:
            return False
    return True

def merge_lookups(dest_path, sources):
    """Merge multiple lookup zip files into one.

    Args:
        dest_path (str): Path to the destination zip file.
        sources (list): List of source lookup zip paths.
    """
    combined_lookup = {}
    combined_summary = DEFAULT_SUMMARY.copy()
    for source in sources:
        with zipfile.ZipFile(source) as zf:
            with zf.open("lookup.json", 'r') as lookup_json:
                lookup = json.loads(lookup_json.read())
                for file_key in tqdm(lookup.keys(), desc=f"Merging {source}"):
                    md = lookup[file_key]
                    if file_key in combined_lookup.keys():
                        same = compare_metadata(combined_lookup[file_key], md)
                        if same:
                            continue # Next file
                        else:
                            raise ValueError(f"Conflicting metadata found in {source} for {file_key!r}")
                    else:
                        combined_lookup[file_key] = md
            with zf.open("summary.json", 'r') as summary_json:
                summary = cast_str_summary(json.loads(summary_json.read()))
                if summary['start'] < combined_summary['start']:
                    combined_summary['start'] = summary['start']
                if summary['end'] > combined_summary['end']:
                    combined_summary['end'] = summary['end']
                if summary['north'] > combined_summary['north']:
                    combined_summary['north'] = summary['north']
                if summary['south'] < combined_summary['south']:
                    combined_summary['south'] = summary['south']
                if summary['east'] > combined_summary['east']:
                    combined_summary['east'] = summary['east']
                if summary['west'] < combined_summary['west']:
                    combined_summary['west'] = summary['west']
                combined_summary['sizeMB'] += summary['sizeMB']
                combined_summary['num_of_files'] += summary['num_of_files']
    with zipfile.ZipFile(dest_path, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lookup.json", json.dumps(combined_lookup).encode("utf-8"))
        zf.writestr("summary.json", json.dumps(cast_summary_to_str(combined_summary)))
    print(f"Wrote {dest_path}.")

def print_summary(zip_path, pretty: bool=False):
    summary = extract_file(zip_path, "summary.json")
    if not pretty:
        print(summary)
    else:
        print(json.dumps(json.loads(summary), indent=2))

def print_lookup(zip_path, pretty: bool=False):
    lookup = extract_file(zip_path, "lookup.json")
    if not pretty:
        print(lookup)
    else:
        print(json.dumps(json.loads(lookup), indent=2))

def print_lookup_files(zip_path, pretty: bool=False):
    lookup_json = extract_file(zip_path, "lookup.json")
    lookup = json.loads(lookup_json)
    if pretty:
        for file in sorted(list(lookup.keys())):
            print(file)
    else:
        print(sorted(list(lookup.keys())))

def parse_args():
    parser = argparse.ArgumentParser(
        description="Inspect and manipulate MDX lookup archives."
    )

    modes = parser.add_mutually_exclusive_group(required=True)

    modes.add_argument(
        "--print-summary",
        metavar="ZIP",
        help="Print the summary from a lookup archive."
    )

    modes.add_argument(
        "--print-lookup",
        metavar="ZIP",
        help="Print the lookup dictionary from an archive."
    )

    modes.add_argument(
        "--print-lookup-files",
        metavar="ZIP",
        help="Print the file names (lookup dictionary keys)."
    )

    modes.add_argument(
        "--merge-lookups",
        nargs="+",
        metavar="ZIP",
        help="Merge lookup archives: DEST SOURCE1 SOURCE2 [SOURCE3 ...]."
    )

    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output."
    )

    args = parser.parse_args()

    if args.merge_lookups is not None and len(args.merge_lookups) < 3:
        parser.error(
            "--merge-lookups requires a destination and at least two sources."
        )

    return args


if __name__ == "__main__":
    args = parse_args()
    pretty_output = args.pretty
    if (args.print_summary):
        print_summary(args.print_summary, pretty=pretty_output)
    elif (args.print_lookup):
        print_lookup(args.print_lookup, pretty=pretty_output)
    elif (args.print_lookup_files):
        print_lookup_files(args.print_lookup_files, pretty=pretty_output)
    elif (args.merge_lookups):
        dest_zip = args.merge_lookups.pop(0)
        src_zips = args.merge_lookups
        merge_lookups(dest_zip, src_zips)
