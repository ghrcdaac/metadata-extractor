import subprocess
import sys
import shutil


def find_collection_processors(collection_name):
    bash_command = f'find . -name process*{collection_name}*.py'
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return str(output).strip('b\'\\n').split('\\n')


if __name__ == '__main__':
    data_sets = ['amsua', 'isslis', 'nalma', 'rss']

    # copy all of the files out of the ./granule_metadata_extractor/src/ dir except ignoring the helpers directory
    shutil.copytree('./granule_metadata_extractor/src/', './package',
                    ignore=shutil.ignore_patterns('helpers', '__init__.py'))

    # install the requirements into the ./package directory
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "./package", '-r', 'requirements.txt'])

    # copy the relevant processing files from ./granule_metadata_extractor/processing/
    for set_name in data_sets:
        [shutil.copy(ele, './package') for ele in find_collection_processors(set_name)]

    # Copy the main and version
    shutil.copy('./process_mdx/main.py', './package')
    shutil.copy('./process_mdx/version.py', './package')

    # Create zip and remove staging directory
    shutil.make_archive('./package', 'zip', './package')
    shutil.rmtree('./package')
