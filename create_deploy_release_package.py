import os
import subprocess
import sys
import shutil


def find_collection_processors(collection_name):
    bash_command = f'find . -name process*{collection_name}*.py'
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return str(output).strip('b\'\\n').split('\\n')


def find_init_py():
    bash_command = 'find . -path ./package -prune -false -o -name __init__.py'
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return str(output).strip('b\'\\n').split('\\n')


if __name__ == '__main__':
    data_sets = ['nalma', 'globalir']

    # Delete previous package directory if there was an error on last run
    if os.path.exists('./package'):
        shutil.rmtree('./package')

    # Delete previous package
    if os.path.exists('package.zip'):
        os.remove('./package.zip')

    # Copy all of the files out of the ./granule_metadata_extractor/src/ dir except ignoring the helpers directory
    shutil.copytree('./granule_metadata_extractor/src/', './package/granule_metadata_extractor/src/',
                    ignore=shutil.ignore_patterns('helpers'))

    # Copy Lambda handler
    shutil.copy("./handler.py", "./package/handler.py")

    # Copy the relevant processing files from ./granule_metadata_extractor/processing/
    for set_name in data_sets:
        processors = find_collection_processors(set_name)
        for processor in processors:
            try:
                shutil.copy(processor, f'./package/{processor}')
            except IOError as io_err:
                os.mkdir(f'./package/{os.path.dirname(processor).lstrip("./")}')
                shutil.copy(processor, f'./package/{processor}')

    # Get __init__.py
    for init in find_init_py():
        try:
            shutil.copy(init, f'./package/{init.lstrip("./")}')
        except IOError as io_err:
            os.mkdir(f'./package/{os.path.dirname(init).lstrip("./")}')
            shutil.copy(init, f'./package/{init}')

    shutil.copy("./temp/__init__.py", "./package/granule_metadata_extractor/processing")

    # install the requirements into the ./package directory
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "./package", '-r',
                           'requirements_lambda.txt'])

    # Create zip and remove staging directory
    shutil.make_archive('./package', 'zip', './package')
    shutil.rmtree('./package')
