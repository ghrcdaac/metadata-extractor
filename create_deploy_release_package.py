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
    bash_command = 'find . -path ./mdx_lambda -prune -false -o -name __init__.py'
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return str(output).strip('b\'\\n').split('\\n')


if __name__ == '__main__':
    data_sets = ['nalma', 'globalir']

    # Delete previous mdx_lambda directory if there was an error on last run
    if os.path.exists('./mdx_lambda'):
        shutil.rmtree('./mdx_lambda')

    # Delete previous mdx_lambda
    if os.path.exists('mdx_lambda.zip'):
        os.remove('./mdx_lambda.zip')

    # Copy all of the files out of the ./granule_metadata_extractor/src/ dir except ignoring the helpers directory
    shutil.copytree('./granule_metadata_extractor/src/', './mdx_lambda/granule_metadata_extractor/src/',
                    ignore=shutil.ignore_patterns('helpers'))

    # Copy Lambda handler
    shutil.copy("./handler.py", "./mdx_lambda/handler.py")

    # Copy the relevant processing files from ./granule_metadata_extractor/processing/
    for set_name in data_sets:
        processors = find_collection_processors(set_name)
        for processor in processors:
            try:
                shutil.copy(processor, f'./mdx_lambda/{processor}')
            except IOError as io_err:
                os.mkdir(f'./mdx_lambda/{os.path.dirname(processor).lstrip("./")}')
                shutil.copy(processor, f'./mdx_lambda/{processor}')

    # Get __init__.py
    for init in find_init_py():
        try:
            shutil.copy(init, f'./mdx_lambda/{init.lstrip("./")}')
        except IOError as io_err:
            os.mkdir(f'./mdx_lambda/{os.path.dirname(init).lstrip("./")}')
            shutil.copy(init, f'./mdx_lambda/{init}')

    shutil.copy("./temp/__init__.py", "./mdx_lambda/granule_metadata_extractor/processing")

    # install the requirements into the ./mdx_lambda directory
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "./mdx_lambda", '-r',
                           'requirements_lambda.txt'])

    # Create zip and remove staging directory
    shutil.make_archive('./mdx_lambda', 'zip', './mdx_lambda')
    shutil.rmtree('./mdx_lambda')
