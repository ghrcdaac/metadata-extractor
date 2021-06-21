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
    staging_dir = "./mdx_lambda"
    data_sets = ['nalma', 'globalir', 'rss', 'goesrpltcrs']

    # Delete previous mdx_lambda directory if there was an error on last run
    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)

    # Delete previous mdx_lambda
    zip_file = f'{staging_dir}.zip'
    if os.path.exists(zip_file):
        os.remove(zip_file)

    # Copy all of the files out of the ./granule_metadata_extractor/src/ dir ignoring the helpers directory
    # src_dir = './granule_metadata_extractor/src/'
    # shutil.copytree(src_dir, f'{staging_dir}/{src_dir.lstrip("./")}', ignore=shutil.ignore_patterns('helpers'))

    # Copy Lambda handler
    # shutil.copy('./handler.py', f'{staging_dir}/handler.py')

    # Copy all processors for testing, all processors are needed due to the structure of the handler.py file
    # src_dir = './granule_metadata_extractor/processing/'
    # shutil.copytree(src_dir, f'{staging_dir}/{src_dir.lstrip("./")}', ignore=shutil.ignore_patterns('helpers'))
    # Copy everything
    src_dir = './granule_metadata_extractor'
    shutil.copytree(src_dir, f'{staging_dir}/{src_dir.lstrip("./")}', ignore=shutil.ignore_patterns('helpers'))
    shutil.copy('./handler.py', f'{staging_dir}/handler.py')

    # Copy the relevant processing files from ./granule_metadata_extractor/processing/
    # for set_name in data_sets:
    #     processors = find_collection_processors(set_name)
    #     for processor in processors:
    #         try:
    #             shutil.copy(processor, f'{staging_dir}/{processor}')
    #         except IOError as io_err:
    #             os.mkdir(f'{staging_dir}/{os.path.dirname(processor).lstrip("./")}')
    #             shutil.copy(processor, f'{staging_dir}/{processor}')

    # Get __init__.py
    # for init in find_init_py():
    #     try:
    #         shutil.copy(init, f'{staging_dir}/{init.lstrip("./")}')
    #     except IOError as io_err:
    #         os.mkdir(f'{staging_dir}/{os.path.dirname(init).lstrip("./")}')
    #         shutil.copy(init, f'{staging_dir}/{init}')

    # This can be done better. Create a stripped init file instead of having a hardcoded one.
    # shutil.copy("./temp/a__init__.py", "./mdx_lambda/granule_metadata_extractor/processing")
    # os.rename("./mdx_lambda/granule_metadata_extractor/processing/a__init__.py",
    #           "./mdx_lambda/granule_metadata_extractor/processing/__init__.py")
    # with open('./mdx_lambda/granule_metadata_extractor/processing/__init__.py', 'r+') as file:
    #     line = file.readlines()
    #     file.seek(0)
    #     for i in line:
    #         if i in data_sets:
    #             file.write(i)

    # install the requirements into the ./mdx_lambda directory
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", staging_dir, '-r',
                           'requirements_lambda.txt'])

    # Create zip and remove staging directory
    shutil.make_archive(staging_dir, 'zip', staging_dir)
    shutil.rmtree(staging_dir)
