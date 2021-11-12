import os
import shutil

exclude_processes_in_lambda = ["creators*", "*pycache*"]


class CreateMDXArtifact:
    """
    Class used to create an artifact to be used within AWS lambda for mdx processing
    """

    def __init__(self):
        self.artifact_location = None
        self.repo_root_dir = None
        self.create_artifact_dir()
        self.add_needed_files()
        shutil.make_archive(self.artifact_location, 'zip', self.artifact_location)
        self.local_cleanup()

    def delete_artifact_dir(self):
        """
        Deletes local artifact directory
        """
        if os.path.exists(self.artifact_location):
            shutil.rmtree(self.artifact_location)

    def create_artifact_dir(self):
        """
        Creates artifact directory. If already exists, deletes first
        """
        self.repo_root_dir = os.path.dirname(os.path.realpath(__file__))
        self.artifact_location = os.path.join(self.repo_root_dir, 'mdx_lambda_artifact')
        self.delete_artifact_dir()
        os.mkdir(self.artifact_location)

    def create_lambda_handler(self):
        """
        Create lambda_handler.py which will be used by AWS to process MDX as lambda
        """
        process_mdx_path = os.path.join(self.repo_root_dir, 'process_mdx')
        output_lines = list()
        header_code = ['import os\n',
                       'import sys\n',
                       "if os.environ.get('CUMULUS_LAYER'):\n",
                       "    sys.path.insert(0, os.environ.get('CUMULUS_LAYER'))\n",
                       'from run_cumulus_task import run_cumulus_task\n']
        lambda_code = ['def task(event, context):\n',
                       '    """\n',
                       '    Intermediate task to parse event and initialize MDX\n',
                       '    :param event: AWS event passed into lambda\n',
                       '    :param context: object provides methods and properties that provide '
                       'information about the\n',
                       '                    invocation, function, and execution environment\n',
                       '    :return: mdx processing output\n',
                       '    """\n',
                       '    print(event)\n',
                       "    mdx_instance = MDX(input=event['input'], config=event['config'])\n",
                       '    return mdx_instance.process()\n',
                       '\n',
                       '\n',
                       'def handler(event, context):\n',
                       '    """\n',
                       '    Lambda handler entry point which will run the mdx_task\n',
                       '    :param event: AWS event passed into lambda\n',
                       '    :param context: object provides methods and properties that provide '
                       'information about the\n',
                       '                    invocation, function, and execution environment\n',
                       '    """\n',
                       '    return run_cumulus_task(task, event, context)\n',
                       '\n',
                       '\n',
                       "if __name__ == '__main__':\n",
                       '    MDX.cli()\n']

        generated_file_path = os.path.join(process_mdx_path, 'lambda_handler.py')
        # Remove file if already exists
        try:
            os.remove(generated_file_path)
        except OSError:
            pass

        # Create new lambda_handler.py file
        with open(os.path.join(process_mdx_path, 'main.py'), 'r') as source_file:
            with open(generated_file_path, 'w+') as generated_file:
                output_lines.extend(header_code)
                for line in source_file.readlines():
                    if '__name__' not in line and 'MDX.cli()' not in line:
                        output_lines.extend(line)
                output_lines.extend(lambda_code)
                generated_file.writelines(output_lines)

    def copy_to_artifact_location(self, source, destination=None, ignore_pattern_list=None):
        """
        Copies object from source relative to current directory to artifact location
        """
        ignore_pattern_list = ignore_pattern_list or ['']
        destination = source if not destination else destination
        source_path = os.path.join(self.repo_root_dir, source)
        output_path = os.path.join(self.artifact_location, destination)
        if os.path.isdir(source_path):
            return shutil.copytree(source_path, output_path,
                                   ignore=shutil.ignore_patterns(*ignore_pattern_list))
        else:
            return shutil.copy(source_path, output_path)

    def add_needed_files(self):
        """
        Copies and creates files needed for artifact creation
        """
        # Create lambda handler
        self.create_lambda_handler()
        # Copy all of process_mdx dir
        self.copy_to_artifact_location('process_mdx')
        # Copy most of granule_metadata_extractor source files
        self.copy_to_artifact_location('granule_metadata_extractor',
                                       ignore_pattern_list=exclude_processes_in_lambda)

    def local_cleanup(self):
        """
        Clean temporary files from local
        """
        # Delete local lambda_handler if exists
        lambda_handler_path = os.path.join(self.repo_root_dir, 'process_mdx', 'lambda_handler.py')
        if os.path.exists(lambda_handler_path):
            os.remove(lambda_handler_path)
        # Clean up local artifact directory
        self.delete_artifact_dir()


if __name__ == '__main__':
    mdx = CreateMDXArtifact()
