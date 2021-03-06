import os
from yaml import load


def set_environment_config(file_name):
    with open(file_name, 'r') as stream:
        configuration = load(stream)
    for k, v in configuration.items():
        os.environ[k] = str(v)

