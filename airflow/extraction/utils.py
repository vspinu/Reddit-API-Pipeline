import configparser
import datetime
import pathlib
from datetime import datetime

def parse_date_input(date_input):
  """Validate that input is of correct format."""
  try:
    return datetime.strptime(date_input, '%Y%m%d')
  except ValueError:
    raise ValueError("Input parameter should be YYYYMMDD")

# Read Configuration File
def read_config(cur_file):
    parent = pathlib.Path(cur_file).parent.resolve()
    conf = configparser.ConfigParser()
    conf.read(f"{parent}/configuration.conf")
    return conf
