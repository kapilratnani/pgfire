import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
config_file = os.path.join(BASE_DIR, "config.json")

conf = None
with open(config_file) as f:
    conf = json.load(f)
