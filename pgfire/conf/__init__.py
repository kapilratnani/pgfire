import os
import json

__all__ = ["config"]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
config_file = os.path.join(BASE_DIR, "config.json")

config = None
with open(config_file) as f:
    config = json.load(f)
