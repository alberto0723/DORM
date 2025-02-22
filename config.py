import logging
from pathlib import Path

# Enable logging
logging.basicConfig(level=logging.INFO)


class Config(object):

    # Path definitions
    base_path = Path(__file__).parent
    output_path = base_path.parent.joinpath("output")

    # Graphical representations
    phantom = False
