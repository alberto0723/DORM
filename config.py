import logging
from pathlib import Path

# Enable logging
logging.basicConfig(level=logging.INFO)


class Config(object):

    # Path definitions
    base_path = Path(__file__).parent
    input_path = base_path.joinpath("output")
    output_path = base_path.joinpath("output")

    # Graphical representations
    phantom = False
