import logging

# Enable logging
logging.basicConfig(level=logging.INFO)
show_warnings = True


class Config(object):

    # Graphical representations
    show_phantoms = False

    # Optimizer constants
    prepend_phantom = "Phantom_"
    prepend_table_alias = "t_"

