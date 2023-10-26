#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import json
import pyclowder
from pyclowder.extractors import Extractor
import pyclowder.files
import os
import matplotlib.pyplot as plt
import netcdfutils

plt.rcParams["figure.figsize"] = (16.0, 12.0)


class GeoNetCDF(Extractor):
    """Count the number of characters, words and lines in a text file."""

    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        logging.basicConfig(level=logging.INFO)
        # setup logging for the exctractor

        logging.getLogger("pyclowder").setLevel(logging.DEBUG)
        logging.getLogger("__main__").setLevel(logging.DEBUG)

    def process_message(
        self,
        connector,
        host,
        secret_key,
        resource,
        parameters,
        projection="Polar Stereographic'",
    ):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        params = json.loads(parameters["parameters"])

        inputfile = resource["local_paths"][0]
        file_id = resource["id"]
        file_name = resource["name"]
        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        logger.debug("Preparing to generate plots")

        png_filepaths = netcdfutils.generate_maps_for_file(path_to_file=inputfile)
        for png_file in png_filepaths:
            base_name = os.path.basename(png_file)
            variable_name = base_name.replace(file_name, "")
            variable_name = variable_name.lstrip("_")
            variable_name = variable_name.rstrip(".png")
            preview_id = pyclowder.files.upload_preview(
                connector,
                host,
                secret_key,
                file_id,
                png_file,
                None,
                "image/" + "png",
                visualization_name=variable_name,
                visualization_description=variable_name,
                visualization_component_id="basic-image-component",
            )
            try:
                os.remove(png_file)
            except Exception as e:
                logger.debug(f"Error removing {png_file}")
                logger.debug(f"{e}")
        try:
            logger.debug("Cleaning up all png files")
            os.system("rm *.png")
        except Exception as e:
            logger.debug(f"Error cleaning up files {e}")


if __name__ == "__main__":
    clowder_version = os.environ.get("CLOWDER_VERSION")
    print(clowder_version, "is the clowder version")
    extractor = GeoNetCDF()
    extractor.start()
