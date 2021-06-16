import argparse
import logging
from pathlib import Path
from tempfile import TemporaryDirectory


from sr import PACKAGE_DIR
from sr.core import run


LOGGER = logging.getLogger(__name__)


def _setup_argparser():
    """Setup the command line arguments"""
    # Description
    parser = argparse.ArgumentParser(description="", usage="")

    # General Options
    gen_opts = parser.add_argument_group("General Options")
    gen_opts.add_argument(
        "-d",
        "--dev",
        help="enable dev mode",
        action="store_true",
    )
    gen_opts.add_argument(
        "--force-download",
        help="force downloading the data tables",
        action="store_true",
        default=False,
    )
    gen_opts.add_argument(
        "--force-regeneration",
        help="force regenerating the data tables",
        action="store_true",
        default=False,
    )
    gen_opts.add_argument(
        "--clean",
        help="remove all data files",
        action="store_true",
        default=False,
    )

    return parser.parse_args()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    args = _setup_argparser()

    src = None
    if args.dev:
        src = PACKAGE_DIR / "temp"
        logging.basicConfig(level=logging.DEBUG)

    if args.clean:
        pass

    run(src, args.force_download, args.force_regeneration)
