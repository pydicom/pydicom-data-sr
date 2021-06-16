
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
    parser = argparse.ArgumentParser(
        description="",
        usage=""
    )

    # General Options
    gen_opts = parser.add_argument_group('General Options')
    gen_opts.add_argument(
        "-d", "--dev",
        help="enable dev mode",
        action="store_true",
    )

    # Development Options
    dev_opts = parser.add_argument_group('Development Options')
    dev_opts.add_argument(
        "--download",
        help="download the CID files",
        action="store_true",
        default=False,
    )
    dev_opts.add_argument(
        "--checksum",
        help="perform the checksum comparison",
        action="store_true",
        default=False,
    )
    dev_opts.add_argument(
        "--force-update",
        help="force regenerating the data tables",
        action="store_true",
        default=False,
    )
    dev_opts.add_argument(
        "--debug-snomed",
        help="SNOMED debugging",
        action="store_true",
        default=False,
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    run(PACKAGE_DIR / "temp")

    # cli_args = _setup_argparser()
    #
    # if cli_args.dev:
    #     LOGGER.debug("Running in development mode")
    #     LOGGER.debug(cli_args)
    #
    #     dst = Path(__file__).parent.resolve() / "temp"
    #     dst.mkdir(exist_ok=True)
    #
    #     if cli_args.debug_snomed:
    #         get_table_o1(dst / "part16_o1.html", dst / "part16_o1.html")
    #         #get_table_d1(dst / "part16_d1.html", dst / "part16_d1.html")
    #
    #     result = download_and_compare(
    #         dst, cli_args.download, cli_args.checksum
    #     )
    #
    #     if not result or cli_args.force_update:
    #         LOGGER.info("CID checksum mismatch - updating package!")
    #         update_package(dst)
    #     else:
    #         # No change necessary
    #         LOGGER.info("No changes required - going back to sleep")
    #
    #     sys.exit()
    #
    # with TemporaryDirectory() as dirname:
    #     dst = Path(dirname)
    #     LOGGER.info(f"Downloading to {dst}")
    #     result = download_and_compare(dst, True, True)
    #     if not result:
    #         # Checksums have changed or new files found -> update package
    #         LOGGER.info("CID checksum mismatch - updating package!")
    #         update_package(dst)
    #     else:
    #         # No change necessary
    #         LOGGER.info("CID checksums match 'hashes.json'")
    #         LOGGER.info("No changes required - going back to sleep")
