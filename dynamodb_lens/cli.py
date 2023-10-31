import argparse
import logging
import os
from argparse import RawTextHelpFormatter
from dynamodb_lens.analyzer import TableAnalyzer, DESCRIPTION
from dynamodb_lens.utils import write_output

logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S"
)

parser = argparse.ArgumentParser(
    prog='cli',
    description=f"{DESCRIPTION}",
    formatter_class=RawTextHelpFormatter
)

parser.add_argument("--table_name", required=True)
parser.add_argument("--save_output", action='store_true', help="save the json formatted output to a file")
parser.add_argument("--verbose", action='store_true', help="Print the full output, otherwise a summary will be printed")
args = parser.parse_args()


def main():
    """
    Wrapper to run as a standalone program
    """
    table_name = args.table_name
    verbose = args.verbose

    table = TableAnalyzer(table_name, verbose=verbose)
    table.print_output()
    if args.save_output:
        outfile_name = write_output(output=table.output, filename=f'table_analyzer_{table_name}')
        logging.info(f'Output saved to {outfile_name}')


if __name__ == '__main__':
    main()
