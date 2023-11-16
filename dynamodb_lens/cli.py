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
parser.add_argument("--metric_consumed_period_s", type=int, default=900, help="The cloudwatch metric period for consumed WCU/RCU")
parser.add_argument("--save_analysis", action='store_true', help="save the json formatted analysis to a file")
parser.add_argument("--silent", action='store_true', help="Supress printing analysis to standard output")
parser.add_argument("--verbose", action='store_true', help="Print the full analysis, otherwise a summary will be printed")
args = parser.parse_args()


def main():
    """
    Wrapper to run as a standalone program
    """
    table_name = args.table_name
    verbose = args.verbose
    consumed_period_s = args.metric_consumed_period_s

    table = TableAnalyzer(table_name, verbose=verbose, consumed_period_s=consumed_period_s)
    if not args.silent:
        table.print_analysis()
    if args.save_analysis:
        descriptor = 'verbose' if verbose else 'summary'
        outfile_name = write_output(output=table.analysis, filename=f'table_analyzer_{table_name}_{descriptor}')
        logging.info(f'Analysis saved to {outfile_name}')
    else:
        logging.warning('Use the --save_analysis argument to save the full analysis to a JSON file')


if __name__ == '__main__':
    main()
