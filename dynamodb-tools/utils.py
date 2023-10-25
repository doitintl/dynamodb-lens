from datetime import datetime
import json
import boto3

stamp = datetime.now().strftime("%Y%m%d%H%M%S")


def date_to_iso(o):
    if isinstance(o, datetime):
        return o.isoformat()


def json_dumps_iso(input_dict):
    return json.dumps(
        input_dict,
        default=date_to_iso,
        indent=4
    )


def return_boto3_client(client_name):
    return boto3.client(client_name)


def write_output(output, filename, output_format='json'):
    output_file = f'{filename}_{stamp}.{output_format}'
    with open(output_file, "w") as outfile:
        outfile.write(output)
    return output_file
