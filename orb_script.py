import csv
import argparse
import uuid
import time
from itertools import batched
import datetime
from dotenv import load_dotenv
import orb

BATCH_SIZE = 500
load_dotenv()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    args = parser.parse_args()

    event_list = []
    now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(args.filename,newline='') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            event_list.append(
                {
                    'event_name': 'transaction_processed', ## arbitrary value
                    'external_customer_id': row['account_id'],
                    'timestamp': now,
                    'idempotency_key': str(uuid.uuid4()),
                    'properties':{
                        'month': row['month'],
                        'transaction_id': row['transaction_id'],
                        'account_type': row['account_type'],
                        'bank_id': row['bank_id'],
                        'standard': parse_as_int(row['standard']),
                        'sameday': parse_as_int(row['sameday'])
                        }
                }
            )

    client = orb.Orb()

    ## using itertools batched() to loop through events neatly (including remainder)
    for batch in batched(event_list, BATCH_SIZE):
        client.events.ingest(events=list(batch), debug=True)
        time.sleep(1)


def parse_as_int(num_str):
    if num_str == '':
        return False
    else:
        return int(num_str.replace(',',''))


if __name__ == '__main__':
    main()
    