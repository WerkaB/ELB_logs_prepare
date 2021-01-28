import os
import csv
import logging
import statistics


log_level = int(os.getenv("flask-log-level", "20"))
log = logging.getLogger("prepare_elb_logs")
logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')

headers = ["type", "version", "time", "elb", "listener", "client:port", "destination:port",
           "connection_time", "tls_handshake_time", "received_bytes", "sent_bytes",
           "incoming_tls_alert", "chosen_cert_arn", "chosen_cert_serial", "tls_cipher",
           "tls_protocol_version", "tls_named_group", "domain_name", "alpn_fe_protocol",
           "alpn_be_protocol", "alpn_client_preference_list"]

def generate_csv(filename, fieldnames, payload):
    with open(filename, mode='w', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for dir in payload:
            writer.writerow(dir)

    log.debug(f"csv generated")


def make_payload(path):
    onlyfiles = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    print(f"onlyfiles: {onlyfiles}")
    lengths = []
    payload = []

    for file in onlyfiles:
        directory = os.path.join(path,file)

        with open(directory) as f:
            lines = f.readlines()
            print(f"lines: {lines}")
            for line in lines:
                parsed = line.split(" ")
                print(f"parsed: {parsed}")
                dictionary = {}
                lengths.append((len(parsed)))
                for i in range(0, 21):
                    dictionary[headers[i]] = parsed[i]
                payload.append(dictionary)
    average = statistics.mean(lengths)
    print(f"average: {average}")

    return payload


if __name__ == '__main__':
    log.info(f"Generating new csv started")

    logs_location = os.getenv("logs_location")
    payload = make_payload(logs_location)
    generate_csv("NLB_logs.csv", headers, payload)
