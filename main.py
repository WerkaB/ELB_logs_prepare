import os
import csv
import logging
import statistics
import prepare_log_files

# ------ you should provide these environment variables ------
# logs_location
# bucket_name
# download_directory
# account_nr
# bucket_AZ
# logs_date

log_level = int(os.getenv("flask-log-level", "20"))
log = logging.getLogger("prepare_elb_logs")
logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')

prepare_log_files.download_and_unzip(log)
# left for testing purpose
# prepare_log_files.unzip_files(log)

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

    log.info(f"csv generated")


def make_payload(path):
    onlyfiles = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    log.debug(f"onlyfiles: {onlyfiles}")
    lengths = []
    payload = []

    for file in onlyfiles:
        directory = os.path.join(path,file)

        with open(directory) as f:
            lines = f.readlines()
            log.debug(f"lines: {lines}")
            for line in lines:
                parsed = line.split(" ")
                log.debug(f"parsed: {parsed}")
                dictionary = {}
                lengths.append((len(parsed)))
                for i in range(0, 21):
                    dictionary[headers[i]] = parsed[i]
                payload.append(dictionary)
    average = statistics.mean(lengths)
    log.info(f"average: {average}")

    return payload


if __name__ == '__main__':
    log.info(f"Generating new csv started")

    logs_location = os.getenv("logs_location")
    payload = make_payload(logs_location)
    csv_abs_path = os.path.join(logs_location,"NLB_logs.csv")
    generate_csv(csv_abs_path, headers, payload)
