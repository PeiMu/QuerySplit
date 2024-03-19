import re
import statistics
from openpyxl import Workbook


def extract_stats(log_file):
    with open(log_file, 'r') as file:
        log_data = file.read()

    # pattern = r'query\s(\d+\.sql)\sgeomean\stime\sis\s([\d.]+)\sms\swith\sdeviation\s=\s([\d.]+)'
    pattern = r'query\s(\d+[a-zA-Z]*\.sql)\sgeomean\stime\sis\s([\d.]+)\sms\swith\sdeviation\s=\s([\d.]+)'
    matches = re.findall(pattern, log_data)

    geomean_times = {}
    deviations = {}

    for match in matches:
        query_id = match[0].split('.')[0]
        query_name = match[0]
        geomean_time = float(match[1])
        deviation = float(match[2])
        geomean_times[query_id] = geomean_time
        deviations[query_id] = deviation

    return geomean_times, deviations


def write_to_excel(geomean_times, deviations, output_file):
    wb = Workbook()
    ws = wb.active
    ws.append(["Query ID", "Query", "Geomean Time (ms)", "Deviation"])

    for query_id, geomean_time in geomean_times.items():
        deviation = deviations[query_id]
        query_name = query_id + ".sql"
        ws.append([query_id, query_name, geomean_time, deviation])

    wb.save(output_file)


#log_file = "imdb/imdb_query_split_log.txt"
#output_file = "imdb-job_query_split.xlsx"
log_file = "imdb/imdb_vanilla_log.txt"
output_file = "imdb-job_vanilla.xlsx"

geomean_times, deviations = extract_stats(log_file)
write_to_excel(geomean_times, deviations, output_file)
