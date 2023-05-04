# - crawl 1k domains from AWSBatchSubmit batch_size = 200 and get all the array_job_id in array response
# - you have array job ids ajid = []
# - describe_jobs(), list_jobs(ajid) get start_time, end_time, run_time_inseconds
# gather all the result values in just one DS
# batch_result = {"domain": {"start_time": "", "end_time": "", "run_time_inseconds": "}}
#
#
# - export new relic data as csv
# - parse csv and get the start and end time
# nr_result = {"domain": {"start_time": "", "end_time": "", "run_time_inseconds": "}}
#
# diff = {"domain": {"nr_run_time_inseconds": "", "batch_run_time_inseconds": "", "nr-batch": "", "batch-nr": ""}}
import os

import boto3
import json
import csv


def execute(array_job_ids, csv_path):
    nr_result = get_nr_data(csv_path)
    batch_result, status_count = get_batch_data(array_job_ids)
    # print("nr", nr_result, "---------------------------------batch", batch_result)

    final_result = {}
    for domain in batch_result:
        batch_run_time_in_seconds = batch_result[domain]["run_time_in_sec"]
        nr_run_time_in_seconds = nr_result[domain]["run_time_in_sec"]
        final_result[domain] = {"nr_run_time_in_seconds": float(nr_run_time_in_seconds),
                                "batch_run_time_in_seconds": float(batch_run_time_in_seconds),
                                "nr-batch": float(nr_run_time_in_seconds) - float(batch_run_time_in_seconds),
                                "batch-nr": float(batch_run_time_in_seconds) - float(nr_run_time_in_seconds)}
    return final_result, status_count


def get_nr_data(csv_path):
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    result = {}
    for row in rows:
        result[row["Domain"]] = {"run_time_in_sec": row["Overall Execution Time"]}

    return result


def get_batch_data(array_job_ids):
    client = boto3.client(service_name="batch", region_name="us-east-1")
    batch_result = {}
    responses = client.describe_jobs(jobs=array_job_ids)
    statuses_count = {}
    min_started_time = float('inf')
    max_stopped_time = 0
    for response in responses["jobs"]:
        params = response["parameters"]["records"]
        parameters = json.loads(params)
        parent_job_id = response["jobId"]
        statuses = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED"]
        for stats in statuses:
            lj_result = client.list_jobs(arrayJobId=parent_job_id, jobStatus=stats)
            child_jobs_results = lj_result["jobSummaryList"]
            for child_job_result in child_jobs_results:
                array_index = child_job_result["arrayProperties"]["index"]
                domain = parameters[array_index]["seed_url"]
                start_time = child_job_result["startedAt"]
                end_time = child_job_result["stoppedAt"]
                min_started_time = min(min_started_time, start_time)
                max_started_time = max(max_stopped_time, end_time)
                run_time_insec = (child_job_result["stoppedAt"] - child_job_result["startedAt"]) \
                                 / 1000
                batch_result[domain] = {"start_time": start_time, "end_time": end_time, "run_time_in_sec":
                    run_time_insec,
                                        "status": stats}
                statuses_count[stats] = statuses_count.get(stats, 0) + 1
    return batch_result, statuses_count


# array_job_ids = ["dd450f7e-0cf1-432a-af6b-eb16e82420e2"]
array_job_ids = ['76ca350d-63d4-4c5c-a6f4-52ea934b1d1e', '88145f70-9dcd-403b-8c1b-6aa33bf63dca', '976384ad-7974-4f47-8091-11e2d80553a3', 'c654f3ca-c6b0-439f-9caa-bc937159dc53', '9b8273e4-2c94-4416-ab0a-995adc6f6c05', 'f76a2912-8eef-4dd7-a7a6-ae8a0784e248', 'e25a4726-d4ae-4066-9bb7-455ffb7f667c', '4a45213e-4061-4a72-afa5-923ba5a8c8b1', '3882eb27-d423-4b1c-b070-63b3caf7a967', '9f496737-ffba-490f-8b04-8512ad49f961']

csv_name = "nr_table_500.csv"
result, statuses = execute(array_job_ids, f"/Users/nitin/workspace/rough/benchmark_batch_crawler/{csv_name}")
print("batch_result: ", result)
print("status_count: ", statuses)
