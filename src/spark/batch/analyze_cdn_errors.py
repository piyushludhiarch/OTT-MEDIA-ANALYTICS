from pyspark import SparkConf, SparkContext


HDFS_INPUT = (
    "hdfs://localhost:9000/ott_media/raw/cdn_logs/cdn_logs.csv"
)


conf = (
    SparkConf()
    .setAppName("OTTCDNErrorAnalysis")
)

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


# Custom accumulators
total_requests = sc.accumulator(0)
error_4xx = sc.accumulator(0)
error_5xx = sc.accumulator(0)
malformed_records = sc.accumulator(0)


def parse_and_count(line):
    """
    Parse one CDN log record and update custom accumulators.
    """

    if line.startswith("timestamp,"):
        return None

    try:
        fields = line.split(",")

        if len(fields) != 7:
            malformed_records.add(1)
            return None

        timestamp = fields[0]
        region = fields[3]
        status_code = int(fields[4])

        total_requests.add(1)

        if 400 <= status_code < 500:
            error_4xx.add(1)

        elif 500 <= status_code < 600:
            error_5xx.add(1)

        return (
            region,
            timestamp,
            status_code
        )

    except Exception:
        malformed_records.add(1)
        return None


cdn_rdd = sc.textFile(HDFS_INPUT)

parsed_rdd = (
    cdn_rdd
    .map(parse_and_count)
    .filter(lambda record: record is not None)
)


# Force evaluation so accumulators are updated.
records = parsed_rdd.collect()


print("\n===== CDN SPARK CORE ANALYSIS =====")

print(
    "Total requests:",
    total_requests.value
)

print(
    "HTTP 4xx errors:",
    error_4xx.value
)

print(
    "HTTP 5xx errors:",
    error_5xx.value
)

print(
    "Malformed records:",
    malformed_records.value
)


# Region-level error analysis
region_errors = (
    sc.parallelize(records)
    .filter(
        lambda record: 400 <= record[2] < 600
    )
    .map(
        lambda record: (record[0], 1)
    )
    .reduceByKey(lambda a, b: a + b)
    .sortBy(
        lambda item: item[1],
        ascending=False
    )
    .collect()
)


print("\n===== HTTP ERRORS BY REGION =====")

for region, count in region_errors:
    print(
        f"{region}: {count} errors"
    )


# Time-bucket analysis
india_spike = (
    sc.parallelize(records)
    .filter(
        lambda record:
            record[0] == "India"
            and 400 <= record[2] < 600
            and "2026-09-01 12:" in record[1]
    )
    .count()
)


print("\n===== ERROR SPIKE ANALYSIS =====")

print(
    "India errors during "
    "2026-09-01 12:00-12:59:",
    india_spike
)

if india_spike > 0:
    print(
        "HTTP ERROR SPIKE DETECTED: "
        "India 12:00-13:00"
    )
else:
    print(
        "No HTTP error spike detected "
        "for the configured test window."
    )


print("\n===== CDN ANALYSIS COMPLETED =====")

sc.stop()