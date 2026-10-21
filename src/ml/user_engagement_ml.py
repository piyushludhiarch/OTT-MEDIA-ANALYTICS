from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when
)
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("OTTUserEngagementML")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# LOAD GOLD FACT TABLE
# ============================================================

gold_fact_path = (
    "hdfs://localhost:9000/ott_media/gold/fact_watch_events"
)

fact_watch_events = (
    spark.read
    .format("delta")
    .load(gold_fact_path)
)


# ============================================================
# USER ENGAGEMENT FEATURES
# ============================================================

user_features = (
    fact_watch_events
    .groupBy("user_id")
    .agg(
        spark_sum("watch_seconds").alias(
            "total_watch_seconds"
        ),
        spark_sum("buffer_seconds").alias(
            "total_buffer_seconds"
        ),
        count("*").alias(
            "total_events"
        ),
        countDistinct("content_id").alias(
            "unique_content_count"
        ),
        count(
            when(
                col("event_type") == "play",
                True
            )
        ).alias(
            "play_events"
        ),
        count(
            when(
                col("event_type") == "buffer",
                True
            )
        ).alias(
            "buffer_events"
        ),
        count(
            when(
                col("event_type") == "pause",
                True
            )
        ).alias(
            "pause_events"
        ),
        count(
            when(
                col("event_type") == "resume",
                True
            )
        ).alias(
            "resume_events"
        ),
        count(
            when(
                col("event_type") == "stop",
                True
            )
        ).alias(
            "stop_events"
        )
    )
)


print("\n===== USER ENGAGEMENT FEATURES =====")
print(
    "Number of users:",
    user_features.count()
)

user_features.show(
    10,
    truncate=False
)

user_features.printSchema()


# ============================================================
# FEATURE COLUMNS
# ============================================================

feature_columns = [
    "total_watch_seconds",
    "total_buffer_seconds",
    "total_events",
    "unique_content_count",
    "play_events",
    "buffer_events",
    "pause_events",
    "resume_events",
    "stop_events"
]


# Collect only the already-aggregated user records.
feature_rows = (
    user_features
    .select(
        "user_id",
        *feature_columns
    )
    .collect()
)

user_ids = [
    row["user_id"]
    for row in feature_rows
]

X = [
    [
        row[column]
        for column in feature_columns
    ]
    for row in feature_rows
]


# ============================================================
# STANDARDIZE FEATURES FOR K-MEANS
# ============================================================

scaler = StandardScaler()

X_scaled = scaler.fit_transform(X)


# ============================================================
# K-MEANS USER ENGAGEMENT CLUSTERING
# ============================================================

kmeans = KMeans(
    n_clusters=3,
    random_state=42,
    n_init=10
)

cluster_labels = kmeans.fit_predict(
    X_scaled
)


print(
    "\n===== K-MEANS USER ENGAGEMENT CLUSTERS ====="
)

for user_id, cluster in list(
    zip(user_ids, cluster_labels)
)[:10]:
    print(
        f"User {user_id} -> "
        f"Engagement Cluster {cluster}"
    )


# ============================================================
# CLUSTER DISTRIBUTION
# ============================================================

print("\nCluster distribution:")

for cluster_id in sorted(
    set(cluster_labels)
):
    count_users = sum(
        1
        for label in cluster_labels
        if label == cluster_id
    )

    print(
        f"Cluster {cluster_id}: "
        f"{count_users} users"
    )


# ============================================================
# CLUSTER PROFILE
# ============================================================

cluster_analysis = []

for cluster_id in sorted(
    set(cluster_labels)
):

    cluster_rows = [
        row
        for row, label in zip(
            feature_rows,
            cluster_labels
        )
        if label == cluster_id
    ]

    cluster_size = len(
        cluster_rows
    )

    avg_values = {}

    for column in feature_columns:

        column_index = (
            feature_columns.index(column)
        )

        avg_values[column] = (
            sum(
                row[column_index]
                for row in cluster_rows
            )
            / cluster_size
        )

    cluster_analysis.append(
        (
            cluster_id,
            cluster_size,
            avg_values[
                "total_watch_seconds"
            ],
            avg_values[
                "total_buffer_seconds"
            ],
            avg_values[
                "total_events"
            ],
            avg_values[
                "unique_content_count"
            ],
            avg_values[
                "play_events"
            ],
            avg_values[
                "buffer_events"
            ],
            avg_values[
                "pause_events"
            ],
            avg_values[
                "resume_events"
            ],
            avg_values[
                "stop_events"
            ]
        )
    )


print("\n===== CLUSTER PROFILE =====")

print(
    "Cluster | Users | Avg Watch | Avg Buffer | "
    "Avg Events | Avg Content | Avg Plays | "
    "Avg Buffers | Avg Pauses | Avg Resumes | Avg Stops"
)


for row in cluster_analysis:

    print(
        f"{row[0]:7} | "
        f"{row[1]:5} | "
        f"{row[2]:9.2f} | "
        f"{row[3]:10.2f} | "
        f"{row[4]:10.2f} | "
        f"{row[5]:11.2f} | "
        f"{row[6]:9.2f} | "
        f"{row[7]:11.2f} | "
        f"{row[8]:10.2f} | "
        f"{row[9]:11.2f} | "
        f"{row[10]:9.2f}"
    )


# ============================================================
# BUSINESS INTERPRETATION
# ============================================================

cluster_labels_map = {
    0: "High Activity - QoE Concern",
    1: "Lower Engagement",
    2: "High Engagement"
}


print(
    "\n===== BUSINESS INTERPRETATION ====="
)

for cluster_id, label in (
    cluster_labels_map.items()
):
    print(
        f"Cluster {cluster_id}: {label}"
    )


# ============================================================
# ENGAGEMENT REGRESSION
# ============================================================

regression_features = [
    "total_events",
    "unique_content_count",
    "play_events",
    "buffer_events",
    "pause_events",
    "resume_events",
    "stop_events",
    "total_buffer_seconds"
]


X_regression = [
    [
        row[
            feature_columns.index(column)
        ]
        for column in regression_features
    ]
    for row in feature_rows
]


y_regression = [
    row[
        feature_columns.index(
            "total_watch_seconds"
        )
    ]
    for row in feature_rows
]


print(
    "\n===== REGRESSION DATASET ====="
)

print(
    "Input features:",
    regression_features
)

print(
    "Target: total_watch_seconds"
)

print(
    "Number of users:",
    len(X_regression)
)


# ============================================================
# TRAIN / TEST SPLIT
# ============================================================

X_train, X_test, y_train, y_test = (
    train_test_split(
        X_regression,
        y_regression,
        test_size=0.2,
        random_state=42
    )
)


print(
    "\n===== REGRESSION TRAIN/TEST SPLIT ====="
)

print(
    "Training users:",
    len(X_train)
)

print(
    "Testing users:",
    len(X_test)
)


# ============================================================
# LINEAR REGRESSION MODEL
# ============================================================

regression_model = LinearRegression()

regression_model.fit(
    X_train,
    y_train
)


# ============================================================
# PREDICTIONS
# ============================================================

y_pred = regression_model.predict(
    X_test
)


# ============================================================
# REGRESSION METRICS
# ============================================================

mae = mean_absolute_error(
    y_test,
    y_pred
)

r2 = r2_score(
    y_test,
    y_pred
)


print(
    "\n===== ENGAGEMENT REGRESSION RESULTS ====="
)

print(
    f"Mean Absolute Error: "
    f"{mae:.2f} seconds"
)

print(
    f"R2 Score: {r2:.4f}"
)


# ============================================================
# BASELINE COMPARISON
# ============================================================

baseline_prediction = (
    sum(y_train)
    / len(y_train)
)

baseline_mae = (
    sum(
        abs(
            actual -
            baseline_prediction
        )
        for actual in y_test
    )
    / len(y_test)
)


print(
    f"Baseline MAE: "
    f"{baseline_mae:.2f} seconds"
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()