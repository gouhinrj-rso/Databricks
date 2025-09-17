from pyspark.sql.functions import col, expr, try_to_timestamp, regexp_replace, when

date_cols = [
    "Case_Open_Date",
    "Closed_AutoClosed_Date",
    "MX_Scheduled_Date",
    "MX_Recommended_Date",
    "MX_Completed_Date",
    "JCN_Ant_Scheduled_MX_Date",
]
date_format = "M/d/yyyy H:mm"
for c in date_cols:
    if c in df.columns:
        df = df.withColumn(
            c,
            when(
                col(c).rlike(r"^\d{1,2}/\d{1,2}/\d{2} \d{1,2}:\d{2}$"),
                regexp_replace(col(c), r"(\d{1,2}/\d{1,2}/)(\d{2})( \d{1,2}:\d{2})", r"$120$2$3")
            ).otherwise(col(c))
        )
        df = df.withColumn(
            c,
            try_to_timestamp(col(c), date_format)
        )
if "SERIAL_NUMBER" in df.columns:
    df = df.withColumn("SERIAL_NUMBER", col("SERIAL_NUMBER").cast("string"))
if set(["Closed_AutoClosed_Date", "Case_Open_Date"]).issubset(df.columns):
    df = df.withColumn(
        "Days_Open_to_Close",
        expr("datediff(Closed_AutoClosed_Date, Case_Open_Date)")
    ).withColumn(
        "Days_Open_to_AutoClosed",
        expr("datediff(Closed_AutoClosed_Date, Case_Open_Date)")
    )
if set(["MX_Completed_Date", "MX_Scheduled_Date"]).issubset(df.columns):
    df = df.withColumn(
        "Days_Sched_to_Comp",
        expr("datediff(MX_Completed_Date, MX_Scheduled_Date)")
    )
if set(["MX_Completed_Date", "MX_Recommended_Date"]).issubset(df.columns):
    df = df.withColumn(
        "Days_Rec_to_Comp",
        expr("datediff(MX_Completed_Date, MX_Recommended_Date)")
    )
if set(["MX_Completed_Date", "JCN_Ant_Scheduled_MX_Date"]).issubset(df.columns):
    df = df.withColumn(
        "Days_AntSched_to_Comp",
        expr("datediff(MX_Completed_Date, JCN_Ant_Scheduled_MX_Date)")
    )
display(df)
display(df.count())
