from src.utility.report_lib import write_output
# from  pyspark.sql import SparkSession
#
#
# spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()


def records_only_in_source(source_df, target_df, key_columns):
    """Validate records present only in the source."""
    only_in_source = source_df.select(key_columns).exceptAll(target_df.select(key_columns))
    only_in_source.show()
    count_only_in_source = only_in_source.count()
    if count_only_in_source > 0:
        failed_records = only_in_source.limit(5).collect() # Get the first 5 failing rows
        print("failed records", failed_records)
        failed_preview = [row.asDict() for row in failed_records]
        print("failed_preview", failed_preview)  # Convert rows to a dictionary for display
        status = "FAIL"
        print("There are extra records in source and extra records count is ", only_in_source.count())
        print("There are extra records in source and validation status is ", status)
        write_output(
            "Records Only in Source",
            status,
            f"Count: {count_only_in_source}, Sample Failed Records: {failed_preview}"
        )

    else:
        status = "PASS"
        write_output("Records Only in Source", status, "No extra records found in source.")
        print("There are no extra records found in source and validation status is ", status)

    return status

#
# source = spark.read.csv("C:\\Users\\kalya\\PycharmProjects\\taf_dec\\input_files\\Contact_info.csv", header=True, inferSchema = True)
# target = spark.read.csv("C:\\Users\\kalya\\PycharmProjects\\taf_dec\\input_files\\Contact_info_t.csv", header=True, inferSchema = True)
#
# records_only_in_source(source, target, ['Identifier'])

