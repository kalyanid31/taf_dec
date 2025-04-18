from src.utility.report_lib import write_output
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()


def records_only_in_target(source_df, target_df, key_columns):
    """Validate records present only in the target."""
    only_in_target = target_df.select(key_columns).exceptAll(source_df.select(key_columns))
    only_in_target.show()
    count_only_in_target = only_in_target.count()
    if count_only_in_target > 0:
        failed_records = only_in_target.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        print("There are extra records in target and extra records count is ", only_in_target.count())
        print("There are extra records in target and validation status is ", status)
        write_output(
            validation_type="Records Only in Target",
            status=status,
            details=f"Count: {count_only_in_target}, Sample Failed Records: {failed_preview}"
        )

    else:
        status = "PASS"
        print("There are no extra records found in source and validation status is ", status)
        write_output(
            validation_type="Records Only in Target",
            status=status,
            details="No extra records found in target.")
    return status


# source = spark.read.csv("C:\\Users\\kalya\\PycharmProjects\\taf_dec\\input_files\\Contact_info.csv", header=True, inferSchema=True)
# target = spark.read.csv("C:\\Users\\kalya\\PycharmProjects\\taf_dec\\input_files\\Contact_info_t.csv", header=True, inferSchema=True)
#
# records_only_in_target(source, target, ['Identifier'])













#
# def records_only_in_target(source_df, target_df, key_columns):
#     """Validate records present only in the target."""
#     only_in_target = target_df.select(key_columns).exceptAll(source_df.select(key_columns))
#     only_in_target.show()
#     count_only_in_target = only_in_target.count()
#     if count_only_in_target > 0:
#         failed_records = only_in_target.limit(5).collect()  # Get the first 5 failing rows
#         failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
#         status = "FAIL"
#         # print("There are extra records in target and extra records count is ", only_in_target.count())
#         # print("There are extra records in target and validation status is ", status)
#         write_output(
#             validation_type="Records Only in Target",
#             status=status,
#             details=f"Count: {count_only_in_target}, Sample Failed Records: {failed_preview}"
#         )
#
#     else:
#         status = "PASS"
#         print("There are no extra records found in target and validation status is ", status)
#         write_output(
#             validation_type="Records Only in Target",
#             status=status,
#             details="No extra records found in target.")
#     return status
#
