from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *
from data_frame import *

## Creating Spark Session
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

## Creating Sample DataFrame
df = spark.sparkContext.parallelize(df_rows).toDF()

## Analzises the data for examples Completeness,Distinctness,Datatype etc...
def analysis_runner(df,col_name):
    result_dict = dict()
    for i in col_name:
        analysisResult = AnalysisRunner(spark) \
                            .onData(df) \
                            .addAnalyzer(Size()) \
                            .addAnalyzer(Completeness(i)) \
                            .run()
        analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
        result_dict[i] = analysisResult_df
    return analysisResult_df


### Complete Profile on dataframe - Column level analysis is provided by this method
def column_profiler(df):
    result_dict = dict()
    result = ColumnProfilerRunner(spark) \
        .onData(df) \
        .run()
    ### Looping through the result object and printing the profile of each column
    for col, profile in result.profiles.items():
        result_dict[col] = profile
    return result_dict



### Constraints Checks
###'''Below example is cheking on completeness, uniqueness,allowed values, email pattern, datatype'''
def constriant_check_string(df,col_name):
    check = Check(spark, CheckLevel.Warning, "Review Check")
    result_dict = dit()
    for i in col_name:
        checkResult = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(
                check.isUnique("emp_name")  \
                .isNonNegative("emp_age") \
                .hasDataType("emp_age",ConstrainableDataTyoes.Integral,assertion=lambda x: x == 1.0) \
                ).run()
        ### Converting the result to dataframe
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
        result_dict[i] = checkResult_df
    return result_dict

def constriant_check_number(df, col_name):
    check = Check(spark, CheckLevel.Warning, "Review Check")
    result_dict = dit()
    for i in col_name:
        checkResult = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(
            check..isUnique("emp_name") \
                .isContainedIn("emp_dept", ["Scocial", "Science", "Maths"]) \
                .containsEmail("emp_email") \
                .hasDataType("emp_email", ConstrainableDataTyoes.String, assertion=lambda x: x == 1.0)).run()

        ### Converting the result to dataframe
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
        result_dict[i] = checkResult_df
    return result_dict

def main()
    analysis_runner(df,col_name).show()
    print(column_profiler(df))
    print(constriant_check_string(df,col_name))
    print(onstriant_check_number(df, col_name))
