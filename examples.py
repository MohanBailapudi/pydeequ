from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.analyzers import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *

spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

df = spark.sparkContext.parallelize([
            Row(emp_name="mohan1", emp_age=21, emp_sal=90,emp_dept = 'Science',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan2", emp_age=22, emp_sal=900,emp_dept = 'Scocial',emp_email='mohan2@gmail.com'),
            Row(emp_name="mohan3", emp_age=23, emp_sal=9000,emp_dept = 'Maths',emp_email='mohan3@gmail.com'),
            Row(emp_name="mohan4", emp_age=24, emp_sal=90000,emp_dept = 'Biology',emp_email='mohan4@gmail.com'),
            Row(emp_name="mohan5", emp_age=25, emp_sal=900000,emp_dept = 'Science',emp_email='mohan5@gmail.com'),
            Row(emp_name="mohan6", emp_age=23, emp_sal=10,emp_dept = 'Maths',emp_email='mohan6@gmail'),
            Row(emp_name="mohan7", emp_age=24, emp_sal=190,emp_dept = 'Biology',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan8", emp_age=25, emp_sal=9220,emp_dept = 'Science',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan9", emp_age=24, emp_sal=9680,emp_dept = 'Maths',emp_email='mohan7@gmail.com'),
            Row(emp_name="mohan10", emp_age=-25, emp_sal=958560,emp_dept = 'Science',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan11", emp_age=25, emp_sal='',emp_dept = 'Scocial',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan12", emp_age=26, emp_sal=9585680,emp_dept = 'Biology',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan13", emp_age=27, emp_sal=None,emp_dept = 'Science',emp_email='mohan9@gmail.com'),
            Row(emp_name="mohan14", emp_age=28, emp_sal=958580,emp_dept = 'Maths',emp_email='mohan1@gmail'),
            Row(emp_name="mohan15", emp_age=25, emp_sal=9585680,emp_dept = 'Biology',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan16", emp_age=26, emp_sal=950,emp_dept = 'Science',emp_email='mohan1gmail.com'),
            Row(emp_name="mohan17", emp_age=27, emp_sal=950,emp_dept = 'Scocial',emp_email='mohan1@gmail.com'),
            Row(emp_name="mohan18", emp_age=28, emp_sal=None,emp_dept = 'Biology',emp_email='mohan1@gmail.com')]).toDF()

analysisResult = AnalysisRunner(spark) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("emp_sal")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

### Complete Profile on dataframe
result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

for col, profile in result.profiles.items():
    print(profile)


### Constraints
check = Check(spark, CheckLevel.Warning, "Review Check")

checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasMin("emp_sal", lambda x: x == 0) \
        .isComplete("emp_sal")  \
        .isUnique("emp_name")  \
        .isContainedIn("emp_dept", ["Scocial", "Science", "Maths"]) \
        .isNonNegative("emp_age")) \
        .containsEmail("emp_email")
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()
