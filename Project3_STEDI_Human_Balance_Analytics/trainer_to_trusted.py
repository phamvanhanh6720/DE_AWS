import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1678771637189 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hanhpv-stedi-lakehouse-sample/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1678771637189",
)

# Script generated for node Renamed keys for Join Customer
RenamedkeysforJoinCustomer_node1678772016701 = ApplyMapping.apply(
    frame=StepTrainerLanding_node1678771637189,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoinCustomer_node1678772016701",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=CustomerCurated_node1,
    frame2=RenamedkeysforJoinCustomer_node1678772016701,
    keys1=["serialnumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1678765893896 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=["email", "phone", "`(right) serialNumber`"],
    transformation_ctx="DropFields_node1678765893896",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678768111256 = DynamicFrame.fromDF(
    DropFields_node1678765893896.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678768111256",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678768111256,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hanhpv-stedi-lakehouse-sample/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
