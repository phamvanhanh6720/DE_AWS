import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1678774505999 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1678774505999",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1678774938738 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hanhpv-stedi-lakehouse-sample/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1678774938738",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1678774505999,
    frame2=AccelerometerTrusted_node1678774938738,
    keys1=["sensorreadingtime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1678775028237 = DropFields.apply(
    frame=Join_node2,
    paths=["sensorreadingtime"],
    transformation_ctx="DropFields_node1678775028237",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1678775028237,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://hanhpv-stedi-lakehouse-sample/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
