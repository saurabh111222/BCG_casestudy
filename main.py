from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.transformations import Transform
from definition import output_path_list

spark = SparkSession. \
builder. \
master('local[*]'). \
getOrCreate()

transform = Transform(spark)

print(f'Answer 1: {transform.find_counts_of_male_deaths(output_path_list["first"], "csv")}')

print(f'Answer 2: {transform.vehicles_booked_for_crashes(output_path_list["second"], "csv")}')

print(f'Answer 3: {transform.crashes_driver_died_airbag_not_deployed(output_path_list["third"], "csv")}')

print(f'Answer 4: {transform.drivers_with_valid_lic_hitNrun(output_path_list["fourth"], "csv")}')

print(f'Answer 5: {transform.states_with_crashes_nofemale_involved(output_path_list["fifth"], "csv")}')

print(f'Answer 6: {transform.top_3to5_makers_contri_to_deaths(output_path_list["sixth"], "csv")}')

print(f'Answer 7: {transform.top_ethnic_usergroup_for_bodystyle(output_path_list["seventh"], "csv")}')

print(f'Answer 8: {transform.top_zipcodes_with_crashes_and_alcohol(output_path_list["eight"], "csv")}')

print(f'Answer 9: {transform.crashes_with_nodamage_damagelevel_above4(output_path_list["ninth"], "csv")}')

print(f'Answer 10: {transform.crashes_with_nodamage_damagelevel_above4(output_path_list["tenth"], "csv")}')
