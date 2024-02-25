from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.transformations import Transform
from definition import output_path_list


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("BCG_casestudy") \
        .getOrCreate()

    transform = Transform(spark)
    
    # Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2
    print(f'Analytics 1: {transform.find_counts_of_male_deaths(output_path_list["first"], "csv")}')

    # Analysis 2: How many two wheelers are booked for crashes?
    print(f'Analysis 2: {transform.vehicles_booked_for_crashes(output_path_list["second"], "csv")}')

    # Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy
    print(f'Analysis 3: {transform.crashes_driver_died_airbag_not_deployed(output_path_list["third"], "csv")}')

    # Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
    print(f'Analysis 4: {transform.drivers_with_valid_lic_hitNrun(output_path_list["fourth"], "csv")}')

    # Analysis 5: Which state has highest number of accidents in which females are not involved?
    print(f'Analysis 5: {transform.states_with_crashes_nofemale_involved(output_path_list["fifth"], "csv")}')

    # Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print(f'Analysis 6: {transform.top_3to5_makers_contri_to_deaths(output_path_list["sixth"], "csv")}')

    # Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print(f'Analysis 7: {transform.top_ethnic_usergroup_for_bodystyle(output_path_list["seventh"], "csv")}')

    # Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    print(f'Analysis 8: {transform.top_zipcodes_with_crashes_and_alcohol(output_path_list["eight"], "csv")}')

    # Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print(f'Analysis 9: {transform.crashes_with_nodamage_damagelevel_above4(output_path_list["ninth"], "csv")}')

    # Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    print(f'Analysis 10: {transform.crashes_with_nodamage_damagelevel_above4(output_path_list["tenth"], "csv")}')


    spark.stop()
