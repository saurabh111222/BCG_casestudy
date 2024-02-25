from definition import dataset_path_list, output_path_list
from utils.utilities import load_csv_file, save_results, extract_numeric
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


class Transform:
    def __init__(self, spark):
        self.charges_df = load_csv_file(spark, dataset_path_list['Charges'])
        self.damages_df = load_csv_file(spark, dataset_path_list['Damages'])
        self.endorse_df = load_csv_file(spark, dataset_path_list['Endorse'])
        self.primary_person_df = load_csv_file(spark, dataset_path_list['Primary_Person'])
        self.units_df = load_csv_file(spark, dataset_path_list['Units'])
        self.restrict_df = load_csv_file(spark, dataset_path_list['Restrict'])

    def find_counts_of_male_deaths(self, output_path, format):
        df = self.primary_person_df.filter("PRSN_GNDR_ID == 'MALE' AND PRSN_INJRY_SEV_ID == 'KILLED'"). \
        groupBy('CRASH_ID').agg(count('*').alias('death_count')).filter('death_count > 2')
        save_results(df,output_path, format)
        result = df.first()
        return result.death_count if result else 0
    
    def vehicles_booked_for_crashes(self, output_path, format):
        df = self.units_df.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE')).select('VIN').distinct()
        save_results(df, output_path, format)
        result = df.count()
        return result
    
    def crashes_driver_died_airbag_not_deployed(self, output_path, format):
        makers_df = self.units_df.filter('VEH_MAKE_ID != "NA"').select('CRASH_ID', 'VEH_MAKE_ID').distinct()
        
        drivers_dead_df = self.primary_person_df.filter('PRSN_TYPE_ID == "DRIVER" and PRSN_INJRY_SEV_ID == "KILLED"'). \
        select('CRASH_ID','PRSN_TYPE_ID', 'PRSN_INJRY_SEV_ID')
        
        df = drivers_dead_df.join(makers_df, makers_df.CRASH_ID==drivers_dead_df.CRASH_ID, 'inner'). \
        select(drivers_dead_df.CRASH_ID, makers_df.VEH_MAKE_ID).groupBy('VEH_MAKE_ID'). \
        agg(countDistinct("CRASH_ID").alias('distinct_crashes')).sort('distinct_crashes', ascending=False).limit(5)
        
        save_results(df, output_path, format)
        result = [row[0] for row in df.collect()]
        return result
    
    def drivers_with_valid_lic_hitNrun(self, output_path, format):
        hnr_crashes_df = self.units_df.filter('VEH_HNR_FL IS NOT NULL AND VEH_HNR_FL == "Y" AND VIN != "UNKNOWN" AND VIN != "UNK"'). \
        selectExpr('CRASH_ID', 'VEH_HNR_FL as HNR_FLAG', 'VIN').distinct()

        licensed_drivers_df = self.primary_person_df.filter(col('DRVR_LIC_TYPE_ID').contains('DRIVER LIC')). \
        select('CRASH_ID', 'DRVR_LIC_TYPE_ID')

        df = licensed_drivers_df.join(hnr_crashes_df, hnr_crashes_df.CRASH_ID==licensed_drivers_df.CRASH_ID, 'inner'). \
        select(hnr_crashes_df.VIN).distinct()

        save_results(df, output_path, format)
        return df.count()
    
    def states_with_crashes_nofemale_involved(self, output_path, format):
        df = self.primary_person_df.filter('DRVR_LIC_STATE_ID != "NA" AND PRSN_GNDR_ID == "MALE"'). \
        select('CRASH_ID', 'PRSN_GNDR_ID', 'DRVR_LIC_STATE_ID'). \
        groupBy('DRVR_LIC_STATE_ID').agg(countDistinct('CRASH_ID').alias('count')). \
        sort('count', ascending=False).limit(1)

        save_results(df, output_path, format)
        return df.first()[0]
    
    def top_3to5_makers_contri_to_deaths(self, output_path, format):
        df = self.units_df.selectExpr('VEH_MAKE_ID', '(DEATH_CNT + TOT_INJRY_CNT) as TOTAL_INJ'). \
        groupBy('VEH_MAKE_ID').agg(sum(col('TOTAL_INJ')).alias('INJ_MAKE')). \
        withColumn('rn', row_number().over(Window.orderBy(col('INJ_MAKE').desc()))). \
        filter((col('rn')>=3) & (col('rn')<6)).drop('rn')

        save_results(df, output_path, format)
        result = [row[0] for row in df.collect()]
        return result
    
    def top_ethnic_usergroup_for_bodystyle(self, output_path, format):
        body_style_df = self.units_df.filter(~col('VEH_BODY_STYL_ID'). \
        isin(["NA", "UNKNOWN", "NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)"])). \
        select('CRASH_ID', 'VEH_BODY_STYL_ID')
        ethnic_df = self.primary_person_df.filter(~col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN"])). \
        select('CRASH_ID', 'PRSN_ETHNICITY_ID')

        df = body_style_df.join(ethnic_df, on=['CRASH_ID'], how='inner'). \
        groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID'). \
        count(). \
        withColumn('rn', row_number().over(Window.partitionBy('VEH_BODY_STYL_ID').orderBy('count'))).filter('rn == 1').drop('count', 'rn')

        save_results(df, output_path, format)
        df.show(truncate=False)

    def top_zipcodes_with_crashes_and_alcohol(self, output_path, format):
        contrib_factors_df = self.units_df.filter(col('CONTRIB_FACTR_1_ID').contains("ALCOHOL") | col('CONTRIB_FACTR_2_ID').contains("ALCOHOL")). \
        select("CRASH_ID", "CONTRIB_FACTR_1_ID", "CONTRIB_FACTR_2_ID")
        zip_df = self.primary_person_df.filter("DRVR_ZIP != 'NA'").select("CRASH_ID", "DRVR_ZIP")
        
        df = contrib_factors_df.join(zip_df, on=["CRASH_ID"], how="inner").groupBy("DRVR_ZIP").count().orderBy("count", ascending=False).limit(5)
        save_results(df, output_path, format)

        df.show(truncate=False)
    
    def crashes_with_nodamage_damagelevel_above4(self, output_path, format):
        property_damage_df = self.damages_df.filter(col('DAMAGED_PROPERTY').contains('NONE'))

        extract_numeric_udf = udf(extract_numeric)
        damage_level_df = self.units_df.withColumn("VEH_DMAG_SCL_1_ID", extract_numeric_udf(self.units_df["VEH_DMAG_SCL_1_ID"]).cast('integer')). \
        withColumn("VEH_DMAG_SCL_2_ID", extract_numeric_udf(self.units_df["VEH_DMAG_SCL_2_ID"]).cast('integer')). \
        filter(col('VEH_DMAG_SCL_1_ID').isNotNull() & col('VEH_DMAG_SCL_2_ID').isNotNull()). \
        filter(col('VEH_DMAG_SCL_1_ID').isin([5, 6, 7]) & col('VEH_DMAG_SCL_2_ID').isin([5, 6, 7])). \
        filter(col('FIN_RESP_TYPE_ID').contains('PROOF OF LIABILITY INSURANCE')). \
        select('CRASH_ID', 'VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID', 'FIN_RESP_TYPE_ID')

        df = damage_level_df.join(property_damage_df, on=['CRASH_ID'], how='inner')
        save_results(df, output_path, format)
        return df.count()
    
    def vehicles_charged_with_speed_offences(self, output_path, format):
        top_25_states_list = self.units_df.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()). \
        groupBy('VEH_LIC_STATE_ID').count(). \
        orderBy('count', ascending=False).limit(25).collect()
        top_25_states_list = [row[0] for row in top_25_states_list]

        top_10_colors_list = self.units_df.filter(col("VEH_COLOR_ID").cast("int").isNull()). \
        groupBy('VEH_COLOR_ID').count(). \
        orderBy('count', ascending=False).limit(10).collect()
        top_10_colors_list = [row[0] for row in top_10_colors_list]

        df = self.charges_df.filter(col('CHARGE').contains('SPEED')). \
        select("CRASH_ID", "CHARGE"). \
        join(self.units_df, on=['CRASH_ID'], how='inner'). \
        join(self.primary_person_df, on=['CRASH_ID'], how='inner'). \
        filter(self.primary_person_df.DRVR_LIC_TYPE_ID.contains('DRIVER LIC')). \
        filter(self.units_df.VEH_LIC_STATE_ID.isin(top_25_states_list)). \
        filter(self.units_df.VEH_COLOR_ID.isin(top_10_colors_list)). \
        groupBy('VEH_MAKE_ID').count().orderBy('count', ascending=False).limit(5)

        save_results(df, output_path, format)
        df.show()
        



    

    
    




        
        
    
        
    

    


    



    