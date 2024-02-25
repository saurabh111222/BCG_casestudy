import re

def load_csv_file(spark, file_path):
    df = spark.read.format('csv').option('inferSchema','true').option('header', 'true').load(file_path)
    return df

def save_results(df, target_path, format):
    df.write.format(format).option('header', 'true').mode('overwrite').option('path', target_path).save()


def extract_numeric(value):
    numeric_value = re.search(r'\d+', value)
    if numeric_value:
        return int(numeric_value.group())
    else:
        return None