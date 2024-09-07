from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, concat
from pyspark.sql.types import StringType, IntegerType


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Preview and Clean .dat File") \
    .getOrCreate()

# Define the path to your .dat file
file_path = "dec17pub.dat" 

output_file_path = 'outputs'


# Read the .dat file into an RDD
rdd = spark.sparkContext.textFile(file_path)

# Remove all white spaces from each line
def remove_whitespace(line):
    return line.replace(" ","0")

# Apply the function to remove white spaces
cleaned_rdd = rdd.map(remove_whitespace)

# Convert the cleaned RDD to a DataFrame
# Name single column of all data 'cleaned_data'
df = cleaned_rdd.map(lambda x: (x,)).toDF(["cleaned_data"])


#function to extract values from cleaned_data
def extract_field(value, start, length):
    return value[start:start + length].strip()


extract_household_id= udf(lambda x: extract_field(x, 0, 15), StringType())
extract_household_id_2= udf(lambda x: extract_field(x, 70, 5), StringType())


extract_interview_month= udf(lambda x: extract_field(x, 15, 2), StringType())
extract_interview_year= udf(lambda x: extract_field(x, 17, 4), StringType())


extract_final_outcome_code= udf(lambda x: extract_field(x, 23, 3), StringType())
def format_final_outcome(value, start, length):
    value_str = value[start:start + length].strip()
    if len(value_str) == 3:
        key = value_str
        map_value = {'001': 'FULLY COMPLETE CATI INTERVIEW','002': 'PARTIALLY COMPLETED CATI INTERVIEW','003': 'COMPLETE BUT PERSONAL VISIT REQUESTED NEXT MONTH',
            '004': 'PARTIAL, NOT COMPLETE AT CLOSEOUT','005': 'LABOR FORCE COMPLETE, SUPPLEMENT INCOMPLETE - CATI','006': 'LF COMPLETE, SUPPLEMENT DK ITEMS INCOMPLETE AT CLOSEOUT–ASEC ONLY',
            '020': 'HH OCCUPIED ENTIRELY BY ARMED FORCES MEMBERS OR ALL UNDER 15 YEARS OF AGE','201': 'CAPI COMPLETE','202': 'CALLBACK NEEDED',
            '203': 'SUFFICIENT PARTIAL - PRECLOSEOUT','204': 'SUFFICIENT PARTIAL - AT CLOSEOUT','205': 'LABOR FORCE COMPLETE, - SUPPL. INCOMPLETE - CAPI',
            '213': 'LANGUAGE BARRIER','214': 'UNABLE TO LOCATE','216': 'NO ONE HOME','217': 'TEMPORARILY ABSENT','218': 'REFUSED',
            '219': 'OTHER OCCUPIED - SPECIFY','223': 'ENTIRE HOUSEHOLD ARMED FORCES','224': 'ENTIRE HOUSEHOLD UNDER 15',
            '225': 'TEMP. OCCUPIED W/PERSONS WITH URE','226': 'VACANT REGULAR','227': 'VACANT - STORAGE OF HHLD FURNITURE',
            '228': 'UNFIT, TO BE DEMOLISHED','229': 'UNDER CONSTRUCTION, NOT READY','230': 'CONVERTED TO TEMP BUSINESS OR STORAGE',
            '231': 'UNOCCUPIED TENT OR TRAILER SITE','232': 'PERMIT GRANTED - CONSTRUCTION NOT STARTED','233': 'OTHER - SPECIFY',
            '240': 'DEMOLISHED','241': 'HOUSE OR TRAILER MOVED','242': 'OUTSIDE SEGMENT',
            '243': 'CONVERTED TO PERM. BUSINESS OR STORAGE','244': 'MERGED',
            '245': 'CONDEMNED','246': 'BUILT AFTER APRIL 1, 2000',
            '247': 'UNUSED SERIAL NO./LISTING SHEET LINE','248':'OTHER - SPECIFY',
            '256':'REMOVED DURING SUB-SAMPLING','257':'UNIT ALREADY HAD A CHANCE OF SELECTION'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_final_outcome_udf = udf(lambda x: format_final_outcome(x, 23, 3), StringType())


extract_type_of_housing_unit_code= udf(lambda x: extract_field(x, 30, 2), StringType())
def format_type_of_housing_unit(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
        '0': 'OTHER UNIT','1': 'HOUSE, APARTMENT, FLAT','2': 'HU IN NONTRANSIENT HOTEL, MOTEL, ETC.','3': 'HU PERMANENT IN TRANSIENT HOTEL, MOTEL',
        '4': 'HU IN ROOMING HOUSE','5': 'MOBILE HOME OR TRAILER W/NO PERM. ROOM ADDED','6': 'MOBILE HOME OR TRAILER W/1 OR MORE PERM. ROOMS ADDED',
        '7': 'HU NOT SPECIFIED ABOVE','8': 'QUARTERS NOT HU IN ROOMING OR BRDING HS','9': 'UNIT NOT PERM. IN TRANSIENT HOTL, MOTL',
        '10': 'UNOCCUPIED TENT SITE OR TRLR SITE','11': 'STUDENT QUARTERS IN COLLEGE DORM','12': 'OTHER UNIT NOT SPECIFIED ABOVE'
        }.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_type_of_housing_unit_udf = udf(lambda x: format_type_of_housing_unit(x, 30, 2), StringType())


extract_household_type_code= udf(lambda x: extract_field(x, 60, 2), StringType())
def format_household_type(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
            '0': 'NON-INTERVIEW HOUSEHOLD',
            '1': 'HUSBAND/WIFE PRIMARY FAMILY (NEITHER AF)',
            '2': 'HUSB/WIFE PRIM. FAMILY (EITHER/BOTH AF)',
            '3':'UNMARRIED CIVILIAN MALE-PRIM. FAM HHLDER', 
            '4': 'UNMARRIED CIV. FEMALE-PRIM FAM HHLDER',
            '5': 'PRIMARY FAMILY HHLDER-RP IN AF, UNMAR.',
            '6': 'CIVILIAN MALE PRIMARY INDIVIDUAL',
            '7': 'CIVILIAN FEMALE PRIMARY INDIVIDUAL',
            '8': 'PRIMARY INDIVIDUAL HHLD-RP IN AF',
            '9': 'GROUP QUARTERS WITH FAMILY',
            '10': 'GROUP QUARTERS WITHOUT FAMILY'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_household_type_udf = udf(lambda x: format_household_type(x, 60, 2), StringType())


extract_has_telephone_code= udf(lambda x: extract_field(x, 32, 2), StringType())
def format_has_telephone(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
            '1': 'YES',
            '2': 'NO'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_has_telephone_udf = udf(lambda x: format_has_telephone(x, 32, 2), StringType())


extract_has_telephone_reachable_code= udf(lambda x: extract_field(x, 34, 2), StringType())
def format_has_telephone_reachable(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
            '1': 'YES',
            '2': 'NO'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_has_telephone_reachable_udf = udf(lambda x: format_has_telephone_reachable(x, 34, 2), StringType())



extract_is_telephone_acceptable_code= udf(lambda x: extract_field(x, 36, 2), StringType())
def format_is_telephone_acceptable(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
            '1': 'YES',
            '2': 'NO'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_is_telephone_acceptable_udf = udf(lambda x: format_is_telephone_acceptable(x, 36, 2), StringType())



extract_type_of_interview_code= udf(lambda x: extract_field(x, 64, 2), StringType())
def format_extract_type_of_interview(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {
            '0': 'NONINTERVIEW/INDETERMINATE',
            '1': 'PERSONAL',
            '2' : 'TELEPHONE'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_extract_type_of_interview_udf = udf(lambda x: format_extract_type_of_interview(x, 64, 2), StringType())



extract_family_income_range_code= udf(lambda x: extract_field(x, 38, 2), StringType())
def format_extract_family_income_range(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {'1': 'LESS THAN $5,000',
                '2': '5,000 TO 7,499',
                '3': '7,500 TO 9,999',
                '4': '10,000 TO 12,499',
                '5': '12,500 TO 14,999',
                '6': '15,000 TO 19,999',
                '7': '20,000 TO 24,999',
                '8': '25,000 TO 29,999',
                '9': '30,000 TO 34,999',
                '10': '35,000 TO 39,999',
                '11': '40,000 TO 49,999',
                '12': '50,000 TO 59,999',
                '13': '60,000 TO 74,999',
                '14': '75,000 TO 99,999',
                '15': '100,000 TO 149,999',
                '16': '150,000 OR MORE'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_extract_family_income_range_udf = udf(lambda x: format_extract_family_income_range(x, 38, 2), StringType())



extract_geo_div_code= udf(lambda x: extract_field(x, 90, 1), StringType())
def format_extract_geo_div(value, start, length):
    value_int = int(value[start:start + length].strip())
    value_str = str(value_int)
    if len(value_str) <= 2:
        key = value_str
        map_value = {'1': 'NEW ENGLAND',
                '2': 'MIDDLE ATLANTIC', 
                '3': 'EAST NORTH CENTRAL',
                '4': 'WEST NORTH CENTRAL',
                '5': 'SOUTH ATLANTIC',
                '6': 'EAST SOUTH CENTRAL',
                '7': 'WEST SOUTH CENTRAL',
                '8': 'MOUNTAIN',
                '9': 'PACIFIC'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_extract_geo_div_udf = udf(lambda x: format_extract_geo_div(x, 90, 1), StringType())


extract_race_code= udf(lambda x: extract_field(x, 138, 2), StringType())
def format_extract_race(value, start, length):
    value_str = value[start:start + length].strip()
    if len(value_str) <= 2:
        key = value_str
        map_value = {'01': 'White Only',        
                '02': 'Black Only',          
                '03': 'American Indian, Alaskan Native Only',
                '04': 'Asian Only',
                '05': 'Hawaiian/Pacific Islander Only',
                '06': 'White-Black',      
                '07': 'White-AI',                      
                '08': 'White-Asian',                          
                '09': 'White-HP',                     
                '10': 'Black-AI',                             
                '11': 'Black-Asian',
                '12': 'Black-HP',                           
                '13': 'AI-Asian',                               
                '14': 'AI-HP',
                '15': 'Asian-HP',                         
                '16': 'W-B-AI',                       
                '17': 'W-B-A',
                '18': 'W-B-HP',                                         
                '19': 'W-AI-A',                         
                '20': 'W-AI-HP',    
                '21': 'W-A-HP',
                '22': 'B-AI-A',                     
                '23': 'W-B-AI-A',
                '24': 'W-AI-A-HP',                          
                '25': 'Other 3 Race Combinations',                          
                '26': 'Other 4 and 5 Race Combinations'}.get(key, 'Unknown')
        return map_value
    return "Invalid Format"
format_extract_race_udf = udf(lambda x: format_extract_race(x, 138, 2), StringType())



processed_df = df.withColumn('full_household_identifier', concat(extract_household_id(col('cleaned_data')), extract_household_id_2(col('cleaned_data'))))\
                       .withColumn('time_of_interview', concat(extract_interview_year(col('cleaned_data')),lit("/"),extract_interview_month(col('cleaned_data'))))\
                       .withColumn('final_survey_outcome_code', extract_final_outcome_code(col('cleaned_data')))\
                       .withColumn('final_survey_outcome', format_final_outcome_udf(col('cleaned_data')))\
                       .withColumn('type_of_housing_unit_code', extract_type_of_housing_unit_code(col('cleaned_data')))\
                       .withColumn('type_of_housing_unit', format_type_of_housing_unit_udf(col('cleaned_data')))\
                       .withColumn('household_type_code', extract_household_type_code(col('cleaned_data')))\
                       .withColumn('household_type', format_household_type_udf(col('cleaned_data')))\
                       .withColumn('has_telephone_code', extract_has_telephone_code(col('cleaned_data')))\
                       .withColumn('has_telephone', format_has_telephone_udf(col('cleaned_data')))\
                       .withColumn('has_telephone_reachable_code', extract_has_telephone_reachable_code(col('cleaned_data')))\
                       .withColumn('has_telephone_reachable', format_has_telephone_reachable_udf(col('cleaned_data')))\
                       .withColumn('is_telephone_acceptable_code', extract_is_telephone_acceptable_code(col('cleaned_data')))\
                       .withColumn('is_telephone_acceptable', format_is_telephone_acceptable_udf(col('cleaned_data')))\
                       .withColumn('type_of_interview_code', extract_type_of_interview_code(col('cleaned_data')))\
                       .withColumn('type_of_interview', format_extract_type_of_interview_udf(col('cleaned_data')))\
                       .withColumn('family_income_range_code', extract_family_income_range_code(col('cleaned_data')))\
                       .withColumn('family_income_range', format_extract_family_income_range_udf(col('cleaned_data')))\
                       .withColumn('geo_div_code', extract_geo_div_code(col('cleaned_data')))\
                       .withColumn('geo_div', format_extract_geo_div_udf(col('cleaned_data')))\
                       .withColumn('race_code', extract_race_code(col('cleaned_data')))\
                       .withColumn('race', format_extract_race_udf(col('cleaned_data')))\



# data frame with coded values included
mapped_data = processed_df.select('cleaned_data','full_household_identifier', 'time_of_interview', 'final_survey_outcome_code', 
    'final_survey_outcome','type_of_housing_unit_code', 'type_of_housing_unit', 'household_type_code', 'household_type',
    'has_telephone_code','has_telephone', 'has_telephone_reachable_code', 'has_telephone_reachable',
    'is_telephone_acceptable_code','is_telephone_acceptable', 'type_of_interview_code', 'type_of_interview',
    'family_income_range_code','family_income_range', 'geo_div_code', 'geo_div', 'race_code', 'race').limit(100)

mapped_data.write.format("org.apache.spark.sql.execution.datasources.v2.csv.CSVDataSourceV2").mode('overwrite').option("header", True).save(output_file_path+"/mapped_data")



# data frame without coded values
processed_df.select('full_household_identifier', 'time_of_interview', 
    'final_survey_outcome', 'type_of_housing_unit', 'household_type',
    'has_telephone', 'has_telephone_reachable',
    'is_telephone_acceptable', 'type_of_interview',
    'family_income_range','geo_div', 'race')



processed_df.createOrReplaceTempView('processed_data')


question_1 = spark.sql("""
    SELECT distinct(family_income_range), count(*) as responders_count
    FROM processed_data
    GROUP BY family_income_range
    """)


question_2 = spark.sql("""
    SELECT geo_div, race, COUNT(*) AS responder_count
    FROM processed_data
    GROUP BY geo_div, race
    ORDER BY responder_count DESC
    LIMIT 10;
    """)



question_3 = spark.sql("""
    SELECT COUNT(*) AS responder_count
    FROM processed_data
    WHERE has_telephone = 'NO'
    AND has_telephone_reachable = 'YES'
    AND is_telephone_acceptable = 'YES';
    """)


question_4 = spark.sql("""
    SELECT COUNT(*) AS responder_count
    FROM processed_data
    WHERE 
    has_telephone = 'YES' AND is_telephone_acceptable = 'NO'
    OR
    has_telephone_reachable = 'YES' AND is_telephone_acceptable = 'NO';
    """)


question_1.show(30,False)
question_2.show(30,False)
question_3.show(30,False)
question_4.show(30,False)

# df.show(30,False)