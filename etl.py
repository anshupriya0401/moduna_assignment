import csv
import requests
import psycopg2

# Connecting to the DB
try:
    conn = psycopg2.connect(database='country_gdp', user='postgres', password='test', host='localhost', port='5432')
    print("Connection to db successful")
except:
    print("Connection to db failed")

# Declare cursor 
cur = conn.cursor()


#*******************************************************************************************************************************#


# Create and load gdp_source     
cur.execute("""
            DROP TABLE IF EXISTS moduna.gdp_source CASCADE;
            CREATE TABLE moduna.gdp_source(
                indicator_code          character varying(30) NOT NULL PRIMARY KEY,
                indicator_name          character varying(50),
                source_note             text,
                source_org              text
                );"""
            )
with open('Metadata_Indicator_API_NY.GDP.MKTP.CD_DS2_en_csv_v2_2593330.csv','r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute("INSERT INTO moduna.gdp_source VALUES (%s, %s, %s, %s)", [row[0],row[1],row[2],row[3]]
    )

conn.commit()

print("Finished loading table: GDP_SOURCE")
    
#*******************************************************************************************************************************#

# Create and Load data to country_detail_stg
cur.execute("""
            DROP TABLE IF EXISTS moduna.country_detail_stg CASCADE;
            CREATE TABLE moduna.country_detail_stg(
                country_code      character varying(3) NOT NULL PRIMARY KEY,
                country_name      character varying(60),
                region            character varying(40),
                income_group      character varying(30),
                special_notes     text
            );"""
            )

with open('Metadata_Country_API_NY.GDP.MKTP.CD_DS2_en_csv_v2_2593330.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute("INSERT INTO moduna.country_detail_stg VALUES (%s, %s, %s, %s, %s)", [row[0],row[4],row[1],row[2],row[3]]
    )

conn.commit()

#*******************************************************************************************************************************************#

# Discard records with invalid country name 
    
cur.execute("""
            DROP TABLE IF EXISTS moduna.country_detail CASCADE;
            CREATE TABLE moduna.country_detail(
                country_code      character varying(3) NOT NULL PRIMARY KEY,
                country_name      character varying(60),
                region            character varying(40),
                income_group      character varying(30),
                special_notes     text
            );
            
            INSERT INTO moduna.country_detail 
            SELECT * FROM moduna.country_detail_stg WHERE country_name not in 
                ('Africa Eastern and Southern','Africa Western and Central','Arab World','Bermuda','British Virgin Islands',
                 'Caribbean small states','Cayman Islands','Central Europe and the Baltics','Channel Islands','Congo, Rep.',
                 'Early-demographic dividend','East Asia & Pacific','East Asia & Pacific (excluding high income)',
                 'East Asia & Pacific (IDA & IBRD countries)','Europe & Central Asia','Europe & Central Asia (excluding high income)',
                 'Europe & Central Asia (IDA & IBRD countries)','European Union','Fragile and conflict affected situations','Gibraltar',
                 'Heavily indebted poor countries (HIPC)','High income','IBRD only','IDA & IBRD total','IDA blend','IDA only','IDA total',
                 'Isle of Man','Kosovo','Late-demographic dividend','Latin America & Caribbean','Latin America & Caribbean (excluding high income)',
                 'Latin America & the Caribbean (IDA & IBRD countries)','Least developed countries: UN classification',
                 'Low & middle income','Low income','Lower middle income','Macao SAR, China','Middle East & North Africa',
                 'Middle East & North Africa (excluding high income)','Middle East & North Africa (c & IBRD countries)','Middle income','North America',
                 'Not classified','OECD members','Other small states','Pacific island small states','Post-demographic dividend',
                 'Pre-demographic dividend','Small states','South Asia','South Asia (IDA & IBRD)','Sub-Saharan Africa (excluding high income)',
                 'Sub-Saharan Africa (IDA & IBRD countries)','Turks and Caicos Islands','Upper middle income','World');
                """
)

conn.commit()
        
print("Finished loading table: COUNTRY_DETAIL")

#*********************************************************************************************************************************************#


# Create and loable to store GDP data

cur.execute("""
            DROP TABLE IF EXISTS moduna.country_gdp_stg;
            CREATE TABLE moduna.country_gdp_stg(
                country_code          character varying(3),
                indicator_code        character varying(30),
                year                  character(4),
                gdp                   numeric(20,4)
                );
            """
            )

with open('API_NY.GDP.MKTP.CD_DS2_en_csv_v2_2593330.csv','r') as f:
    reader = csv.reader(f)
    for i in range(5):
        next(reader) #Skip the header row
    
    for row in reader:
        for x in range(1960, 2020):
            if (row[x-1956] != ''):
                cur.execute("""
                            INSERT INTO moduna.country_gdp_stg (country_code,indicator_code,year,gdp) 
                            VALUES (%s, %s, %s, %s);
                            """,[row[1],row[3],x,row[x-1956]]
                            )
conn.commit()


cur.execute("""               
            DROP TABLE IF EXISTS moduna.country_gdp CASCADE;
            CREATE TABLE moduna.country_gdp(
                id                    serial PRIMARY KEY,
                country_code          character varying(3) NOT NULL,
                indicator_code        character varying(30) NOT NULL,
                year                  character(4),
                gdp                   numeric(20,4),
                CONSTRAINT code_fk1 FOREIGN KEY(country_code) REFERENCES moduna.country_detail(country_code),
                CONSTRAINT code_fk2 FOREIGN KEY(indicator_code) REFERENCES moduna.gdp_source(indicator_code)
                );
           
            """
            )

conn.commit()

cur.execute("""
            INSERT INTO moduna.country_gdp (country_code,indicator_code,year,gdp) 
            SELECT a.country_code,a.indicator_code,a.year,a.gdp 
            FROM 
            moduna.country_gdp_stg a 
            LEFT JOIN 
            moduna.country_detail b 
            ON a.country_code = b.country_code WHERE b.country_code IS NOT NULL;
            """
            )

conn.commit()

print("Finished loading table: COUNTRY_GDP")

#*********************************************************************************************************************************************#

# Requesting data from API
response = requests.get("http://api.worldbank.org/v2/country/?format=json")

cur.execute("""
            DROP TABLE IF EXISTS moduna.country_list_stg;
            CREATE TABLE moduna.country_list_stg(
                country_code              character varying(3),
                country_name              character varying(60),
                country_iso2code          character(2),
                region_code               character(3),
                region_iso2code           character(2),
                region_desc               character varying(100),
                admin_region_code         character(3),
                admin_iso2code            character(2),
                admin_region_desc         character varying(100),
                income_level_code         character(3),
                income_level_iso2code     character(2),
                income_level_desc         character varying(100),
                lending_type_code         character(3),   
                lending_type_iso2code     character(2),
                lending_type_region_desc  character varying(100),
                capital_city              character varying(100),
                longitude                 character varying(30),
                latitude                  character varying(30));"""
                )
            
api_result = response.json()

for country_info in api_result[1]:
    country_list=[
        country_info['id'],
        country_info['name'],
        country_info['iso2Code'],
        country_info['region']['id'],
        country_info['region']['iso2code'],
        country_info['region']['value'],
        country_info['adminregion']['id'],
        country_info['adminregion']['iso2code'],
        country_info['adminregion']['value'],
        country_info['incomeLevel']['id'],
        country_info['incomeLevel']['iso2code'],        
        country_info['incomeLevel']['value'],
        country_info['lendingType']['id'],
        country_info['lendingType']['iso2code'],
        country_info['lendingType']['value'],
        country_info['capitalCity'],
        country_info['longitude'],
        country_info['latitude']
        ]   
    
    cur.execute("""
                INSERT INTO moduna.country_list_stg VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, country_list)    
               
conn.commit()

cur.execute("""
            DROP TABLE IF EXISTS moduna.country_list;
            CREATE TABLE moduna.country_list(
                country_code              character varying(3),
                country_name              character varying(60),
                country_iso2code          character(2),
                region_code               character(3),
                region_iso2code           character(2),
                region_desc               character varying(100),
                admin_region_code         character(3),
                admin_iso2code            character(2),
                admin_region_desc         character varying(100),
                income_level_code         character(3),
                income_level_iso2code     character(2),
                income_level_desc         character varying(100),
                lending_type_code         character(3),   
                lending_type_iso2code     character(2),
                lending_type_region_desc  character varying(100),
                capital_city              character varying(100),
                longitude                 character varying(30),
                latitude                  character varying(30),
                CONSTRAINT country_list_fk1 FOREIGN KEY(country_code) REFERENCES moduna.country_detail(country_code)   
                );"""
            )


cur.execute("""
            INSERT INTO moduna.country_list 
            SELECT a.* FROM 
            moduna.country_list_stg a 
            LEFT JOIN 
            moduna.country_detail b 
            ON a.country_code = b.country_code WHERE b.country_code IS NOT NULL;
            """
            )

conn.commit()

print("Finished loading table: COUNTRY_LIST")

#*********************************************************************************************************************************************#

print("Finished loading all the tables")

cur.close()

conn.close()
        