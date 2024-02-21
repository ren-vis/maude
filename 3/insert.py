import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import boto3
from typing import Dict, List, Union
import json
import os
import subprocess
import sys
from zipfile import ZipFile
from urllib import request

from awsglue.utils import getResolvedOptions
target_bucket = 'sim-prd001-wl-diu-rsh001-primary-data-bucket'
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s] [%(levelname)s] - %(message)s', )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
bucket_folder = 'transformed/public/maude/'
required_packages= "requests,beautifulsoup4"
pipeline_secret_id ='global/PipelineCredentials'
region = 'eu-west-1'
s3 = boto3.resource('s3', region_name=region)
s3_bucket = s3.Bucket(target_bucket)
s3_client = boto3.client('s3')
session = boto3.session.Session()
s3_Session = session.client('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
def get_secret(secret_name: str, region: str) -> Dict[str, str]:
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(get_secret_value_response['SecretString'])
    return secrets
    
def install_python_packages(packages: Union[str, List], user, password, dest='libs', verbose=False):
    """
    Install python modules directly from artifactory
    # WARNING: Makes test loop slow.
    # pip install cache MAY be reused between job runs.
    """
    if isinstance(packages, str):
        packages = packages.split(",")

    if verbose:
        print("Checking pip version.")
        subprocess.check_call([sys.executable, "-m", "pip", "--version"])

    index_url = "packages.schroders.com/artifactory/api/pypi/pypi/simple"
    target_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), dest)
    if verbose: print("target dir: {}".format(target_dir))

    print('Installing python packages..')
    os.makedirs(target_dir, exist_ok=True)
    subprocess.check_call([
                              sys.executable, "-m", "pip", "install", *packages,
                              "--index-url", 'https://{}:{}@{}'.format(user, password, index_url),
                              '-t', target_dir]
                          + (["-v"] if verbose else [])
                          )
    print('Installed python packages..')

    print(f"modifying sys.path...")
    sys.path.insert(1, target_dir)
    print("sys.path:{}".format(sys.path))

required_packages= "jaydebeapi"
pipelinecred = get_secret(pipeline_secret_id,region)
PIPELINE_USER = pipelinecred['pipelineUser']
PIPELINE_PASSWORD = pipelinecred['pipelinePassword']
install_python_packages(required_packages, PIPELINE_USER, PIPELINE_PASSWORD)

import jaydebeapi as jdbc


FOIDEV_TABLE_COLUMNS = (('mdr_report_key','nvarchar(50)',1),
                         ('device_event_key','nvarchar(50)',0),
                         ('implant_flag','nvarchar(10)',0),
                         ('date_removed_flag','nvarchar(50)',0),
                         ('device_sequence_no','nvarchar(50)',1),
                         ('date_received','date',0),
                         ('brand_name','nvarchar(600)',0),
                         ('generic_name','nvarchar(600)',0),
                         ('manufacturer_d_name','nvarchar(200)',0),
                         ('manufacturer_d_address_1','nvarchar(200)',0),
                         ('manufacturer_d_address_2','nvarchar(200)',0),
                         ('manufacturer_d_city','nvarchar(200)',0),
                         ('manufacturer_d_state_code','nvarchar(100)',0),
                         ('manufacturer_d_zip_code','nvarchar(50)',0),
                         ('manufacturer_d_zip_code_ext','nvarchar(50)',0),
                         ('manufacturer_d_country_code','nvarchar(50)',0),
                         ('manufacturer_d_postal_code','nvarchar(70)',0),
                         ('expiration_date_of_device','nvarchar(100)',0),
                         ('model_number','nvarchar(300)',0),
                         ('catalog_number','nvarchar(200)',0),
                         ('lot_number','nvarchar(100)',0),
                         ('other_id_number','nvarchar(200)',0),
                         ('device_operator','nvarchar(50)',0),
                         ('device_availability','nvarchar(50)',0),
                         ('date_returned_to_manufacturer','date',0),
                         ('device_report_product_code','nvarchar(50)',0),
                         ('device_age_text','nvarchar(50)',0),
                         ('device_evaluated_by_manufactur','nvarchar(100)',0),
                         ('combination_product_flag','nvarchar(100)',0),
                         ('udi-di','nvarchar(100)',0),
                         ('udi-public','nvarchar(100)',0));
         
FOITEXT_TABLE_COLUMNS = (('mdr_report_key', 'nvarchar(50)',1),
                         ('mdr_text_key', 'nvarchar(50)',1),
                         ('text_type_code', 'nvarchar(50)',0),
                         ('patient_sequence_number', 'int',0),
                         ('date_report', 'date',0),
                         ('text', 'nvarchar(max)',0))
                           
MDRFOI_TABLE_COLUMNS = (('mdr_report_key','nvarchar(50)',1),
                         ('event_key','nvarchar(50)',1),
                         ('report_number','nvarchar(50)',0),
                         ('report_source_code','nvarchar(50)',0),
                         ('manufacturer_link_flag_','nvarchar(50)',0),
                         ('number_devices_in_event','int',0),
                         ('number_patients_in_event','nvarchar(50)',0),
                         ('date_received','date',0),
                         ('adverse_event_flag','nvarchar(50)',0),
                         ('product_problem_flag','nvarchar(50)',0),
                         ('date_report','date',0),
                         ('date_of_event','date',0),
                         ('reprocessed_and_reused_flag','nvarchar(50)',0),
                         ('reporter_occupation_code','nvarchar(50)',0),
                         ('health_professional','nvarchar(50)',0),
                         ('initial_report_to_fda','nvarchar(50)',0),
                         ('date_facility_aware','date',0),
                         ('report_date','date',0),
                         ('report_to_fda','nvarchar(50)',0),
                         ('date_report_to_fda','date',0),
                         ('event_location','nvarchar(50)',0),
                         ('date_report_to_manufacturer','date',0),
                         ('manufacturer_contact_t_name','nvarchar(50)',0),
                         ('manufacturer_contact_f_name','nvarchar(50)',0),
                         ('manufacturer_contact_l_name','nvarchar(50)',0),
                         ('manufacturer_contact_street_1','nvarchar(100)',0),
                         ('manufacturer_contact_street_2','nvarchar(100)',0),
                         ('manufacturer_contact_city','nvarchar(50)',0),
                         ('manufacturer_contact_state','nvarchar(50)',0),
                         ('manufacturer_contact_zip_code','nvarchar(50)',0),
                         ('manufacturer_contact_zip_ext','nvarchar(50)',0),
                         ('manufacturer_contact_country','nvarchar(50)',0),
                         ('manufacturer_contact_postal','nvarchar(50)',0),
                         ('manufacturer_contact_area_code','nvarchar(50)',0),
                         ('manufacturer_contact_exchange','nvarchar(50)',0),
                         ('manufacturer_contact_phone_no','nvarchar(50)',0),
                         ('manufacturer_contact_extension','nvarchar(50)',0),
                         ('manufacturer_contact_pcountry','nvarchar(50)',0),
                         ('manufacturer_contact_pcity','nvarchar(50)',0),
                         ('manufacturer_contact_plocal','nvarchar(50)',0),
                         ('manufacturer_g1_name','nvarchar(100)',0),
                         ('manufacturer_g1_street_1','nvarchar(100)',0),
                         ('manufacturer_g1_street_2','nvarchar(100)',0),
                         ('manufacturer_g1_city','nvarchar(60)',0),
                         ('manufacturer_g1_state_code','nvarchar(50)',0),
                         ('manufacturer_g1_zip_code','nvarchar(50)',0),
                         ('manufacturer_g1_zip_code_ext','nvarchar(50)',0),
                         ('manufacturer_g1_country_code','nvarchar(50)',0),
                         ('manufacturer_g1_postal_code','nvarchar(50)',0),
                         ('date_manufacturer_received','date',0),
                         ('device_date_of_manufacture','date',0),
                         ('single_use_flag','nvarchar(50)',0),
                         ('remedial_action','nvarchar(50)',0),
                         ('previous_use_code','nvarchar(50)',0),
                         ('removal_correction_number','nvarchar(50)',0),
                         ('event_type','nvarchar(50)',0),
                         ('distributor_name','nvarchar(50)',0),
                         ('distributor_address_1','nvarchar(50)',0),
                         ('distributor_address_2','nvarchar(50)',0),
                         ('distributor_city','nvarchar(50)',0),
                         ('distributor_state_code','nvarchar(50)',0),
                         ('distributor_zip_code','nvarchar(50)',0),
                         ('distributor_zip_code_ext','nvarchar(50)',0),
                         ('report_to_manufacturer','nvarchar(50)',0),
                         ('manufacturer_name','nvarchar(50)',0),
                         ('manufacturer_address_1','nvarchar(50)',0),
                         ('manufacturer_address_2','nvarchar(50)',0),
                         ('manufacturer_city','nvarchar(50)',0),
                         ('manufacturer_state_code','nvarchar(50)',0),
                         ('manufacturer_zip_code','nvarchar(50)',0),
                         ('manufacturer_zip_code_ext','nvarchar(50)',0),
                         ('manufacturer_country_code','nvarchar(50)',0),
                         ('manufacturer_postal_code','nvarchar(50)',0),
                         ('type_of_report','nvarchar(50)',0),
                         ('source_type','nvarchar(50)',0),
                         ('date_added','date',0),
                         ('date_changed','date',0),
                         ('reporter_country_code','nvarchar(50)',0),
                         ('pma_pmn_num','nvarchar(50)',0),
                         ('exemption_number','nvarchar(50)',0),
                         ('summary_report','nvarchar(350)',0),
                         ('noe_summarized','nvarchar(150)',0)
                         )

PATIENT_TABLE_COLUMNS = (('mdr_report_key', 'nvarchar(50)',1),
                        ('patient_sequence_number', 'int',1),
                        ('date_received', 'date',0),
                        ('sequence_number_treatment', 'nvarchar(4000)',0),
                        ('sequence_number_outcome', 'nvarchar(100)',0))

def getcolumnstring(columns,alias,skiplogic = False):
    cols = [f"{alias}.[{col[0]}]" for col in columns]
    columnstring = ','.join(cols)       
    return columnstring

def main():
    args = getResolvedOptions(sys.argv, ['loadtype','JOB_NAME'])
    loadtype = args["loadtype"]
    table_filename = {"mdrfoi_event":"mdrfoi","patient":"patient","foidev_device":"dev","foitext_text":"foitext",}
    table_list = ["mdrfoi_event","patient","foidev_device","foitext_text"]
    table_columns = {"foidev_device":FOIDEV_TABLE_COLUMNS,"foitext_text":FOITEXT_TABLE_COLUMNS,"mdrfoi_event":MDRFOI_TABLE_COLUMNS,"patient":PATIENT_TABLE_COLUMNS}
    
    # Carbon Configs
    carbon_secret = get_secret("global/CarbonDIUPipelineServiceAccount",'eu-west-1')
    server = carbon_secret["host"]
    database = "CARBON"
    user = carbon_secret["username"]
    password = carbon_secret["password"]
    driver_class_name = "com.microsoft.sqlserver.jdbc.SQLServerDriver"            
    jdbcUrl = f"jdbc:jtds:sqlserver://{server}/{database}"
    port = "1433"
    sql_url = f"jdbc:sqlserver://{server}:{port};databasename={database};integratedSecurity=false;authenticationScheme=NTLM;"
    prop = {"url":jdbcUrl,"driver": "net.sourceforge.jtds.jdbc.Driver","batchSize": "100000","user": user,"password": password,"sql_url": sql_url}       
    jars_path = '/tmp/mssql-jdbc-8.2.0.jre8.jar'
    SCHEMA_NAME = "prod_maude"
    
    conn = jdbc.connect(jclassname=driver_class_name,url=sql_url,driver_args=[user, password], jars=jars_path)
    cur = conn.cursor()

    for table in table_list:
        print("Table",table)
        table_name = table
        columns = table_columns[table]

        # Insert add 
        columnstring = getcolumnstring(columns,"t_add") 
        keylist = [col[0] for col in columns if col[2] == 1]
        joincolumnlist =['t_add.'+col+' = t_main.'+col for col in keylist]
        joinstring =  ' and '.join(joincolumnlist)
        keycolumn = keylist[0]
    
        querystring = f"""INSERT INTO {SCHEMA_NAME}.{table_name}
            SELECT {columnstring},CONVERT(date,getdate()) as start_date,NULL as end_date
            FROM {SCHEMA_NAME}.{table}_staging_ADD as t_add
            LEFT JOIN {SCHEMA_NAME}.{table_name} as t_main on {joinstring}
            WHERE t_main.{keycolumn} IS NULL""".replace("\n"," ")     
        print(querystring)
        cur.execute(querystring)
        
        
        # Update Change 
        keylist = [col[0] for col in columns if col[2] == 1]
        joincolumnlist =['t_change.'+col+' = t_main.'+col for col in keylist]
        joinstring =  ' and '.join(joincolumnlist)
        keycolumn = keylist[0]
        nonkeycolumns = [col[0] for col in columns if col[2] == 0]
        nonkeynonmatchlist = ['t_change.['+col+'] <> t_main.['+col+']' for col in nonkeycolumns]
        differencestring =  ' or '.join(nonkeynonmatchlist)
    
        querystring = f"""UPDATE t_main
            SET t_main.end_date = CONVERT(date,getdate())
            FROM {SCHEMA_NAME}.{table_name} as t_main
            LEFT JOIN {SCHEMA_NAME}.{table}_staging_CHANGE as t_change on {joinstring}
            WHERE t_change.{keycolumn} IS NOT NULL and t_main.end_date IS NULL and ({differencestring})""".replace("\n"," ")        
        print(querystring)
        cur.execute(querystring)
    
        # Insert change
        columnstring = getcolumnstring(columns,"tbl") 
        columnstring_main = getcolumnstring(columns,"tbl",True) 
        
        
        querystring = f"""INSERT INTO {SCHEMA_NAME}.{table_name}
            SELECT {columnstring},CONVERT(date,getdate()) as start_date,NULL as end_date
            from (SELECT {columnstring}
                    FROM {SCHEMA_NAME}.{table}_staging_CHANGE as tbl
                    EXCEPT 
                    SELECT {columnstring_main}
                    FROM {SCHEMA_NAME}.{table_name} as tbl where end_date IS NULL) as tbl""".replace("\n"," ")    
                    
        print(querystring)
        cur.execute(querystring)

        cur.execute(f"IF OBJECT_ID('[{SCHEMA_NAME}].[{table}_staging_ADD]', 'U') IS NOT NULL DROP TABLE [{SCHEMA_NAME}].[{table}_staging_ADD]")
        cur.execute(f"IF OBJECT_ID('[{SCHEMA_NAME}].[{table}_staging_CHANGE]', 'U') IS NOT NULL DROP TABLE [{SCHEMA_NAME}].[{table}_staging_CHANGE]")

        
            
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    job.commit()
    return

main()







