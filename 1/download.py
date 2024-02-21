import sys
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

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s] [%(levelname)s] - %(message)s', )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
#logging.getLogger('smbprotocol').setLevel(logging.WARNING)

bucket_folder = 'transformed/public/maude/'
required_packages= "JayDeBeApi,beautifulsoup4"
pipeline_secret_id ='global/PipelineCredentials'
region = 'eu-west-1'
s3 = boto3.resource('s3', region_name=region)
s3_bucket = s3.Bucket(target_bucket)
s3_client = boto3.client('s3')
session = boto3.session.Session()
s3_Session = session.client('s3')
    
def get_secret(secret_name: str, region: str) -> Dict[str, str]:
    """
    Gets a secret from secretsmanager and returns SecretString
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )

    secrets = json.loads(get_secret_value_response['SecretString'])
    return secrets

def install_python_packages(packages: Union[str, List], user, password, dest='libs', verbose=False):
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

pipelinecred = get_secret(pipeline_secret_id,region)
PIPELINE_USER = pipelinecred['pipelineUser']
PIPELINE_PASSWORD = pipelinecred['pipelinePassword']
install_python_packages(required_packages, PIPELINE_USER, PIPELINE_PASSWORD)

import requests
from bs4 import BeautifulSoup

def main():
    args = getResolvedOptions(sys.argv, ['loadtype','JOB_NAME'])
    loadtype = args["loadtype"]
    
    #loadtype = "INC"
    table_filename = {"mdrfoi_event":"mdrfoi","patient":"patient","foidev_device":"dev","foitext_text":"foitext",}
    table_list = ["mdrfoi_event","patient","foidev_device","foitext_text"]
    
    IDE_HTTPS_PROXY=f"https://{PIPELINE_USER}:{PIPELINE_PASSWORD}@webgateway-uk.schroders.com:9090/"
    IDE_HTTP_PROXY="http://{PIPELINE_USER}:{PIPELINE_PASSWORD}@webgateway-uk.schroders.com:9090/"
    maude_url = "https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude"
    proxy_dict = {"http": IDE_HTTP_PROXY,"https": IDE_HTTPS_PROXY}

    session = requests.Session()
    session.proxies = proxy_dict
    
    resp = session.get(maude_url,verify=False)
    assert resp.status_code == 200, f'Resp.status_code is {resp.status_code}.'
    maude_page = resp.content
    soup = BeautifulSoup(maude_page, "html.parser")
    table = soup.find("table", {"class":"table"})
    rows = table.findAll("tr")[1:]
    urllist = []
    for row in rows:    
        urlcolumn = row.findAll(lambda tag: tag.name=='td')[0]
        filenametag = urlcolumn.find('a')
        if filenametag is None:
            continue
        filename = filenametag.text
        fileurl = urlcolumn.find('a').get('href')
        if "problem" not in filename:
            urllist.append({"name":filename,"url":fileurl})
            
    if loadtype == "FUL":
        processurllist = [url for url in urllist if not (url["name"].lower().endswith("change.zip") or url["name"].lower().endswith("add.zip"))]

    #load incremental load files    
    processurllist = [url for url in urllist if not (url["name"].lower().endswith("change.zip") or url["name"].lower().endswith("add.zip"))]
            
    for item in processurllist:
        logger.info(f"downloading file {item['url']}")
        url = item["url"]
        filename = url.split("/")[-1]
        
        if "mdrfoi" in filename:
            table = "mdrfoi_event"
        elif "patient" in filename:
            table = "patient"
        elif "dev" in filename:
            table = "foidev_device" 
        elif "foitext" in filename:
            table = "foitext_text"        
        else:
            table = "notfound"
           
        for attempt in range(3):            
            try:    
                request.urlretrieve(url, filename)
                break
            except:
                print("Retrying Download")
                time.sleep(5)
        
        filenameonly = filename.split(".")[0]
        with ZipFile(filename, 'r') as zip_ref:
            zipinfos = zip_ref.infolist()
            for zipinfo in zipinfos:
                print(zipinfo.filename)
                zipinfo.filename = filenameonly  + ".txt"
                print("Unzippedfile",zipinfo.filename)
                zip_ref.extract(zipinfo)

                filename_unzipped = zipinfo.filename
                if  filename_unzipped.lower().endswith("change.txt"):
                    destination_folder = "INC/change/"
                elif  filename_unzipped.lower().endswith("add.txt"):
                    destination_folder = "INC/add/"
                else:
                    destination_folder = "FUL/"
                 
                key = f"{bucket_folder}{table}/{destination_folder}{filename_unzipped}"
                print("Upload File",key)
                response = s3_client.upload_file(filename_unzipped, target_bucket, key)
                s3_bucket.upload_file(filename_unzipped, key)

                os.remove(filename)
                os.remove(filename_unzipped)

        print("******************************")
        print("\n")


main()







