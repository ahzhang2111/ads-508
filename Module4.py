#!/usr/bin/env python
# coding: utf-8

# In[127]:


get_ipython().system('python --version')


# In[128]:


get_ipython().system('pip install --disable-pip-version-check -q sagemaker==2.38.0')
get_ipython().system('pip install --disable-pip-version-check -q smdebug==1.0.4')
get_ipython().system('pip install --disable-pip-version-check -q sagemaker-experiments==0.1.28')


# In[129]:


get_ipython().system('pip install --disable-pip-version-check -q tensorflow==2.3.1')


# In[130]:


get_ipython().system('pip install --disable-pip-version-check -q tensorflow==2.3.1')


# In[131]:


get_ipython().system('pip install --disable-pip-version-check -q transformers==3.5.1')


# In[132]:


get_ipython().system('pip install --disable-pip-version-check -q PyAthena==2.1.1')


# In[133]:


get_ipython().system('pip install --disable-pip-version-check -q SQLAlchemy==1.3.23')


# In[134]:


get_ipython().system('conda install -q -y zip')


# In[135]:


get_ipython().system('pip install --disable-pip-version-check -q matplotlib==3.1.3')


# In[136]:


get_ipython().system('pip install --disable-pip-version-check -q seaborn==0.10.0')


# In[137]:


get_ipython().system('pip list')


# In[138]:


setup_dependencies_passed = True


# In[139]:


get_ipython().run_line_magic('store', '')


# In[140]:


from pyathena import connect
import pandas as pd


# In[141]:


get_ipython().run_line_magic('store', '-r setup_dependencies_passed')


# In[142]:


try:
    setup_dependencies_passed
except NameError:
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN THE PREVIOUS NOTEBOOK ")
    print("You did not install the required libraries.   ")
    print("++++++++++++++++++++++++++++++++++++++++++++++")


# In[143]:


print(setup_dependencies_passed)


# In[144]:


if not setup_dependencies_passed:
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN THE PREVIOUS NOTEBOOK ")
    print("You did not install the required libraries.   ")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
else:
    print("[OK]")


# In[145]:


get_ipython().run_line_magic('store', '-r setup_iam_roles_passed')


# In[146]:


try:
    setup_iam_roles_passed
except NameError:
    print("+++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup IAM Roles.")
    print("+++++++++++++++++++++++++++++++")


# In[147]:


print(setup_iam_roles_passed)


# In[148]:


import boto3

region = boto3.Session().region_name
session = boto3.session.Session()

ec2 = boto3.Session().client(service_name="ec2", region_name=region)
sm = boto3.Session().client(service_name="sagemaker", region_name=region)


# In[149]:


import json

notebook_instance_name = None

try:
    with open("/opt/ml/metadata/resource-metadata.json") as notebook_info:
        data = json.load(notebook_info)
        domain_id = data["DomainId"]
        resource_arn = data["ResourceArn"]
        region = resource_arn.split(":")[3]
        name = data["ResourceName"]
    print("DomainId: {}".format(domain_id))
    print("Name: {}".format(name))
except:
    print("+++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR]: COULD NOT RETRIEVE THE METADATA.")
    print("+++++++++++++++++++++++++++++++++++++++++")


# In[150]:


get_ipython().run_line_magic('store', '-r setup_instance_check_passed')


# In[151]:


try:
    setup_instance_check_passed
except NameError:
    print("+++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Instance Check.")
    print("+++++++++++++++++++++++++++++++")


# In[152]:


get_ipython().run_line_magic('store', '-r setup_dependencies_passed')


# In[153]:


try:
    setup_dependencies_passed
except NameError:
    print("+++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup Dependencies.")
    print("+++++++++++++++++++++++++++++++")


# In[154]:


print(setup_dependencies_passed)


# In[155]:


get_ipython().run_line_magic('store', '-r setup_s3_bucket_passed')


# In[156]:


try:
    setup_s3_bucket_passed
except NameError:
    print("+++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup Dependencies.")
    print("+++++++++++++++++++++++++++++++")


# In[157]:


print(setup_s3_bucket_passed)


# In[158]:


if not setup_instance_check_passed:
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Instance Check.")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
if not setup_dependencies_passed:
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup Dependencies.")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
if not setup_s3_bucket_passed:
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup S3 Bucket.")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
if not setup_iam_roles_passed:
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL NOTEBOOKS IN THE SETUP FOLDER FIRST. You are missing Setup IAM Roles.")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


# In[159]:


get_ipython().system('aws s3 ls s3://ads-508-azhang/finalproject/')


# In[160]:


import boto3
import sagemaker
import pandas as pd

sess = sagemaker.Session()
bucket = sess.default_bucket()
role = sagemaker.get_execution_role()
region = boto3.Session().region_name
account_id = boto3.client("sts").get_caller_identity().get("Account")

sm = boto3.Session().client(service_name="sagemaker", region_name=region)


# In[161]:


s3_public_path_tsv = "s3://ads-508-azhang/finalproject/"


# In[162]:


get_ipython().run_line_magic('store', 's3_public_path_tsv')


# In[163]:


s3_private_path_tsv = "s3://{}/ads-508-azhang/finalproject".format(bucket)


# In[164]:


print(s3_private_path_tsv)


# In[165]:


get_ipython().run_line_magic('store', 's3_private_path_tsv')


# In[166]:


get_ipython().system('aws s3 cp --recursive $s3_public_path_tsv/ $s3_private_path_tsv/ --exclude "*" --include "NHSDA-1988-DS0001-data-excel.tsv"')
get_ipython().system('aws s3 cp --recursive $s3_public_path_tsv/ $s3_private_path_tsv/ --exclude "*" --include "NHSDA-1995-DS0001-data-excel.tsv"')
get_ipython().system('aws s3 cp --recursive $s3_public_path_tsv/ $s3_private_path_tsv/ --exclude "*" --include "NHSDA-1979-DS0001-data-excel.tsv"')


# In[167]:


print(s3_private_path_tsv)


# In[168]:


get_ipython().system('aws s3 ls $s3_private_path_tsv/')


# In[169]:


session = boto3.Session()


#Then use the session to get the resource
s3 = session.resource('s3')

my_bucket = s3.Bucket('ads-508-azhang')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)


# In[170]:


from IPython.core.display import display, HTML

display(
    HTML(
        '<b>Review <a target="blank" href="https://s3.console.aws.amazon.com/s3/buckets/sagemaker-{}-{}/ads-508-azhang/finalproject/?region={}&tab=overview">S3 Bucket</a></b>'.format(
            region, account_id, region
        )
    )
)


# In[171]:


get_ipython().run_line_magic('store', '')


# In[172]:


get_ipython().system('aws s3 cp s3://ads-508-azhang/finalproject/NHSDA-1979-DS0001-data-excel.tsv - | head')


# In[173]:


s3_client = boto3.client("s3")


# In[174]:


bucket = 'ads-508-azhang'
key = 'finalproject/NHSDA-1988-DS0001-data-excel.tsv'


# In[175]:


response = s3_client.get_object(Bucket = bucket, Key = key)


# In[176]:


df = pd.read_csv(response.get("Body"))


# In[177]:


df.head(1)


# In[178]:


#Create Athena DB Schema


# In[179]:


import boto3
import sagemaker

sess = sagemaker.Session()
bucket = sess.default_bucket()
role = sagemaker.get_execution_role()
region = boto3.Session().region_name


# In[180]:


ingest_create_athena_db_passed = False


# In[181]:


get_ipython().run_line_magic('store', '-r s3_public_path_tsv')


# In[182]:


try:
    s3_public_path_tsv
except NameError:
    print("*****************************************************************************")
    print("[ERROR] PLEASE RE-RUN THE PREVIOUS COPY TSV TO S3 NOTEBOOK ******************")
    print("[ERROR] THIS NOTEBOOK WILL NOT RUN PROPERLY. ********************************")
    print("*****************************************************************************")


# In[183]:


print(s3_public_path_tsv)


# In[184]:


get_ipython().run_line_magic('store', '-r s3_private_path_tsv')


# In[185]:


try:
    s3_private_path_tsv
except NameError:
    print("*****************************************************************************")
    print("[ERROR] PLEASE RE-RUN THE PREVIOUS COPY TSV TO S3 NOTEBOOK ******************")
    print("[ERROR] THIS NOTEBOOK WILL NOT RUN PROPERLY. ********************************")
    print("*****************************************************************************")


# In[186]:


print(s3_private_path_tsv)


# In[187]:


#import PyAthena
get_ipython().system('pip install --disable-pip-version-check -q PyAthena==2.1.0')
from pyathena import connect


# In[188]:


database_name = "drugs"


# In[189]:


# Set S3 staging directory -- this is a temporary directory used for Athena queries
s3_staging_dir = "s3://{0}/ads-508-azhang/finalproject/staging".format(bucket)


# In[190]:


conn = connect(region_name=region, s3_staging_dir=s3_staging_dir)


# In[191]:


statement = "CREATE DATABASE IF NOT EXISTS {}".format(database_name)
print(statement)


# In[192]:


import pandas as pd

pd.read_sql(statement, conn)


# In[193]:


statement = "SHOW DATABASES"

df_show = pd.read_sql(statement, conn)
df_show.head(5)


# In[194]:


drug_dir = 's3://sagemaker-us-east-1-189468192453/ads-508-azhang/finalproject'


# In[195]:


table_name ='NHSDA_1979'
pd.read_sql(f'DROP TABLE IF EXISTS {database_name}.{table_name}', conn)
file_name1 = 'NHSDA-1979-DS0001-data-excel.tsv'
file_name2 = 'NHSDA-1988-DS0001-data-excel.tsv'
file_name3 = 'NHSDA-1995-DS0001-data-excel.tsv'

create_table = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name}(
                CASEID  float, 
RESPID  float, 
ENCPSU  float, 
ENCSEG  float, 
ENCCASE  float, 
CIGMORLS  float, 
CIGTRY  float, 
CIG5PK  float, 
CIGREC  float, 
AVCIG  float, 
HRDHER  float, 
HRDMJ  float, 
HRDCOC  float, 
HRDLSD  float, 
HRDBAR  float, 
HRDTRN  float, 
HRDAMP  float, 
ADDHER  float, 
ADDALC  float, 
ADDMJ  float, 
ADDTOB  float, 
ADDBAR  float, 
ADDTRN  float, 
ADDAMP  float, 
ADDLSD  float, 
ADDCOC  float, 
ADDNONE  float, 
SEDLIKE  float, 
SEDFEEL  float, 
SEDNEED  float, 
SEDREC  float, 
SED30MOA  float, 
SED30MOB  float, 
SED30MOC  float, 
SEDDAL30  float, 
BUTISOL  float, 
BUTICAPS  float, 
AMYTAL  float, 
ESKABARB  float, 
LUMINAL  float, 
MEBARAL  float, 
AMOBARB  float, 
PHENOBAR  float, 
ALURATE  float, 
PLACIDYL  float, 
DORIDEN  float, 
NOLUDAR  float, 
SOPOR  float, 
QUAALUDE  float, 
PAREST  float, 
NOCTEC  float, 
METHAQ  float, 
CHHYD  float, 
NEMBUTAL  float, 
CARBTAL  float, 
SECONAL  float, 
TUINAL  float, 
PENTOB  float, 
SECOB  float, 
DALMANE  float, 
SEDDKNAM  float, 
NOSEDAT  float, 
SEDAGE  float, 
TRNLIKE  float, 
TRNFEEL  float, 
TRNNEED  float, 
TRANREC  float, 
TRN30MOA  float, 
TRN30MOB  float, 
TRN30MOC  float, 
TRNBEN30  float, 
VALIUM  float, 
LIBRIUM  float, 
LIBRITAB  float, 
SKLY  float, 
SERAX  float, 
TRANXENE  float, 
ATIVAN  float, 
VERSTRAN  float, 
MEPRSPAN  float, 
MILTOWN  float, 
EQUANIL  float, 
MEPROB  float, 
VISTAR  float, 
ATARAX  float, 
BENADRYL  float, 
TRDKNAM  float, 
NOTRANQ  float, 
TRANAGE  float, 
STIMLIKE  float, 
STIMFEEL  float, 
STIMNEED  float, 
STIMREC  float, 
STM30MOA  float, 
STM30MOB  float, 
STMRIT30  float, 
STMCYL30  float, 
DEXED  float, 
DEXAMYL  float, 
ESKAT  float, 
BENZ  float, 
BIPHET  float, 
DESOXYN  float, 
DETAMP  float, 
METHI  float, 
OBLA  float, 
TENUATE  float, 
TEPANIL  float, 
DIDREX  float, 
PLEGINE  float, 
PRELUDIN  float, 
PRESATE  float, 
IONAMIN  float, 
PONDIMIN  float, 
VORANIL  float, 
SANOREX  float, 
RITALIN  float, 
CYLERT  float, 
STMDKNAM  float, 
NOSTIMS  float, 
STIMAGE  float, 
ANALLIKE  float, 
ANALFEEL  float, 
ANALNEED  float, 
ANALREC  float, 
ANL30MOA  float, 
ANL30MOB  float, 
ANL30MOC  float, 
ANLTAL30  float, 
DARVON  float, 
DOLENE  float, 
SK65A  float, 
PROPOXY  float, 
LERITINE  float, 
LEVODRO  float, 
PERCODAN  float, 
DEMEROL  float, 
DILAUD  float, 
TYLCOD  float, 
CODEINE  float, 
DOLOP  float, 
WESTODON  float, 
METHDON  float, 
TALWIN  float, 
ANLDKNAM  float, 
ANALNONE  float, 
ANALAGE  float, 
ALCFIRST  float, 
ALCTRY  float, 
ALCREC  float, 
ALCDAYS  float, 
MODR30A  float, 
MODR30DY  float, 
UNDSTAS1  float, 
VRA7AS1  float, 
MRKEAAS1  float, 
VRA8AS1  float, 
MJKNOWN  float, 
MJOPP  float, 
MJFIRST  float, 
MJAGE  float, 
MJLIVE  float, 
MJREC  float, 
MJDAY30A  float, 
MJTOT  float, 
UNDSTAS2  float, 
VRM9AS2  float, 
MRKEAAS2  float, 
VRM10AS2  float, 
INHREAD  float, 
INHOPP  float, 
INHFIRST  float, 
INHAGE  float, 
GAS  float, 
SPPAINT  float, 
AEROS  float, 
GLUE  float, 
SOLVENT  float, 
AMYLNIT  float, 
ETHER  float, 
NITOXID  float, 
ODORIZER  float, 
INHNEVER  float, 
GAS30A  float, 
SPPAN30A  float, 
AEROS30A  float, 
GLUE30A  float, 
SOLVN30A  float, 
AMLNT30A  float, 
ETHER30A  float, 
NOX30A  float, 
ODR30A  float, 
INH30NO  float, 
INHREC  float, 
INHTOT  float, 
INHODRHR  float, 
INHODRUS  float, 
UNDSTAS3  float, 
VRG10AS3  float, 
MRKEAAS3  float, 
VRG11AS3  float, 
HALLOPP  float, 
HALFIRST  float, 
HALLAGE  float, 
HALLREC  float, 
HAL30USE  float, 
HALLTOT  float, 
HALPCPHR  float, 
PCP  float, 
HALPCP30  float, 
UNDSTAS4  float, 
VRL10AS4  float, 
MRKEAAS4  float, 
VRL11AS4  float, 
COCOPP  float, 
COCFIRST  float, 
COCAGE  float, 
COCREC  float, 
COCUS30A  float, 
COCTOT  float, 
UNDSTAS5  float, 
VRC7AS5  float, 
MRKEAAS5  float, 
VRC8AS5  float, 
HERKNOW  float, 
HEROPP  float, 
HERFIRST  float, 
HERAGE  float, 
HERREC  float, 
HER30USE  float, 
HERTOT  float, 
HERFRNDS  float, 
HERNOADR  float, 
HERNEEDL  float, 
UNDSTAS6  float, 
VRH11AS6  float, 
MRKEAAS6  float, 
VRH12AS6  float, 
SPLCOC  float, 
SPLHAL  float, 
SPLCIG  float, 
SPLHER  float, 
SPLBEER  float, 
SPLLQR  float, 
SPLMJR  float, 
SPLPILLS  float, 
SPLINH  float, 
GMJNOHO  float, 
GMJNONE  float, 
GMJMED  float, 
GMJJOB  float, 
GMJFUN  float, 
GMJRELAX  float, 
GMJAWARE  float, 
GMJCNFDN  float, 
GMJDEAL  float, 
GMJSLEEP  float, 
GMJSEX  float, 
GMJAPPET  float, 
GMJDK  float, 
GMJMISC  float, 
GMJREF1  float, 
BMJCONTR  float, 
BMJMEMRY  float, 
BMJNONE  float, 
BMJHABIT  float, 
BMJSTRGR  float, 
BMJHLTH  float, 
BMJDIZZY  float, 
BMJREFLX  float, 
BMJMOOD  float, 
BMJHALLU  float, 
BMJAPTHY  float, 
BMJJOB  float, 
BMJDRIVE  float, 
BMJILLEG  float, 
BMJCRIME  float, 
BMJEXPNS  float, 
BMJDK  float, 
BMJMISC  float, 
BMJREF1  float, 
MJHIGH  float, 
MJDRHIGH  float, 
MJOTHDR  float, 
MJPUFFS  float, 
MJDRPUFF  float, 
MJOTHPUF  float, 
MJINVOLV  float, 
MJCAREMR  float, 
MJCRMORE  float, 
MJOTHMOR  float, 
MJCARELS  float, 
MJCRLESS  float, 
MJOTHLES  float, 
MJWKEND  float, 
MJCRWKEN  float, 
MJOTHWKN  float, 
ALHIGH  float, 
ALDRHIGH  float, 
ALOTHDR  float, 
ALSOME  float, 
ALDRSOME  float, 
ALOTHSOM  float, 
ALOTHDRK  float, 
ALYOUDRK  float, 
CLOSFRNS  float, 
FRNSHER  float, 
FRNSEX  float, 
FRNAGE  float, 
FRNTRYH  float, 
FRNRECH  float, 
SEENUSE  float, 
CONFESS  float, 
TESTMNY  float, 
TRACKMRK  float, 
ARREST  float, 
UNPRREF  float, 
UNPRREP  float, 
UNPRBEH  float, 
UNPROTH  float, 
AMBULANC  float, 
DETECOTH  float, 
GIVESELL  float, 
TREATMNT  float, 
OTHKNOW  float, 
LVDHEREA  float, 
LVDHEREB  float, 
EVRLIVEA  float, 
AGEINA1  float, 
AGEOUTA1  float, 
AGEINA2  float, 
AGEOUTA2  float, 
AGEINA3  float, 
AGEOUTA3  float, 
ALLLIFEA  float, 
EVRLIVEB  float, 
AGEINB1  float, 
AGEOUTB1  float, 
AGEINB2  float, 
AGEOUTB2  float, 
AGEINB3  float, 
AGEOUTB3  float, 
ALLLIFEB  float, 
EVRLIVEC  float, 
AGEINC1  float, 
AGEOUTC1  float, 
AGEINC2  float, 
AGEOUTC2  float, 
AGEINC3  float, 
AGEOUTC3  float, 
ALLLIFEC  float, 
SEX  float, 
RESPAGE  float, 
HISPANIC  float, 
HISPGRP  float, 
RESPRACE  float, 
RAGEGRP  float, 
ENRLCOLL  float, 
TYPESCHL  float, 
STUDFTPT  float, 
EDUC  float, 
TOTPEOP  float, 
UNDAGE18  float, 
UNDAGE6  float, 
AGE612  float, 
AGE1217  float, 
HHPAREN  float, 
NUMPAREN  float, 
HHSPOUS  float, 
NUMSPOUS  float, 
HHSIBLN  float, 
NUMSIBLN  float, 
HHOTREL  float, 
NUMOTREL  float, 
HHFRNDS  float, 
NUMFRNDS  float, 
HHOTPER  float, 
NUMOTPER  float, 
MARITAL  float, 
EMPLOYED  float, 
ROCCUP2  float, 
NOLABOR  float, 
CWE  float, 
CWEOCC2  float, 
INCOME  float, 
ESTHHIN  float, 
YTHSTUD  float, 
YSTDFTPT  float, 
YTHEDUC  float, 
YTOTPEOP  float, 
MOTHER  float, 
FATHER  float, 
OLDSIBS  float, 
NUMOSIBS  float, 
YNGSIBS  float, 
NUMYSIBS  float, 
YTHOTREL  float, 
NUMYOREL  float, 
YTHOTPER  float, 
NUMYOPER  float, 
OTHSIBS  float, 
YTHEMPLD  float, 
YTHOCCU2  float, 
YNOLABOR  float, 
HHAREA  float, 
MILINSTA  float, 
LOGCAMP  float, 
COLLEGE  float, 
RESORT  float, 
CONSTR  float, 
RANCH  float, 
MIGRANTS  float, 
TEMPRES  float, 
HHTYPE  float, 
UNDINT  float, 
COOPINT  float, 
PRIVACY  float, 
ADULTYTH  float, 
PAREXAMQ  float, 
ADLTQCD  float, 
QUEXTYPE  float, 
INTVLEN  float, 
FIID  float, 
TOTHHVIS  float, 
FINLRES1  float, 
VSADLTCM  float, 
PHADLTCM  float, 
FINLRES2  float, 
VSYTHCM  float, 
PHYTHCM  float, 
YTHINHH  float, 
RES1825  float, 
RES2649  float, 
RES50OVR  float, 
AGR1REL1  float, 
AGR1SEX1  float, 
AGR1AGE1  float, 
AGR1RSP1  float, 
AGR1REL2  float, 
AGR1SEX2  float, 
AGR1AGE2  float, 
AGR1RSP2  float, 
AGR1REL3  float, 
AGR1SEX3  float, 
AGR1AGE3  float, 
AGR1RSP3  float, 
AGR1REL4  float, 
AGR1SEX4  float, 
AGR1AGE4  float, 
AGR1RSP4  float, 
AGR2REL1  float, 
AGR2SEX1  float, 
AGR2AGE1  float, 
AGR2RSP1  float, 
AGR2REL2  float, 
AGR2SEX2  float, 
AGR2AGE2  float, 
AGR2RSP2  float, 
AGR2REL3  float, 
AGR2SEX3  float, 
AGR2AGE3  float, 
AGR2RSP3  float, 
AGR2REL4  float, 
AGR2SEX4  float, 
AGR2AGE4  float, 
AGR2RSP4  float, 
AGR3REL1  float, 
AGR3SEX1  float, 
AGR3AGE1  float, 
AGR3RSP1  float, 
AGR3REL2  float, 
AGR3SEX2  float, 
AGR3AGE2  float, 
AGR3RSP2  float, 
AGR3REL3  float, 
AGR3SEX3  float, 
AGR3AGE3  float, 
AGR3RSP3  float, 
AGR3REL4  float, 
AGR3SEX4  float, 
AGR3AGE4  float, 
AGR3RSP4  float, 
YTH1217  float, 
YTH1REL  float, 
YTH1SEX  float, 
YTH1AGE  float, 
YTH1RSP  float, 
YTH2REL  float, 
YTH2SEX  float, 
YTH2AGE  float, 
YTH2RSP  float, 
YTH3REL  float, 
YTH3SEX  float, 
YTH3AGE  float, 
YTH3RSP  float, 
YTH4REL  float, 
YTH4SEX  float, 
YTH4AGE  float, 
YTH4RSP  float, 
REGION  float, 
DIVISION  float, 
POPDENX  float, 
IRAGE  float, 
IIAGE  float, 
IRSEX  float, 
IISEX  float, 
IRRACEX  float, 
IIRACEX  float, 
IRHOIND  float, 
IIHOIND  float, 
IRHOGRP  float, 
IIHOGRP  float, 
IRMARIT  float, 
IIMARIT  float, 
IREDUC  float, 
IIEDUC  float, 
IRALCRC  float, 
IIALCRC  float, 
IRMJRC  float, 
IIMJRC  float, 
IRCOCRC  float, 
IICOCRC  float, 
IRSEDRC  float, 
IISEDRC  float, 
IRTRANRC  float, 
IITRANRC  float, 
IRSTIMRC  float, 
IISTIMRC  float, 
IRANALRC  float, 
IIANALRC  float, 
IRCIGRC  float, 
IICIGRC  float, 
IRINHRC  float, 
IIINHRC  float, 
IRHALLRC  float, 
IIHALLRC  float, 
IRHERRC  float, 
IIHERRC  float, 
CATAGE  float, 
CATAG2  float, 
CATAG3  float, 
RACE  float, 
HISPRACE  float, 
EDUCCAT2  float, 
HALFLAG  float, 
HALYR  float, 
HALMON  float, 
STMFLAG  float, 
STMYR  float, 
STMMON  float, 
SEDFLAG  float, 
SEDYR  float, 
SEDMON  float, 
TRQFLAG  float, 
TRQYR  float, 
TRQMON  float, 
ANLFLAG  float, 
ANLYR  float, 
ANLMON  float, 
ALCFLAG  float, 
ALCYR  float, 
ALCMON  float, 
CIGFLAG  float, 
CIGYR  float, 
CIGMON  float, 
HERFLAG  float, 
HERYR  float, 
HERMON  float, 
MRJFLAG  float, 
MRJYR  float, 
MRJMON  float, 
COCFLAG  float, 
COCYR  float, 
COCMON  float, 
INHFLAG  float, 
INHYR  float, 
INHMON  float, 
PSYFLAG2  float, 
PSYYR2  float, 
PSYMON2  float, 
SUMFLAG  float, 
SUMYR  float, 
SUMMON  float, 
MJOFLAG  float, 
MJOYR2  float, 
MJOMON2  float, 
IEMFLAG  float, 
IEMYR  float, 
IEMMON  float, 
VESTR  float, 
VEREP  float, 
ANALWT float,
CANALWT float,
NANALWT float,
INITWT float,
WT1 float,
WT2 float,
CINITWT float,
CWT1 float,
CWT2 float,
NINITWT float,
NWT1 float,
NWT2 float
)
                
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '	'
                LINES TERMINATED BY '\n'
                LOCATION '{drug_dir}/NHSDA-1979-DS0001-data-excel'
                TBLPROPERTIES ('skip.header.line.count'='1')
"""


# In[196]:


pd.read_sql(create_table, conn)


# In[197]:


pd.read_sql(f'SELECT count(*) FROM {database_name}.{table_name} LIMIT 5', conn)


# In[198]:


table_name2 ='NHSDA_1988'
pd.read_sql(f'DROP TABLE IF EXISTS {database_name}.{table_name2}', conn)

create_table = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name2}(
                CASEID  float, 
RESPID  float, 
ENCPSU  float, 
ENCSEG  float, 
ENCCASE  float, 
CIGMORLS  float, 
CIGTRY  float, 
CIG5PK  float, 
CIGREC  float, 
AVCIG  float, 
HRDHER  float, 
HRDMJ  float, 
HRDCOC  float, 
HRDLSD  float, 
HRDBAR  float, 
HRDTRN  float, 
HRDAMP  float, 
ADDHER  float, 
ADDALC  float, 
ADDMJ  float, 
ADDTOB  float, 
ADDBAR  float, 
ADDTRN  float, 
ADDAMP  float, 
ADDLSD  float, 
ADDCOC  float, 
ADDNONE  float, 
SEDLIKE  float, 
SEDFEEL  float, 
SEDNEED  float, 
SEDREC  float, 
SED30MOA  float, 
SED30MOB  float, 
SED30MOC  float, 
SEDDAL30  float, 
BUTISOL  float, 
BUTICAPS  float, 
AMYTAL  float, 
ESKABARB  float, 
LUMINAL  float, 
MEBARAL  float, 
AMOBARB  float, 
PHENOBAR  float, 
ALURATE  float, 
PLACIDYL  float, 
DORIDEN  float, 
NOLUDAR  float, 
SOPOR  float, 
QUAALUDE  float, 
PAREST  float, 
NOCTEC  float, 
METHAQ  float, 
CHHYD  float, 
NEMBUTAL  float, 
CARBTAL  float, 
SECONAL  float, 
TUINAL  float, 
PENTOB  float, 
SECOB  float, 
DALMANE  float, 
SEDDKNAM  float, 
NOSEDAT  float, 
SEDAGE  float, 
TRNLIKE  float, 
TRNFEEL  float, 
TRNNEED  float, 
TRANREC  float, 
TRN30MOA  float, 
TRN30MOB  float, 
TRN30MOC  float, 
TRNBEN30  float, 
VALIUM  float, 
LIBRIUM  float, 
LIBRITAB  float, 
SKLY  float, 
SERAX  float, 
TRANXENE  float, 
ATIVAN  float, 
VERSTRAN  float, 
MEPRSPAN  float, 
MILTOWN  float, 
EQUANIL  float, 
MEPROB  float, 
VISTAR  float, 
ATARAX  float, 
BENADRYL  float, 
TRDKNAM  float, 
NOTRANQ  float, 
TRANAGE  float, 
STIMLIKE  float, 
STIMFEEL  float, 
STIMNEED  float, 
STIMREC  float, 
STM30MOA  float, 
STM30MOB  float, 
STMRIT30  float, 
STMCYL30  float, 
DEXED  float, 
DEXAMYL  float, 
ESKAT  float, 
BENZ  float, 
BIPHET  float, 
DESOXYN  float, 
DETAMP  float, 
METHI  float, 
OBLA  float, 
TENUATE  float, 
TEPANIL  float, 
DIDREX  float, 
PLEGINE  float, 
PRELUDIN  float, 
PRESATE  float, 
IONAMIN  float, 
PONDIMIN  float, 
VORANIL  float, 
SANOREX  float, 
RITALIN  float, 
CYLERT  float, 
STMDKNAM  float, 
NOSTIMS  float, 
STIMAGE  float, 
ANALLIKE  float, 
ANALFEEL  float, 
ANALNEED  float, 
ANALREC  float, 
ANL30MOA  float, 
ANL30MOB  float, 
ANL30MOC  float, 
ANLTAL30  float, 
DARVON  float, 
DOLENE  float, 
SK65A  float, 
PROPOXY  float, 
LERITINE  float, 
LEVODRO  float, 
PERCODAN  float, 
DEMEROL  float, 
DILAUD  float, 
TYLCOD  float, 
CODEINE  float, 
DOLOP  float, 
WESTODON  float, 
METHDON  float, 
TALWIN  float, 
ANLDKNAM  float, 
ANALNONE  float, 
ANALAGE  float, 
ALCFIRST  float, 
ALCTRY  float, 
ALCREC  float, 
ALCDAYS  float, 
MODR30A  float, 
MODR30DY  float, 
UNDSTAS1  float, 
VRA7AS1  float, 
MRKEAAS1  float, 
VRA8AS1  float, 
MJKNOWN  float, 
MJOPP  float, 
MJFIRST  float, 
MJAGE  float, 
MJLIVE  float, 
MJREC  float, 
MJDAY30A  float, 
MJTOT  float, 
UNDSTAS2  float, 
VRM9AS2  float, 
MRKEAAS2  float, 
VRM10AS2  float, 
INHREAD  float, 
INHOPP  float, 
INHFIRST  float, 
INHAGE  float, 
GAS  float, 
SPPAINT  float, 
AEROS  float, 
GLUE  float, 
SOLVENT  float, 
AMYLNIT  float, 
ETHER  float, 
NITOXID  float, 
ODORIZER  float, 
INHNEVER  float, 
GAS30A  float, 
SPPAN30A  float, 
AEROS30A  float, 
GLUE30A  float, 
SOLVN30A  float, 
AMLNT30A  float, 
ETHER30A  float, 
NOX30A  float, 
ODR30A  float, 
INH30NO  float, 
INHREC  float, 
INHTOT  float, 
INHODRHR  float, 
INHODRUS  float, 
UNDSTAS3  float, 
VRG10AS3  float, 
MRKEAAS3  float, 
VRG11AS3  float, 
HALLOPP  float, 
HALFIRST  float, 
HALLAGE  float, 
HALLREC  float, 
HAL30USE  float, 
HALLTOT  float, 
HALPCPHR  float, 
PCP  float, 
HALPCP30  float, 
UNDSTAS4  float, 
VRL10AS4  float, 
MRKEAAS4  float, 
VRL11AS4  float, 
COCOPP  float, 
COCFIRST  float, 
COCAGE  float, 
COCREC  float, 
COCUS30A  float, 
COCTOT  float, 
UNDSTAS5  float, 
VRC7AS5  float, 
MRKEAAS5  float, 
VRC8AS5  float, 
HERKNOW  float, 
HEROPP  float, 
HERFIRST  float, 
HERAGE  float, 
HERREC  float, 
HER30USE  float, 
HERTOT  float, 
HERFRNDS  float, 
HERNOADR  float, 
HERNEEDL  float, 
UNDSTAS6  float, 
VRH11AS6  float, 
MRKEAAS6  float, 
VRH12AS6  float, 
SPLCOC  float, 
SPLHAL  float, 
SPLCIG  float, 
SPLHER  float, 
SPLBEER  float, 
SPLLQR  float, 
SPLMJR  float, 
SPLPILLS  float, 
SPLINH  float, 
GMJNOHO  float, 
GMJNONE  float, 
GMJMED  float, 
GMJJOB  float, 
GMJFUN  float, 
GMJRELAX  float, 
GMJAWARE  float, 
GMJCNFDN  float, 
GMJDEAL  float, 
GMJSLEEP  float, 
GMJSEX  float, 
GMJAPPET  float, 
GMJDK  float, 
GMJMISC  float, 
GMJREF1  float, 
BMJCONTR  float, 
BMJMEMRY  float, 
BMJNONE  float, 
BMJHABIT  float, 
BMJSTRGR  float, 
BMJHLTH  float, 
BMJDIZZY  float, 
BMJREFLX  float, 
BMJMOOD  float, 
BMJHALLU  float, 
BMJAPTHY  float, 
BMJJOB  float, 
BMJDRIVE  float, 
BMJILLEG  float, 
BMJCRIME  float, 
BMJEXPNS  float, 
BMJDK  float, 
BMJMISC  float, 
BMJREF1  float, 
MJHIGH  float, 
MJDRHIGH  float, 
MJOTHDR  float, 
MJPUFFS  float, 
MJDRPUFF  float, 
MJOTHPUF  float, 
MJINVOLV  float, 
MJCAREMR  float, 
MJCRMORE  float, 
MJOTHMOR  float, 
MJCARELS  float, 
MJCRLESS  float, 
MJOTHLES  float, 
MJWKEND  float, 
MJCRWKEN  float, 
MJOTHWKN  float, 
ALHIGH  float, 
ALDRHIGH  float, 
ALOTHDR  float, 
ALSOME  float, 
ALDRSOME  float, 
ALOTHSOM  float, 
ALOTHDRK  float, 
ALYOUDRK  float, 
CLOSFRNS  float, 
FRNSHER  float, 
FRNSEX  float, 
FRNAGE  float, 
FRNTRYH  float, 
FRNRECH  float, 
SEENUSE  float, 
CONFESS  float, 
TESTMNY  float, 
TRACKMRK  float, 
ARREST  float, 
UNPRREF  float, 
UNPRREP  float, 
UNPRBEH  float, 
UNPROTH  float, 
AMBULANC  float, 
DETECOTH  float, 
GIVESELL  float, 
TREATMNT  float, 
OTHKNOW  float, 
LVDHEREA  float, 
LVDHEREB  float, 
EVRLIVEA  float, 
AGEINA1  float, 
AGEOUTA1  float, 
AGEINA2  float, 
AGEOUTA2  float, 
AGEINA3  float, 
AGEOUTA3  float, 
ALLLIFEA  float, 
EVRLIVEB  float, 
AGEINB1  float, 
AGEOUTB1  float, 
AGEINB2  float, 
AGEOUTB2  float, 
AGEINB3  float, 
AGEOUTB3  float, 
ALLLIFEB  float, 
EVRLIVEC  float, 
AGEINC1  float, 
AGEOUTC1  float, 
AGEINC2  float, 
AGEOUTC2  float, 
AGEINC3  float, 
AGEOUTC3  float, 
ALLLIFEC  float, 
SEX  float, 
RESPAGE  float, 
HISPANIC  float, 
HISPGRP  float, 
RESPRACE  float, 
RAGEGRP  float, 
ENRLCOLL  float, 
TYPESCHL  float, 
STUDFTPT  float, 
EDUC  float, 
TOTPEOP  float, 
UNDAGE18  float, 
UNDAGE6  float, 
AGE612  float, 
AGE1217  float, 
HHPAREN  float, 
NUMPAREN  float, 
HHSPOUS  float, 
NUMSPOUS  float, 
HHSIBLN  float, 
NUMSIBLN  float, 
HHOTREL  float, 
NUMOTREL  float, 
HHFRNDS  float, 
NUMFRNDS  float, 
HHOTPER  float, 
NUMOTPER  float, 
MARITAL  float, 
EMPLOYED  float, 
ROCCUP2  float, 
NOLABOR  float, 
CWE  float, 
CWEOCC2  float, 
INCOME  float, 
ESTHHIN  float, 
YTHSTUD  float, 
YSTDFTPT  float, 
YTHEDUC  float, 
YTOTPEOP  float, 
MOTHER  float, 
FATHER  float, 
OLDSIBS  float, 
NUMOSIBS  float, 
YNGSIBS  float, 
NUMYSIBS  float, 
YTHOTREL  float, 
NUMYOREL  float, 
YTHOTPER  float, 
NUMYOPER  float, 
OTHSIBS  float, 
YTHEMPLD  float, 
YTHOCCU2  float, 
YNOLABOR  float, 
HHAREA  float, 
MILINSTA  float, 
LOGCAMP  float, 
COLLEGE  float, 
RESORT  float, 
CONSTR  float, 
RANCH  float, 
MIGRANTS  float, 
TEMPRES  float, 
HHTYPE  float, 
UNDINT  float, 
COOPINT  float, 
PRIVACY  float, 
ADULTYTH  float, 
PAREXAMQ  float, 
ADLTQCD  float, 
QUEXTYPE  float, 
INTVLEN  float, 
FIID  float, 
TOTHHVIS  float, 
FINLRES1  float, 
VSADLTCM  float, 
PHADLTCM  float, 
FINLRES2  float, 
VSYTHCM  float, 
PHYTHCM  float, 
YTHINHH  float, 
RES1825  float, 
RES2649  float, 
RES50OVR  float, 
AGR1REL1  float, 
AGR1SEX1  float, 
AGR1AGE1  float, 
AGR1RSP1  float, 
AGR1REL2  float, 
AGR1SEX2  float, 
AGR1AGE2  float, 
AGR1RSP2  float, 
AGR1REL3  float, 
AGR1SEX3  float, 
AGR1AGE3  float, 
AGR1RSP3  float, 
AGR1REL4  float, 
AGR1SEX4  float, 
AGR1AGE4  float, 
AGR1RSP4  float, 
AGR2REL1  float, 
AGR2SEX1  float, 
AGR2AGE1  float, 
AGR2RSP1  float, 
AGR2REL2  float, 
AGR2SEX2  float, 
AGR2AGE2  float, 
AGR2RSP2  float, 
AGR2REL3  float, 
AGR2SEX3  float, 
AGR2AGE3  float, 
AGR2RSP3  float, 
AGR2REL4  float, 
AGR2SEX4  float, 
AGR2AGE4  float, 
AGR2RSP4  float, 
AGR3REL1  float, 
AGR3SEX1  float, 
AGR3AGE1  float, 
AGR3RSP1  float, 
AGR3REL2  float, 
AGR3SEX2  float, 
AGR3AGE2  float, 
AGR3RSP2  float, 
AGR3REL3  float, 
AGR3SEX3  float, 
AGR3AGE3  float, 
AGR3RSP3  float, 
AGR3REL4  float, 
AGR3SEX4  float, 
AGR3AGE4  float, 
AGR3RSP4  float, 
YTH1217  float, 
YTH1REL  float, 
YTH1SEX  float, 
YTH1AGE  float, 
YTH1RSP  float, 
YTH2REL  float, 
YTH2SEX  float, 
YTH2AGE  float, 
YTH2RSP  float, 
YTH3REL  float, 
YTH3SEX  float, 
YTH3AGE  float, 
YTH3RSP  float, 
YTH4REL  float, 
YTH4SEX  float, 
YTH4AGE  float, 
YTH4RSP  float, 
REGION  float, 
DIVISION  float, 
POPDENX  float, 
IRAGE  float, 
IIAGE  float, 
IRSEX  float, 
IISEX  float, 
IRRACEX  float, 
IIRACEX  float, 
IRHOIND  float, 
IIHOIND  float, 
IRHOGRP  float, 
IIHOGRP  float, 
IRMARIT  float, 
IIMARIT  float, 
IREDUC  float, 
IIEDUC  float, 
IRALCRC  float, 
IIALCRC  float, 
IRMJRC  float, 
IIMJRC  float, 
IRCOCRC  float, 
IICOCRC  float, 
IRSEDRC  float, 
IISEDRC  float, 
IRTRANRC  float, 
IITRANRC  float, 
IRSTIMRC  float, 
IISTIMRC  float, 
IRANALRC  float, 
IIANALRC  float, 
IRCIGRC  float, 
IICIGRC  float, 
IRINHRC  float, 
IIINHRC  float, 
IRHALLRC  float, 
IIHALLRC  float, 
IRHERRC  float, 
IIHERRC  float, 
CATAGE  float, 
CATAG2  float, 
CATAG3  float, 
RACE  float, 
HISPRACE  float, 
EDUCCAT2  float, 
HALFLAG  float, 
HALYR  float, 
HALMON  float, 
STMFLAG  float, 
STMYR  float, 
STMMON  float, 
SEDFLAG  float, 
SEDYR  float, 
SEDMON  float, 
TRQFLAG  float, 
TRQYR  float, 
TRQMON  float, 
ANLFLAG  float, 
ANLYR  float, 
ANLMON  float, 
ALCFLAG  float, 
ALCYR  float, 
ALCMON  float, 
CIGFLAG  float, 
CIGYR  float, 
CIGMON  float, 
HERFLAG  float, 
HERYR  float, 
HERMON  float, 
MRJFLAG  float, 
MRJYR  float, 
MRJMON  float, 
COCFLAG  float, 
COCYR  float, 
COCMON  float, 
INHFLAG  float, 
INHYR  float, 
INHMON  float, 
PSYFLAG2  float, 
PSYYR2  float, 
PSYMON2  float, 
SUMFLAG  float, 
SUMYR  float, 
SUMMON  float, 
MJOFLAG  float, 
MJOYR2  float, 
MJOMON2  float, 
IEMFLAG  float, 
IEMYR  float, 
IEMMON  float, 
VESTR  float, 
VEREP  float, 
ANALWT float,
CANALWT float,
NANALWT float,
INITWT float,
WT1 float,
WT2 float,
CINITWT float,
CWT1 float,
CWT2 float,
NINITWT float,
NWT1 float,
NWT2 float
)
                
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '	'
                LINES TERMINATED BY '\n'
                LOCATION '{drug_dir}/NHSDA-1988-DS0001-data-excel'
                TBLPROPERTIES ('skip.header.line.count'='1')
"""


# In[199]:


pd.read_sql(create_table, conn)


# In[200]:


pd.read_sql(f'SELECT count(*) FROM {database_name}.{table_name2} LIMIT 5', conn)


# In[201]:


table_name3 ='NHSDA_1995'
pd.read_sql(f'DROP TABLE IF EXISTS {database_name}.{table_name3}', conn)

create_table = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name3}(
                CASEID  float, 
RESPID  float, 
ENCPSU  float, 
ENCSEG  float, 
ENCCASE  float, 
CIGMORLS  float, 
CIGTRY  float, 
CIG5PK  float, 
CIGREC  float, 
AVCIG  float, 
HRDHER  float, 
HRDMJ  float, 
HRDCOC  float, 
HRDLSD  float, 
HRDBAR  float, 
HRDTRN  float, 
HRDAMP  float, 
ADDHER  float, 
ADDALC  float, 
ADDMJ  float, 
ADDTOB  float, 
ADDBAR  float, 
ADDTRN  float, 
ADDAMP  float, 
ADDLSD  float, 
ADDCOC  float, 
ADDNONE  float, 
SEDLIKE  float, 
SEDFEEL  float, 
SEDNEED  float, 
SEDREC  float, 
SED30MOA  float, 
SED30MOB  float, 
SED30MOC  float, 
SEDDAL30  float, 
BUTISOL  float, 
BUTICAPS  float, 
AMYTAL  float, 
ESKABARB  float, 
LUMINAL  float, 
MEBARAL  float, 
AMOBARB  float, 
PHENOBAR  float, 
ALURATE  float, 
PLACIDYL  float, 
DORIDEN  float, 
NOLUDAR  float, 
SOPOR  float, 
QUAALUDE  float, 
PAREST  float, 
NOCTEC  float, 
METHAQ  float, 
CHHYD  float, 
NEMBUTAL  float, 
CARBTAL  float, 
SECONAL  float, 
TUINAL  float, 
PENTOB  float, 
SECOB  float, 
DALMANE  float, 
SEDDKNAM  float, 
NOSEDAT  float, 
SEDAGE  float, 
TRNLIKE  float, 
TRNFEEL  float, 
TRNNEED  float, 
TRANREC  float, 
TRN30MOA  float, 
TRN30MOB  float, 
TRN30MOC  float, 
TRNBEN30  float, 
VALIUM  float, 
LIBRIUM  float, 
LIBRITAB  float, 
SKLY  float, 
SERAX  float, 
TRANXENE  float, 
ATIVAN  float, 
VERSTRAN  float, 
MEPRSPAN  float, 
MILTOWN  float, 
EQUANIL  float, 
MEPROB  float, 
VISTAR  float, 
ATARAX  float, 
BENADRYL  float, 
TRDKNAM  float, 
NOTRANQ  float, 
TRANAGE  float, 
STIMLIKE  float, 
STIMFEEL  float, 
STIMNEED  float, 
STIMREC  float, 
STM30MOA  float, 
STM30MOB  float, 
STMRIT30  float, 
STMCYL30  float, 
DEXED  float, 
DEXAMYL  float, 
ESKAT  float, 
BENZ  float, 
BIPHET  float, 
DESOXYN  float, 
DETAMP  float, 
METHI  float, 
OBLA  float, 
TENUATE  float, 
TEPANIL  float, 
DIDREX  float, 
PLEGINE  float, 
PRELUDIN  float, 
PRESATE  float, 
IONAMIN  float, 
PONDIMIN  float, 
VORANIL  float, 
SANOREX  float, 
RITALIN  float, 
CYLERT  float, 
STMDKNAM  float, 
NOSTIMS  float, 
STIMAGE  float, 
ANALLIKE  float, 
ANALFEEL  float, 
ANALNEED  float, 
ANALREC  float, 
ANL30MOA  float, 
ANL30MOB  float, 
ANL30MOC  float, 
ANLTAL30  float, 
DARVON  float, 
DOLENE  float, 
SK65A  float, 
PROPOXY  float, 
LERITINE  float, 
LEVODRO  float, 
PERCODAN  float, 
DEMEROL  float, 
DILAUD  float, 
TYLCOD  float, 
CODEINE  float, 
DOLOP  float, 
WESTODON  float, 
METHDON  float, 
TALWIN  float, 
ANLDKNAM  float, 
ANALNONE  float, 
ANALAGE  float, 
ALCFIRST  float, 
ALCTRY  float, 
ALCREC  float, 
ALCDAYS  float, 
MODR30A  float, 
MODR30DY  float, 
UNDSTAS1  float, 
VRA7AS1  float, 
MRKEAAS1  float, 
VRA8AS1  float, 
MJKNOWN  float, 
MJOPP  float, 
MJFIRST  float, 
MJAGE  float, 
MJLIVE  float, 
MJREC  float, 
MJDAY30A  float, 
MJTOT  float, 
UNDSTAS2  float, 
VRM9AS2  float, 
MRKEAAS2  float, 
VRM10AS2  float, 
INHREAD  float, 
INHOPP  float, 
INHFIRST  float, 
INHAGE  float, 
GAS  float, 
SPPAINT  float, 
AEROS  float, 
GLUE  float, 
SOLVENT  float, 
AMYLNIT  float, 
ETHER  float, 
NITOXID  float, 
ODORIZER  float, 
INHNEVER  float, 
GAS30A  float, 
SPPAN30A  float, 
AEROS30A  float, 
GLUE30A  float, 
SOLVN30A  float, 
AMLNT30A  float, 
ETHER30A  float, 
NOX30A  float, 
ODR30A  float, 
INH30NO  float, 
INHREC  float, 
INHTOT  float, 
INHODRHR  float, 
INHODRUS  float, 
UNDSTAS3  float, 
VRG10AS3  float, 
MRKEAAS3  float, 
VRG11AS3  float, 
HALLOPP  float, 
HALFIRST  float, 
HALLAGE  float, 
HALLREC  float, 
HAL30USE  float, 
HALLTOT  float, 
HALPCPHR  float, 
PCP  float, 
HALPCP30  float, 
UNDSTAS4  float, 
VRL10AS4  float, 
MRKEAAS4  float, 
VRL11AS4  float, 
COCOPP  float, 
COCFIRST  float, 
COCAGE  float, 
COCREC  float, 
COCUS30A  float, 
COCTOT  float, 
UNDSTAS5  float, 
VRC7AS5  float, 
MRKEAAS5  float, 
VRC8AS5  float, 
HERKNOW  float, 
HEROPP  float, 
HERFIRST  float, 
HERAGE  float, 
HERREC  float, 
HER30USE  float, 
HERTOT  float, 
HERFRNDS  float, 
HERNOADR  float, 
HERNEEDL  float, 
UNDSTAS6  float, 
VRH11AS6  float, 
MRKEAAS6  float, 
VRH12AS6  float, 
SPLCOC  float, 
SPLHAL  float, 
SPLCIG  float, 
SPLHER  float, 
SPLBEER  float, 
SPLLQR  float, 
SPLMJR  float, 
SPLPILLS  float, 
SPLINH  float, 
GMJNOHO  float, 
GMJNONE  float, 
GMJMED  float, 
GMJJOB  float, 
GMJFUN  float, 
GMJRELAX  float, 
GMJAWARE  float, 
GMJCNFDN  float, 
GMJDEAL  float, 
GMJSLEEP  float, 
GMJSEX  float, 
GMJAPPET  float, 
GMJDK  float, 
GMJMISC  float, 
GMJREF1  float, 
BMJCONTR  float, 
BMJMEMRY  float, 
BMJNONE  float, 
BMJHABIT  float, 
BMJSTRGR  float, 
BMJHLTH  float, 
BMJDIZZY  float, 
BMJREFLX  float, 
BMJMOOD  float, 
BMJHALLU  float, 
BMJAPTHY  float, 
BMJJOB  float, 
BMJDRIVE  float, 
BMJILLEG  float, 
BMJCRIME  float, 
BMJEXPNS  float, 
BMJDK  float, 
BMJMISC  float, 
BMJREF1  float, 
MJHIGH  float, 
MJDRHIGH  float, 
MJOTHDR  float, 
MJPUFFS  float, 
MJDRPUFF  float, 
MJOTHPUF  float, 
MJINVOLV  float, 
MJCAREMR  float, 
MJCRMORE  float, 
MJOTHMOR  float, 
MJCARELS  float, 
MJCRLESS  float, 
MJOTHLES  float, 
MJWKEND  float, 
MJCRWKEN  float, 
MJOTHWKN  float, 
ALHIGH  float, 
ALDRHIGH  float, 
ALOTHDR  float, 
ALSOME  float, 
ALDRSOME  float, 
ALOTHSOM  float, 
ALOTHDRK  float, 
ALYOUDRK  float, 
CLOSFRNS  float, 
FRNSHER  float, 
FRNSEX  float, 
FRNAGE  float, 
FRNTRYH  float, 
FRNRECH  float, 
SEENUSE  float, 
CONFESS  float, 
TESTMNY  float, 
TRACKMRK  float, 
ARREST  float, 
UNPRREF  float, 
UNPRREP  float, 
UNPRBEH  float, 
UNPROTH  float, 
AMBULANC  float, 
DETECOTH  float, 
GIVESELL  float, 
TREATMNT  float, 
OTHKNOW  float, 
LVDHEREA  float, 
LVDHEREB  float, 
EVRLIVEA  float, 
AGEINA1  float, 
AGEOUTA1  float, 
AGEINA2  float, 
AGEOUTA2  float, 
AGEINA3  float, 
AGEOUTA3  float, 
ALLLIFEA  float, 
EVRLIVEB  float, 
AGEINB1  float, 
AGEOUTB1  float, 
AGEINB2  float, 
AGEOUTB2  float, 
AGEINB3  float, 
AGEOUTB3  float, 
ALLLIFEB  float, 
EVRLIVEC  float, 
AGEINC1  float, 
AGEOUTC1  float, 
AGEINC2  float, 
AGEOUTC2  float, 
AGEINC3  float, 
AGEOUTC3  float, 
ALLLIFEC  float, 
SEX  float, 
RESPAGE  float, 
HISPANIC  float, 
HISPGRP  float, 
RESPRACE  float, 
RAGEGRP  float, 
ENRLCOLL  float, 
TYPESCHL  float, 
STUDFTPT  float, 
EDUC  float, 
TOTPEOP  float, 
UNDAGE18  float, 
UNDAGE6  float, 
AGE612  float, 
AGE1217  float, 
HHPAREN  float, 
NUMPAREN  float, 
HHSPOUS  float, 
NUMSPOUS  float, 
HHSIBLN  float, 
NUMSIBLN  float, 
HHOTREL  float, 
NUMOTREL  float, 
HHFRNDS  float, 
NUMFRNDS  float, 
HHOTPER  float, 
NUMOTPER  float, 
MARITAL  float, 
EMPLOYED  float, 
ROCCUP2  float, 
NOLABOR  float, 
CWE  float, 
CWEOCC2  float, 
INCOME  float, 
ESTHHIN  float, 
YTHSTUD  float, 
YSTDFTPT  float, 
YTHEDUC  float, 
YTOTPEOP  float, 
MOTHER  float, 
FATHER  float, 
OLDSIBS  float, 
NUMOSIBS  float, 
YNGSIBS  float, 
NUMYSIBS  float, 
YTHOTREL  float, 
NUMYOREL  float, 
YTHOTPER  float, 
NUMYOPER  float, 
OTHSIBS  float, 
YTHEMPLD  float, 
YTHOCCU2  float, 
YNOLABOR  float, 
HHAREA  float, 
MILINSTA  float, 
LOGCAMP  float, 
COLLEGE  float, 
RESORT  float, 
CONSTR  float, 
RANCH  float, 
MIGRANTS  float, 
TEMPRES  float, 
HHTYPE  float, 
UNDINT  float, 
COOPINT  float, 
PRIVACY  float, 
ADULTYTH  float, 
PAREXAMQ  float, 
ADLTQCD  float, 
QUEXTYPE  float, 
INTVLEN  float, 
FIID  float, 
TOTHHVIS  float, 
FINLRES1  float, 
VSADLTCM  float, 
PHADLTCM  float, 
FINLRES2  float, 
VSYTHCM  float, 
PHYTHCM  float, 
YTHINHH  float, 
RES1825  float, 
RES2649  float, 
RES50OVR  float, 
AGR1REL1  float, 
AGR1SEX1  float, 
AGR1AGE1  float, 
AGR1RSP1  float, 
AGR1REL2  float, 
AGR1SEX2  float, 
AGR1AGE2  float, 
AGR1RSP2  float, 
AGR1REL3  float, 
AGR1SEX3  float, 
AGR1AGE3  float, 
AGR1RSP3  float, 
AGR1REL4  float, 
AGR1SEX4  float, 
AGR1AGE4  float, 
AGR1RSP4  float, 
AGR2REL1  float, 
AGR2SEX1  float, 
AGR2AGE1  float, 
AGR2RSP1  float, 
AGR2REL2  float, 
AGR2SEX2  float, 
AGR2AGE2  float, 
AGR2RSP2  float, 
AGR2REL3  float, 
AGR2SEX3  float, 
AGR2AGE3  float, 
AGR2RSP3  float, 
AGR2REL4  float, 
AGR2SEX4  float, 
AGR2AGE4  float, 
AGR2RSP4  float, 
AGR3REL1  float, 
AGR3SEX1  float, 
AGR3AGE1  float, 
AGR3RSP1  float, 
AGR3REL2  float, 
AGR3SEX2  float, 
AGR3AGE2  float, 
AGR3RSP2  float, 
AGR3REL3  float, 
AGR3SEX3  float, 
AGR3AGE3  float, 
AGR3RSP3  float, 
AGR3REL4  float, 
AGR3SEX4  float, 
AGR3AGE4  float, 
AGR3RSP4  float, 
YTH1217  float, 
YTH1REL  float, 
YTH1SEX  float, 
YTH1AGE  float, 
YTH1RSP  float, 
YTH2REL  float, 
YTH2SEX  float, 
YTH2AGE  float, 
YTH2RSP  float, 
YTH3REL  float, 
YTH3SEX  float, 
YTH3AGE  float, 
YTH3RSP  float, 
YTH4REL  float, 
YTH4SEX  float, 
YTH4AGE  float, 
YTH4RSP  float, 
REGION  float, 
DIVISION  float, 
POPDENX  float, 
IRAGE  float, 
IIAGE  float, 
IRSEX  float, 
IISEX  float, 
IRRACEX  float, 
IIRACEX  float, 
IRHOIND  float, 
IIHOIND  float, 
IRHOGRP  float, 
IIHOGRP  float, 
IRMARIT  float, 
IIMARIT  float, 
IREDUC  float, 
IIEDUC  float, 
IRALCRC  float, 
IIALCRC  float, 
IRMJRC  float, 
IIMJRC  float, 
IRCOCRC  float, 
IICOCRC  float, 
IRSEDRC  float, 
IISEDRC  float, 
IRTRANRC  float, 
IITRANRC  float, 
IRSTIMRC  float, 
IISTIMRC  float, 
IRANALRC  float, 
IIANALRC  float, 
IRCIGRC  float, 
IICIGRC  float, 
IRINHRC  float, 
IIINHRC  float, 
IRHALLRC  float, 
IIHALLRC  float, 
IRHERRC  float, 
IIHERRC  float, 
CATAGE  float, 
CATAG2  float, 
CATAG3  float, 
RACE  float, 
HISPRACE  float, 
EDUCCAT2  float, 
HALFLAG  float, 
HALYR  float, 
HALMON  float, 
STMFLAG  float, 
STMYR  float, 
STMMON  float, 
SEDFLAG  float, 
SEDYR  float, 
SEDMON  float, 
TRQFLAG  float, 
TRQYR  float, 
TRQMON  float, 
ANLFLAG  float, 
ANLYR  float, 
ANLMON  float, 
ALCFLAG  float, 
ALCYR  float, 
ALCMON  float, 
CIGFLAG  float, 
CIGYR  float, 
CIGMON  float, 
HERFLAG  float, 
HERYR  float, 
HERMON  float, 
MRJFLAG  float, 
MRJYR  float, 
MRJMON  float, 
COCFLAG  float, 
COCYR  float, 
COCMON  float, 
INHFLAG  float, 
INHYR  float, 
INHMON  float, 
PSYFLAG2  float, 
PSYYR2  float, 
PSYMON2  float, 
SUMFLAG  float, 
SUMYR  float, 
SUMMON  float, 
MJOFLAG  float, 
MJOYR2  float, 
MJOMON2  float, 
IEMFLAG  float, 
IEMYR  float, 
IEMMON  float, 
VESTR  float, 
VEREP  float, 
ANALWT float,
CANALWT float,
NANALWT float,
INITWT float,
WT1 float,
WT2 float,
CINITWT float,
CWT1 float,
CWT2 float,
NINITWT float,
NWT1 float,
NWT2 float
)
                
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '	'
                LINES TERMINATED BY '\n'
                LOCATION '{drug_dir}/NHSDA-1995-DS0001-data-excel'
                TBLPROPERTIES ('skip.header.line.count'='1')
"""


# In[202]:


pd.read_sql(create_table, conn)

pd.read_sql(f'SELECT * FROM {database_name}.{table_name3} LIMIT 2', conn)


# In[203]:



pd.read_sql(f'SELECT * FROM {database_name}.{table_name2} LIMIT 2', conn)


# In[204]:


pd.read_sql(f'SELECT * FROM {database_name}.{table_name} LIMIT 2', conn)


# In[205]:


pd.read_sql(f'DROP VIEW IF EXISTS all_record', conn)


# In[206]:


pd.read_sql(f'create view all_record as SELECT * FROM {database_name}.{table_name} union all SELECT * FROM {database_name}.{table_name2} union all SELECT * FROM {database_name}.{table_name3} ', conn)


# In[207]:


pd.read_sql(f'SELECT count(*) FROM all_record', conn)


# In[208]:


df = pd.read_sql(f'SELECT * FROM all_record', conn)


# In[209]:


df.head()


# In[210]:


print('Number of Rows:', df.shape[0])
print('Number of Columns:', df.shape[1], '\n')

data_types = df.dtypes
data_types = pd.DataFrame(data_types)
data_types = data_types.assign(Null_Values =  df.isnull().sum())
data_types.reset_index(inplace = True)
data_types.rename(columns={0:'Data Type',
                          'index': 'Column/Variable',
                          'Null_Values': "# of Nulls"})


# In[211]:


df.corr


# In[212]:


print(df.shape)


# In[213]:


print(df.columns)


# In[214]:


print(df.info())


# In[215]:


df.describe()


# In[216]:


# Drop columns with missing values


# In[217]:


df2 = df.dropna(axis='columns')


# In[218]:


print(df2.shape)


# In[219]:


#get index of target column
df2.columns.get_loc("herneedl")


# In[220]:


df2.drop(df2.iloc[:, 234:599], inplace = True, axis = 1)


# In[221]:


print(df2.shape)


# In[222]:


# Convert target from Continuous to Categorical
df2['herneedl'] = pd.cut(x=df2['herneedl'], bins=[0,50,100],
                     labels=['0','1'])


# In[223]:


print(df2['herneedl'].value_counts())


# In[224]:


df3 = df2


# In[225]:


print(df3.columns)


# In[226]:


df3.shape


# In[227]:


import seaborn as sns
import matplotlib.pyplot as plt


# In[228]:


df3.select_dtypes('float').hist(figsize=(25,25),ec='w')
plt.show()


# In[229]:


corr = df3.corrwith(df3['herneedl'],method='spearman').reset_index()


# In[230]:


corr.columns = ['Index','Correlations']
corr = corr.set_index('Index')
corr = corr.sort_values(by=['Correlations'], ascending = False).head(10)


# In[231]:


plt.figure(figsize=(10, 15))
fig = sns.heatmap(corr, annot=True, fmt="g", cmap='Set3', linewidths=0.4, linecolor='green')
plt.title("Correlation of Variables with Class", fontsize=20)
plt.show()


# In[232]:


array = df3.values
X = array[:,0:233]
y = array[:,233]


# In[233]:


# feature extraction
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.feature_selection import f_classif


# In[234]:


selector = SelectKBest(f_classif, k=15)
selector.fit(X, y)
# Get columns to keep and create new dataframe with those only
cols = selector.get_support(indices=True)


# In[235]:


colname = df3.columns[cols]
X_new = df3.loc[:,colname]
X_new


# In[236]:


from sklearn.decomposition import PCA


# In[237]:


pca = PCA(n_components=10)
fit = pca.fit(X_new)


# In[238]:


# summarize components
print("Explained Variance: %s" % fit.explained_variance_ratio_)


# In[239]:


# Splitting the dataframe


# In[240]:


from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X_new, y, test_size=0.3, random_state=0)


# In[241]:


X_train.shape, X_test.shape, y_train.shape, y_test.shape


# In[249]:


df3.describe()


# In[ ]:




