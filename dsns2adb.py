import sys
import argparse
import datetime
import oci
import gzip
import os
import csv
import cx_Oracle
import time
import pytz

os.putenv("TNS_ADMIN", "/home/opc/wallet/Wallet_ADWshared")

naive= datetime.datetime.now()
timezone = pytz.timezone("Europe/Berlin")
aware1 = naive.astimezone(timezone)
current_time = str(aware1.strftime("%Y-%m-%d %H:%M:%S"))

##########################################################################
# Print header centered
##########################################################################
def print_header(name, category):
    options = {0: 90, 1: 60, 2: 30}
    chars = int(options[category])
    print("")
    print('#' * chars)
    print("#" + name.center(chars - 2, " ") + "#")
    print('#' * chars)

##########################################################################
# Create signer
##########################################################################

def create_signer(cmd):

    # assign default values
    config_file = oci.config.DEFAULT_LOCATION
    config_section = oci.config.DEFAULT_PROFILE

    if cmd.config:
        if cmd.config.name:
            config_file = cmd.config.name

    if cmd.profile:
        config_section = cmd.profile

    if cmd.instance_principals:
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            config = {'region': signer.region, 'tenancy': signer.tenancy_id}
            return config, signer
        except Exception:
            print_header("Error obtaining instance principals certificate, aborting", 0)
            raise SystemExit
    else:
        config = oci.config.from_file(config_file, config_section)
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config.get("key_file"),
            pass_phrase=oci.config.get_config_value_or_default(config, "pass_phrase"),
            private_key_content=config.get("key_content")
        )
        return config, signer
    

##########################################################################
# set parser
##########################################################################
def set_parser_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', type=argparse.FileType('r'), dest='config', help="Config File")
    parser.add_argument('-t', default="", dest='profile', help='Config file section to use (tenancy profile)')
    parser.add_argument('-f', default="", dest='fileid', help='File Id to load')
    parser.add_argument('-d', default="", dest='filedate', help='Minimum File Date to load (i.e. yyyy-mm-dd)')
    parser.add_argument('-p', default="", dest='proxy', help='Set Proxy (i.e. www-proxy-server.com:80) ')
    parser.add_argument('-ip', action='store_true', default=False, dest='instance_principals', help='Use Instance Principals for Authentication')
    parser.add_argument('-du', default="", dest='duser', help='ADB User')
    parser.add_argument('-dp', default="", dest='dpass', help='ADB Password')
    parser.add_argument('-dn', default="", dest='dname', help='ADB Name')
    

    result = parser.parse_args()

    if not (result.duser and result.dpass and result.dname):
        parser.print_help()
        print_header("You must specify database credentials!!", 0)
        return None

    return result
##########################################################################
# def check table ds_nbs
##########################################################################
def check_database_table_structure_ds_nbs(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_DS_NBS'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_DS_NBS was not exist, creating")
            sql = "create table OCI_DS_NBS ("
            sql += "    DISPLAY_NAME             VARCHAR2(200),"
            sql += "    COMPARTMENT_ID             VARCHAR2(200),"
            sql += "    LIFECYCLE_STATE              VARCHAR2(30),"
            sql += "    ID             VARCHAR2(200),"
            sql += "    NOTEBOOKSESSION_CONFIG_DETAILS             VARCHAR2(500),"
            sql += "    NOTEBOOKSESSION_URL             VARCHAR2(500),"
            sql += "    PROJECT_ID             VARCHAR2(200),"
            sql += "    TIME_CREATED              VARCHAR2(30),"
            sql += "    DEFINED_TAGS              VARCHAR2(500),"
            sql += "    FREEFORM_TAGS              VARCHAR2(500),"         
            sql += "    OCI_REGION              VARCHAR2(100)"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_DS_NBS created")
            cursor.close()
        else:
            print("Table OCI_DS_NBS exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))




##########################################################################
# Update DATA SCIENCE SESSION Function
##########################################################################

def update_oci_ds_nbs(connection,notebooklist):
    
    cursor = connection.cursor()
    sql = "delete from OCI_DS_NBS"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("OCI_DS_NBS Deleted")
######
    sql = "INSERT INTO OCI_DS_NBS ("
    sql += "    DISPLAY_NAME             ,"
    sql += "    COMPARTMENT_ID             ,"
    sql += "    LIFECYCLE_STATE              ,"
    sql += "    ID             ,"
    sql += "    NOTEBOOKSESSION_CONFIG_DETAILS             ,"
    sql += "    NOTEBOOKSESSION_URL             ,"
    sql += "    PROJECT_ID             ,"
    sql += "    TIME_CREATED              ,"
    sql += "    DEFINED_TAGS              ,"
    sql += "    FREEFORM_TAGS             ,"         
    sql += "    OCI_REGION              "
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10, "
    sql += ":11"
    sql += ") "

    cursor.prepare(sql)
    cursor.executemany(None, notebooklist)
    connection.commit()
    cursor.close()
    print("DATA SCIENCE NOTEBOOKS Updated")

##########################################################################
# Insert Update Time
##########################################################################

def update_time(connection, current_time):
    
    cursor = connection.cursor()
    report = 'DATASCIENCE_NOTEBOOKS'
    time_updated = current_time
                
######

    sql = """insert into OCI_UPDATE_TIME (REPORT, TIME_UPDATED)
          values (:report, :time_updated)"""
    cursor.execute(sql, [report, time_updated])

    connection.commit()
    cursor.close()
    print("TIME Updated")

##########################################################################
# Main
##########################################################################
def main_process():
    cmd = set_parser_arguments()
    if cmd is None:
        exit()
    config, signer = create_signer(cmd)

    ############################################
    # Start
    ############################################
    print_header("Running Users to ADW", 0)
    print("Starts at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("Command Line : " + ' '.join(x for x in sys.argv[1:]))

    ############################################
    # connect to database
    ############################################
    connection = None
    try:
        print("\nConnecting to database " + cmd.dname)
        connection = cx_Oracle.connect(user=cmd.duser, password=cmd.dpass, dsn=cmd.dname, encoding="UTF-8", nencoding="UTF-8")
        cursor = connection.cursor()
        print("   Connected")

        # Check tables structure
        print("\nChecking Database Structure...")
        check_database_table_structure_ds_nbs(connection)
    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database - " + str(e))
    ############################################
    # Getting Compartments from Database
    ############################################
        # open cursor
    cursor = connection.cursor()
    print("Getting Compartments from Database")
    # check if OCI_COMPARTMENTS table exist, if not create
    sql = "select OCID from oci_compartments where LIFECYCLE_STATE = 'ACTIVE'"
    cursor.execute(sql)
    l_ocid = cursor.fetchall()
    l_ocid_n = []
    for c in range(len(l_ocid)):
        l_ocid_n.append(l_ocid[c][0])

    ############################################
    # API extract ADBs
    ############################################

    try:
        print("\nConnecting to DS Client...")
        datascienceclient = oci.data_science.DataScienceClient(config, signer=signer)
        if cmd.proxy:
            datascienceclient.base_client.session.proxies = {'https': cmd.proxy}
        
        print("Getting Data Science Notebooks")
        notebooklist = []
        for region in [#'ap-sydney-1',
        'ap-tokyo-1',
        'us-phoenix-1',
        'us-ashburn-1',
        'eu-frankfurt-1',
        'uk-london-1',
        'eu-amsterdam-1',
        #'ca-toronto-1',
        #'sa-saopaulo-1'
        ]:
            config['region'] = region
            datascienceclient = oci.data_science.DataScienceClient(config, signer=signer)
            print('Check for...',config['region'])
            for a in range(len(l_ocid_n)):
                notebooks = datascienceclient.list_notebook_sessions(compartment_id = l_ocid_n[a])
                if len(notebooks.data) != 0:
                    for i in range(len(notebooks.data)):

                        row_data = (
                            notebooks.data[i].display_name,
                            notebooks.data[i].compartment_id,
                            notebooks.data[i].id,
                            notebooks.data[i].lifecycle_state,
                            str(notebooks.data[i].notebook_session_configuration_details),
                            notebooks.data[i].notebook_session_url,
                            notebooks.data[i].project_id,
                            notebooks.data[i].time_created.isoformat(),
                            str(notebooks.data[i].defined_tags),
                            str(notebooks.data[i].freeform_tags),
                            region
                            
                        )
                        print('Listed...', notebooks.data[i].display_name)
                        notebooklist.append(row_data)
    except Exception as e:
        print("\nError extracting ADBs - " + str(e) + "\n")
        raise SystemExit           
                        
    ############################################
    # Update ADBs
    ############################################
    update_oci_ds_nbs(connection,notebooklist)
    cursor.close()

    ############################################
    # print completed
    ############################################
    print("\nCompleted at " + current_time)
    update_time(connection, current_time)
    
##########################################################################
# Execute Main Process
##########################################################################
main_process()