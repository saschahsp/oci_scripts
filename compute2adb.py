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
def check_database_table_structure_compute(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_COMPUTE'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_COMPUTE was not exist, creating")
            sql = "create table OCI_COMPUTE ("
            sql += "    AGENT_CONFIG             VARCHAR2(200),"
            sql += "    AVAILABILITY_DOMAIN             VARCHAR2(200),"
            sql += "    COMPARTMENT_ID             VARCHAR2(200),"
            sql += "    DEDICATED_VM_HOST_ID             VARCHAR2(200),"
            sql += "    DEFINED_TAGS              VARCHAR2(500),"
            sql += "    DISPLAY_NAME             VARCHAR2(200),"
            sql += "    EXTENDED_METADATA             VARCHAR2(200),"
            sql += "    FAULT_DOMAIN             VARCHAR2(200),"
            sql += "    FREEFORM_TAGS              VARCHAR2(500),"
            sql += "    ID             VARCHAR2(200),"
            sql += "    IMAGE_ID             VARCHAR2(200),"
            sql += "    IPXE_SCRIPT             VARCHAR2(200),"
            sql += "    LAUNCH_MODE             VARCHAR2(200),"
            sql += "    LIFECYCLE_STATE             VARCHAR2(200),"
            sql += "    METADATA             VARCHAR2(200),"
            sql += "    REGION             VARCHAR2(200),"
            sql += "    SHAPE             VARCHAR2(200),"
            sql += "    SHAPE_CONFIG             VARCHAR2(200),"
            sql += "    SOURCE_DETAILS             VARCHAR2(200),"
            sql += "    SYSTEM_TAGS             VARCHAR2(200),"
            sql += "    TIME_CREATED             VARCHAR2(200),"
            sql += "    TIME_MAINTENANCE_REBOOT_DUE             VARCHAR2(200),"
            sql += "    OCI_REGION              VARCHAR2(100)"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_COMPUTE created")
            cursor.close()
        else:
            print("Table OCI_COMPUTE exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))




##########################################################################
# Update DATA SCIENCE SESSION Function
##########################################################################

def update_oci_compute(connection,computelist):
    
    cursor = connection.cursor()
    sql = "delete from OCI_COMPUTE"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("OCI_COMPUTE Deleted")
######
    sql = "INSERT INTO OCI_COMPUTE ("
    sql += "    AGENT_CONFIG             ,"
    sql += "    AVAILABILITY_DOMAIN             ,"
    sql += "    COMPARTMENT_ID             ,"
    sql += "    DEDICATED_VM_HOST_ID            ,"
    sql += "    DEFINED_TAGS              ,"
    sql += "    DISPLAY_NAME             ,"
    sql += "    EXTENDED_METADATA             ,"
    sql += "    FAULT_DOMAIN             ,"
    sql += "    FREEFORM_TAGS              ,"
    sql += "    ID           ,"
    sql += "    IMAGE_ID             ,"
    sql += "    IPXE_SCRIPT            ,"
    sql += "    LAUNCH_MODE            ,"
    sql += "    LIFECYCLE_STATE             ,"
    sql += "    METADATA             ,"
    sql += "    REGION             ,"
    sql += "    SHAPE             ,"
    sql += "    SHAPE_CONFIG             ,"
    sql += "    SOURCE_DETAILS             ,"
    sql += "    SYSTEM_TAGS             ,"
    sql += "    TIME_CREATED             ,"
    sql += "    TIME_MAINTENANCE_REBOOT_DUE             ,"
    sql += "    OCI_REGION             "
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10, "
    sql += ":11, 12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23"
    sql += ") "

    cursor.prepare(sql)
    cursor.executemany(None, computelist)
    connection.commit()
    cursor.close()
    print("COMPUTE Updated")

##########################################################################
# Insert Update Time
##########################################################################

def update_time(connection, current_time):
    
    cursor = connection.cursor()
    report = 'COMPUTE'
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
        check_database_table_structure_compute(connection)
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
        print("\nConnecting to Compute Client...")
        computeclient = oci.core.ComputeClient(config, signer=signer)
        if cmd.proxy:
            datascienceclient.base_client.session.proxies = {'https': cmd.proxy}
        
        print("Getting Compute Instances")
        computelist = []
        for region in [#'ap-sydney-1',
        #'ap-tokyo-1',
        #'us-phoenix-1',
        #'us-ashburn-1',
        'eu-frankfurt-1',
        'uk-london-1',
        #'eu-amsterdam-1',
        #'ca-toronto-1',
        #'sa-saopaulo-1'
        ]:#oci.regions.REGIONS:
            config['region'] = region
            computeclient = oci.core.ComputeClient(config, signer=signer)
            #time.sleep(60)
            print('Check for...',config['region'])
            for a in range(len(l_ocid_n)):       
                instances = computeclient.list_instances(compartment_id = l_ocid_n[a])
                if len(instances.data) != 0:
                    for i in range(len(instances.data)):

                        row_data = (
                            str(instances.data[i].agent_config),
                            instances.data[i].availability_domain,
                            instances.data[i].compartment_id,
                            instances.data[i].dedicated_vm_host_id,
                            str(instances.data[i].defined_tags),
                            instances.data[i].display_name,
                            str(instances.data[i].extended_metadata),
                            instances.data[i].fault_domain,
                            str(instances.data[i].freeform_tags),
                            instances.data[i].id,
                            instances.data[i].image_id,
                            'null',#instances.data[i].ipxe_script,
                            instances.data[i].launch_mode,
                            str(instances.data[i].launch_options),
                            instances.data[i].lifecycle_state,
                            'null',#instances.data[i].metadata,
                            instances.data[i].region,
                            str(instances.data[i].shape),
                            'null',#instances.data[i].shape_config,
                            instances.data[i].source_details,
                            str(instances.data[i].system_tags),
                            instances.data[i].time_created.isoformat(),
                            'null',#instances.data[i].time_maintenance_reboot_due,
                            region
            
                        )
                        print('\tListed...', instances.data[i].display_name)
                        computelist.append(row_data)
    except Exception as e:
        print("\nError extracting Compute - " + str(e) + "\n")
        raise SystemExit           
                        
    ############################################
    # Update ADBs
    ############################################
    update_oci_compute(connection,computelist)
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