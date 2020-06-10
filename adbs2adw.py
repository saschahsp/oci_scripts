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
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="logfile", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")

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
# def check table adbs
##########################################################################
def check_database_table_structure_adbs(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_ADBS'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_ADBS was not exist, creating")
            sql = "create table OCI_ADBS ("
            sql += "    CONTAINER_ID             VARCHAR2(200),"
            sql += "    COMPARTMENT_ID             VARCHAR2(200),"
            sql += "    OCPUS    NUMBER,"
            sql += "    DISPLAY_NAME      VARCHAR(100),"
            sql += "    OCID             VARCHAR2(200),"
            sql += "    AUTO_SCALING            VARCHAR2(30),"
            sql += "    DEDICATED      VARCHAR2(30),"
            sql += "    FREE_TIER    VARCHAR2(30),"
            sql += "    PREVIEW    VARCHAR2(100),"
            sql += "    LICENSE_MODEL              VARCHAR(30),"
            sql += "    LIFECYCLE_STATE              VARCHAR2(30),"
            sql += "    TIME_CREATED              VARCHAR2(30),"
            sql += "    TIME_DELETION_OF_FREE_ADB              VARCHAR2(300),"
            sql += "    TIME_MAINTENANCE_BEGIN              VARCHAR2(300),"
            sql += "    TIME_MAINTENANCE_END              VARCHAR2(300),"
            sql += "    TIME_RECLAMATION_OF_FREE_ADB              VARCHAR2(300),"
            sql += "    DEFINED_TAGS              VARCHAR2(500),"
            sql += "    REGION              VARCHAR2(100)"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_ADBS created")
            cursor.close()
        else:
            print("Table OCI_ADBS exist")
            logging.info("Table OCI_ADBS exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))




##########################################################################
# Update ADBs Function
##########################################################################

def update_adbs(connection,adblist):
    
    cursor = connection.cursor()
    sql = "delete from OCI_ADBS"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("ADBS Deleted")
    logging.info("ADBS Deleted")
######
    sql = "INSERT INTO OCI_ADBS ("
    sql += "CONTAINER_ID,"
    sql += "COMPARTMENT_ID,"
    sql += "OCPUS,"
    sql += "DISPLAY_NAME,"
    sql += "OCID,"
    sql += "AUTO_SCALING,"
    sql += "DEDICATED,"
    sql += "FREE_TIER,"
    sql += "PREVIEW,"
    sql += "LICENSE_MODEL,"
    sql += "LIFECYCLE_STATE,"
    sql += "TIME_CREATED,"
    sql += "TIME_DELETION_OF_FREE_ADB,"
    sql += "TIME_MAINTENANCE_BEGIN,"
    sql += "TIME_MAINTENANCE_END,"
    sql += "TIME_RECLAMATION_OF_FREE_ADB,"
    sql += "DEFINED_TAGS,"
    sql += "REGION"
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10, "
    sql += ":11, :12 , :13, :14, :15, :16, :17, :18"
    sql += ") "

    cursor.prepare(sql)
    cursor.executemany(None, adblist)
    connection.commit()
    cursor.close()
    print("ADBs Updated")
    logging.info("ADBs Updated")

##########################################################################
# Insert Update Time
##########################################################################

def update_time(connection, current_time):
    
    cursor = connection.cursor()
    report = 'ADBS'
    time_updated = current_time
                
######

    sql = """insert into OCI_UPDATE_TIME (REPORT, TIME_UPDATED)
          values (:report, :time_updated)"""
    cursor.execute(sql, [report, time_updated])

    connection.commit()
    cursor.close()
    print("TIME Updated")
    logging.info("TIME Updated")

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
        check_database_table_structure_adbs(connection)
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
        print("\nConnecting to ADB Client...")
        logging.info("Connecting to ADB Client...")
        adbclient = oci.database.DatabaseClient(config, signer=signer)
        if cmd.proxy:
            adbclient.base_client.session.proxies = {'https': cmd.proxy}
        
        print("Getting ADBs")
        adblist = []
        for region in [#'ap-sydney-1',
        #'ap-tokyo-1',
        #'us-phoenix-1',
        #'us-ashburn-1',
        'eu-frankfurt-1',
        'uk-london-1',
        #'ca-toronto-1',
        'eu-amsterdam-1'
        #'sa-saopaulo-1'
        ]:
            config['region'] = region
            adbclient = oci.database.DatabaseClient(config)
            print('Check for...',config['region'])
            for a in range(len(l_ocid_n)):
                testadb = adbclient.list_autonomous_databases(compartment_id = l_ocid_n[a])
                if len(testadb.data) != 0:
                    for i in range(len(testadb.data)):

                        row_data = (
                            testadb.data[i].autonomous_container_database_id,
                            testadb.data[i].compartment_id,
                            testadb.data[i].cpu_core_count,
                            testadb.data[i].display_name,
                            testadb.data[i].id,
                            str(testadb.data[i].is_auto_scaling_enabled),
                            str(testadb.data[i].is_dedicated),
                            str(testadb.data[i].is_free_tier),
                            str(testadb.data[i].is_preview),
                            testadb.data[i].license_model,
                            testadb.data[i].lifecycle_state,
                            testadb.data[i].time_created.isoformat(),
                            str(testadb.data[i].time_deletion_of_free_autonomous_database),
                            testadb.data[i].time_maintenance_begin.isoformat(),
                            testadb.data[i].time_maintenance_end.isoformat(),
                            str(testadb.data[i].time_reclamation_of_free_autonomous_database),
                            str(testadb.data[i].defined_tags),
                            region
                            )
                        adblist.append(row_data)
    except Exception as e:
        print("\nError extracting ADBs - " + str(e) + "\n")
        raise SystemExit           
                        
    ############################################
    # Update ADBs
    ############################################
    update_adbs(connection,adblist)
    cursor.close()

    ############################################
    # print completed
    ############################################
    print("\nCompleted at " + current_time)
    update_time(connection, current_time)
    logging.info("Completed at " + current_time")
    
##########################################################################
# Execute Main Process
##########################################################################
main_process()