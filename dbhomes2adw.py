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
# def check table dbhomes
##########################################################################
def check_database_table_structure_dbhomes(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_DBHOMES'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_DBHOMES was not exist, creating")
            sql = "create table OCI_dbhomes ("
            sql += "    COMPARTMENT_ID             VARCHAR2(200),"
            sql += "    db_home_location VARCHAR2(300),"
            sql += "    db_system_id      VARCHAR(200),"
            sql += "    db_version             VARCHAR2(20),"
            sql += "    display_name            VARCHAR2(100),"
            sql += "    id      VARCHAR2(300),"
            sql += "    last_patch_history_entry_id    VARCHAR2(300),"
            sql += "    PREVIEW    VARCHAR2(100),"
            sql += "    lifecycle_details              VARCHAR(30),"
            sql += "    LIFECYCLE_STATE              VARCHAR2(30),"
            sql += "    TIME_CREATED              VARCHAR2(50),"
            sql += "    vm_cluster_id              VARCHAR2(300),"
            sql += "    LIFECYCLE_STATE_AVAILABLE              VARCHAR2(300),"
            sql += "    LIFECYCLE_STATE_FAILED              VARCHAR2(300),"
            sql += "    LIFECYCLE_STATE_PROVISIONING              VARCHAR2(300),"
            sql += "    LIFECYCLE_STATE_TERMINATED              VARCHAR2(500),"
            sql += "    LIFECYCLE_STATE_TERMINATING              VARCHAR2(500),"
            sql += "    LIFECYCLE_STATE_UPDATING              VARCHAR2(500)"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_DBHOMES created")
            cursor.close()
        else:
            print("Table OCI_DBHOMES exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))




##########################################################################
# Update ADBs Function
##########################################################################

def update_dbhomes(connection,adblist):
    
    cursor = connection.cursor()
    sql = "delete from OCI_DBHOMES"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("DB HOMES Deleted")
######
    sql = "INSERT INTO OCI_DBHOMES ("
    sql += "    COMPARTMENT_ID,"
    sql += "    db_home_location,"
    sql += "    db_system_id,"
    sql += "    db_version,"
    sql += "    display_name,"
    sql += "    id,"
    sql += "    last_patch_history_entry_id,"
    sql += "    PREVIEW,"
    sql += "    lifecycle_details,"
    sql += "    LIFECYCLE_STATE,"
    sql += "    TIME_CREATED,"
    sql += "    vm_cluster_id,"
    sql += "    LIFECYCLE_STATE_AVAILABLE,"
    sql += "    LIFECYCLE_STATE_FAILED,"
    sql += "    LIFECYCLE_STATE_PROVISIONING,"
    sql += "    LIFECYCLE_STATE_TERMINATED,"
    sql += "    LIFECYCLE_STATE_TERMINATING,"
    sql += "    LIFECYCLE_STATE_UPDATING"
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10, "
    sql += ":11, :12 , :13, :14, :15, :16, :17, :18"
    sql += ") "

    cursor.prepare(sql)
    cursor.executemany(None, dbhomelist)
    connection.commit()
    cursor.close()
    print("DB HOMES Updated")

##########################################################################
# Insert Update Time
##########################################################################

def update_time(connection, current_time):
    
    cursor = connection.cursor()
    report = 'DB_HOMES'
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
    print_header("Running DB HOMES to ADW", 0)
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
        check_database_table_structure_dbhomes(connection)
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
        print("\nConnecting to DB Client...")
        dbclient = oci.database.DatabaseClient(config, signer=signer)
        if cmd.proxy:
            dbclient.base_client.session.proxies = {'https': cmd.proxy}
        
        print("Getting DB HOMES")
        dbhomelist = []
        for a in range(len(l_ocid_n)):
            dbhomes = dbclient.list_db_homes(compartment_id = l_ocid_n[a])
            if len(dbhomes.data) != 0:
                for i in range(len(dbhomes.data)):
                    print("Getting... ", dbhomes.data[i].display_name)
                    row_data = (
                        dbhomes.data[i].compartment_id,
                        dbhomes.data[i].db_home_location,
                        dbhomes.data[i].db_system_id,
                        dbhomes.data[i].db_version,
                        dbhomes.data[i].display_name,
                        dbhomes.data[i].id,
                        dbhomes.data[i].last_patch_history_entry_id,
                        dbhomes.data[i].lifecycle_details,
                        dbhomes.data[i].lifecycle_state,
                        dbhomes.data[i].time_created.isoformat(),
                        dbhomes.data[i].vm_cluster_id,
                        dbhomes.data[i].LIFECYCLE_STATE_AVAILABLE,
                        dbhomes.data[i].LIFECYCLE_STATE_FAILED,
                        dbhomes.data[i].LIFECYCLE_STATE_PROVISIONING,
                        dbhomes.data[i].LIFECYCLE_STATE_TERMINATED,
                        dbhomes.data[i].LIFECYCLE_STATE_TERMINATING,
                        dbhomes.data[i].LIFECYCLE_STATE_UPDATING
                    )

                    dbhomelist.append(row_data)
    except Exception as e:
        print("\nError extracting DB HOMES - " + str(e) + "\n")
        raise SystemExit           
                        
    ############################################
    # Update ADBs
    ############################################
    update_adbs(connection,dbhomelist)
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