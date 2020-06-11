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

os.putenv("TNS_ADMIN", "/home/opc/wallet/Wallet_ADWshared")

filename = '/home/opc/oci_usage/logs/logfile_compartments2adw_' + str(datetime.datetime.utcnow())
logging.basicConfig(level=logging.DEBUG, filename=filename, filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s")

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
# def check table compartments
##########################################################################
def check_database_table_structure_compartments(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_COMPARTMENTS'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_COMPARTMENTS was not exist, creating")
            sql = "create table OCI_COMPARTMENTS ("
            sql += "    PARENT             VARCHAR2(100),"
            sql += "    PARENTOCID             VARCHAR2(100),"
            sql += "    OCID                 VARCHAR2(100),"
            sql += "    DEFINED_TAGS    VARCHAR(500),"
            sql += "    DESCRIPTION      VARCHAR(100),"
            sql += "    FREEFORM_TAGS             VARCHAR2(500),"
            sql += "    INACTIVE_STATUS            VARCHAR2(30),"
            sql += "    IS_ACCESSIBLE      VARCHAR2(30),"
            sql += "    LIFECYCLE_STATE    VARCHAR2(30),"
            sql += "    NAME    VARCHAR2(100),"
            sql += "    TIME_CREATED              VARCHAR(300),"
            sql += "    PATH              VARCHAR2(300),"
            sql += "    DEPTH              NUMBER"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_COMPARTMENTS created")
        else:
            print("Table OCI_COMPARTMENTS exist")
            logging.info("Table OCI_COMPARTMENTS exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))




##########################################################################
# Update ADBs Function
##########################################################################

def update_compartments(connection,compartmentlist):
    
    cursor = connection.cursor()
    sql = "delete from OCI_COMPARTMENTS"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("COMPARTMENTS Deleted")
    logging.info("COMPARTMENTS Deleted")
######
    # insert bulk to database
    cursor = cx_Oracle.Cursor(connection)
    sql = "INSERT INTO OCI_COMPARTMENTS ("
    sql += "PARENT,"
    sql += "PARENTOCID,"
    sql += "OCID,"
    sql += "DEFINED_TAGS, "
    sql += "DESCRIPTION, "
    sql += "FREEFORM_TAGS, "
    # 6
    sql += "INACTIVE_STATUS, "
    sql += "IS_ACCESSIBLE, "
    sql += "LIFECYCLE_STATE, "
    sql += "NAME, "
    sql += "TIME_CREATED, "
    # 11
    sql += "PATH, "
    sql += "DEPTH "
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10, "
    sql += ":11, :12 , :13"
    sql += ") "

    cursor.prepare(sql)
    cursor.executemany(None, compartmentlist)
    connection.commit()
    cursor.close()

##########################################################################
# Insert Update Time
##########################################################################

def update_time(connection, current_time):
    
    cursor = connection.cursor()
    report = 'COMPARTMENTS'
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
    logging.info("Running Users to ADW")
    logging.info("Starts at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    logging.info("Command Line : " + ' '.join(x for x in sys.argv[1:]))
    ############################################
    # connect to database
    ############################################
    connection = None
    try:
        print("\nConnecting to database " + cmd.dname)
        logging.info("\nConnecting to database " + cmd.dname)
        connection = cx_Oracle.connect(user=cmd.duser, password=cmd.dpass, dsn=cmd.dname, encoding="UTF-8", nencoding="UTF-8")
        cursor = connection.cursor()
        print("   Connected")
        logging.info("   Connected")
        # Check tables structure
        print("\nChecking Database Structure...")
        logging.info("\nChecking Database Structure...")
        check_database_table_structure_compartments(connection)
    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database - " + str(e))


    ############################################
    # API extract COMPARTMENTS
    ############################################

    try:
        print("\nConnecting to IDENTITY/COMPARTMENT Client...")
        logging.info("\nConnecting to IDENTITY/COMPARTMENT Client...")
        idclient = oci.identity.IdentityClient(config, signer=signer)
        if cmd.proxy:
            idclient.base_client.session.proxies = {'https': cmd.proxy}
        tenancy = idclient.get_tenancy(config["tenancy"]).data
        print("   Tenant Name : " + str(tenancy.name))
        print("   Tenant Id   : " + tenancy.id)
        print("")
        print("GettingCOMPARTMENTS")
        logging.info("   Tenant Name : " + str(tenancy.name))
        logging.info("   Tenant Id   : " + tenancy.id)
        logging.info("")
        logging.info("GettingCOMPARTMENTS")        
        data = []
        compartments = idclient.list_compartments(compartment_id=tenancy.id)
        for i in range(len(compartments.data)):
            if i == 6:
                time.sleep(60)
            print("Getting...", "root / ",compartments.data[i].name, 'i:', i)
            logging.info("Getting...", "root / ",compartments.data[i].name, 'i:', i)
            row_data = (
                "root",
                compartments.data[i].compartment_id,
                compartments.data[i].id,
                str(compartments.data[i].defined_tags),
                compartments.data[i].description,
                str(compartments.data[i].freeform_tags),
                compartments.data[i].inactive_status,
                compartments.data[i].is_accessible,
                compartments.data[i].lifecycle_state,
                compartments.data[i].name,
                compartments.data[i].time_created.isoformat(),
                'root/',
                1
                    )
            data.append(row_data)
            compartments1 = idclient.list_compartments(compartment_id= compartments.data[i].id)
            print('the parent is: ', 'root', 'i:', i)
            logging.info('the parent is: ', 'root', 'i:', i)
            if len(compartments1.data) != 0:
                print('root'," / ", compartments.data[i].name, "nested...")
                logging.info('root'," / ", compartments.data[i].name, "nested...")
                for a in range(len(compartments1.data)):
                    row_data = (
                        compartments.data[i].name,
                        compartments1.data[a].compartment_id,
                        compartments1.data[a].id,
                        str(compartments1.data[a].defined_tags),
                        compartments1.data[a].description,
                        str(compartments1.data[a].freeform_tags),
                        compartments1.data[a].inactive_status,
                        compartments1.data[a].is_accessible,
                        compartments1.data[a].lifecycle_state,
                        compartments1.data[a].name,
                        compartments1.data[a].time_created.isoformat(),
                        'root/' + compartments.data[i].name + '/'+ compartments1.data[a].name,
                        2
                            )
                    data.append(row_data)
                    print("\tGetting...", compartments.data[i].name," / ",compartments1.data[a].name)
                    print('\tthe parent is: ', compartments.data[i].name, 'i:', i, 'a:',a)
                    logging.info("\tGetting...", compartments.data[i].name," / ",compartments1.data[a].name)
                    logging.info('\tthe parent is: ', compartments.data[i].name, 'i:', i, 'a:',a)
                    compartments2 = idclient.list_compartments(compartment_id= compartments1.data[a].id)
                    if len(compartments2.data) != 0:               
                        print('\troot'," / ", compartments.data[i].name,' / ', compartments1.data[i].name,"nested...")
                        logging.info('\troot'," / ", compartments.data[i].name,' / ', compartments1.data[i].name,"nested...")
                        for b in range(len(compartments2.data)):
                            row_data = (
                                compartments1.data[a].name,
                                compartments2.data[b].compartment_id,
                                compartments2.data[b].id,
                                str(compartments2.data[b].defined_tags),
                                compartments2.data[b].description,
                                str(compartments2.data[b].freeform_tags),
                                compartments2.data[b].inactive_status,
                                compartments2.data[b].is_accessible,
                                compartments2.data[b].lifecycle_state,
                                compartments2.data[b].name,
                                compartments2.data[b].time_created.isoformat(),
                                   'root/' + compartments.data[i].name + '/'+ compartments1.data[a].name + '/' + compartments2.data[b].name
                                      )
                            data.append(row_data)
                            print("\t\tGetting...", compartments.data[i].name," / ",compartments1.data[a].name, " / ",compartments2.data[b].name)
                            print('\t\tthe parent is: ', compartments1.data[a].name, 'i:', i, 'a:',a ,'b:', b)
                            logging.info("\t\tGetting...", compartments.data[i].name," / ",compartments1.data[a].name, " / ",compartments2.data[b].name)
                            logging.info('\t\tthe parent is: ', compartments1.data[a].name, 'i:', i, 'a:',a ,'b:', b)

    except Exception as e:
        print("\nError extracting COMPARTMENTS - " + str(e) + "\n")
        raise SystemExit           
                        
    ############################################
    # Update ADBs
    ############################################
    update_compartments(connection,data)
    cursor.close()

    ############################################
    # print completed
    ############################################
    print("\nCompleted at " + current_time)
    logging.info("\nCompleted at " + current_time)
    update_time(connection, current_time)

    
##########################################################################
# Execute Main Process
##########################################################################
main_process()