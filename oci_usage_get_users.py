import sys
import argparse
import datetime
import oci
import gzip
import os
import csv
import cx_Oracle

os.putenv("TNS_ADMIN", "/home/opc/wallet/Wallet_ADWshared")

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
# def check table users
##########################################################################
def check_database_table_structure_users(connection):
    try:
        # open cursor
        cursor = connection.cursor()

        # check if OCI_COMPARTMENTS table exist, if not create
        sql = "select count(*) from user_tables where table_name = 'OCI_USERS'"
        cursor.execute(sql)
        val, = cursor.fetchone()

        # if table not exist, create it
        if val == 0:
            print("Table OCI_USERS was not exist, creating")
            sql = "create table OCI_USERS ("
            sql += "    COMPARTMENT_ID             VARCHAR2(200),"
            sql += "    DESCRIPTION             VARCHAR2(200),"
            sql += "    EMAIL      VARCHAR(100),"
            sql += "    OCID             VARCHAR2(200),"
            sql += "    IDENTITY_PROVIDER_ID            VARCHAR2(300),"
            sql += "    INACTIVE      VARCHAR2(30),"
            sql += "    MFA_ACTIVATED    VARCHAR2(30),"
            sql += "    LIFECYCLE_STATE              VARCHAR2(30),"
            sql += "    NAME              VARCHAR2(100),"
            sql += "    TIME_CREATED              VARCHAR2(500)"
            #sql += "    CONSTRAINT primary_key PRIMARY KEY (OCID)"
            sql += ") COMPRESS"
            cursor.execute(sql)
            print("Table OCI_USERS created")
        else:
            print("Table OCI_USERS exist")

    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database at check_database_table_structure_usage() - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database at check_database_table_structure_usage() - " + str(e))

##########################################################################
# Update Users
##########################################################################

def update_users(connection):
    
    cursor = connection.cursor()
    sql = "delete from OCI_USERS"
    cursor.execute(sql)
    sql = "begin commit; end;"
    cursor.execute(sql)
    print("Users Deleted")
######
    sql = "INSERT INTO OCI_USERS ("
    sql += "    COMPARTMENT_ID,"
    sql += "    DESCRIPTION,"
    sql += "    EMAIL,"
    sql += "    OCID,"
    sql += "    IDENTITY_PROVIDER_ID,"
    sql += "    INACTIVE,"
    sql += "    MFA_ACTIVATED,"
    sql += "    LIFECYCLE_STATE,"
    sql += "    NAME,"
    sql += "    TIME_CREATED"
    sql += ") VALUES ("
    sql += ":1, :2, :3, :4, :5,  "
    sql += ":6, :7, :8, :9, :10 "
    sql += ")"
    cursor.prepare(sql)
    cursor.executemany(None, userlist)
    connection.commit()
    print("Users Updated")
    cursor.close()


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
    # Identity extract compartments
    ############################################
    tenancy = None
    try:
        print("\nConnecting to Identity Service...")
        identity = oci.identity.IdentityClient(config, signer=signer)
        if cmd.proxy:
            identity.base_client.session.proxies = {'https': cmd.proxy}

        tenancy = identity.get_tenancy(config["tenancy"]).data
        print("   Tenant Name : " + str(tenancy.name))
        print("   Tenant Id   : " + tenancy.id)
        print("")
        
        print("Getting Users")
        l_users = identity.list_users(compartment_id=tenancy.id)
        userlist = []
        for i in range(len(l_users.data)):
            user_data = (
                l_users.data[i].compartment_id,
                l_users.data[i].description,
                l_users.data[i].email,
                l_users.data[i].id,
                l_users.data[i].identity_provider_id,
                str(l_users.data[i].inactive_status),
                str(l_users.data[i].is_mfa_activated),
                l_users.data[i].lifecycle_state,
                l_users.data[i].name,
                l_users.data[i].time_created.isoformat()

            )
            userlist.append(user_data)
        print("Downloaded Users")
    except Exception as e:
        print("\nError extracting users - " + str(e) + "\n")
        raise SystemExit

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
        check_database_table_structure_users(connection)
        cursor.close()
    except cx_Oracle.DatabaseError as e:
        print("\nError manipulating database - " + str(e) + "\n")
        raise SystemExit

    except Exception as e:
        raise Exception("\nError manipulating database - " + str(e))
    ############################################
    # print completed
    ############################################
    print("\nCompleted at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


##########################################################################
# Execute Main Process
##########################################################################
main_process()