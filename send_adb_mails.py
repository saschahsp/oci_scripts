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
    ############################################
    # connect to database
    ############################################
    connection = None
    try:
        print("\nConnecting to database " + cmd.dname)
        connection = cx_Oracle.connect(user=cmd.duser, password=cmd.dpass, dsn=cmd.dname, encoding="UTF-8", nencoding="UTF-8")
        cursor = connection.cursor()
        print("   Connected")
    ############################################
    # Getting Emails
    ############################################
        print("Getting Emails...")
        sql = "select a.*, b.MANAGER, b.adminone, b.admintwo from V_LISTAGG_FLAGGED_ADBS a left join (select distinct EXTRACTED_EMAIL, manager, adminone, admintwo from V_SENDMAILS) b on a.EXTRACTED_EMAIL=b.EXTRACTED_EMAIL"
        cursor.execute(sql)
        l_flaggedadbs = cursor.fetchall()

    ############################################
    # Send Emails
    ############################################
        print("Sending Emails...")
        for i in range(len(l_flaggedadbs)):
            tmp_amdin_l = []
            tmp_amdin_l  = [i for i in [l_flaggedadbs[i][3],l_flaggedadbs[i][4]] if i] 
            bcss = tmp_amdin_l + ['sascha.hagedorn@oracle.com', 'leopold.gault@oracle.com', 'sinan.petrus.toma@oracle.com', 'mikko.puhakka@oracle.com']
            bccs = ', '.join(list(set(bcss)))
            cursor.callproc('SEND_ADB_MAIL',
                            ['apex_mail@oracle.com',
                            l_flaggedadbs[i][0],
                            'Private ADB',
                            str(l_flaggedadbs[i][0].split('.')[0].title()),
                            str(l_flaggedadbs[i][1]),
                            bccs
                            ])
            print("Pushed Email to ",str(l_flaggedadbs[i][0].split('.')[0].title()),' (Email:',l_flaggedadbs[i][0],') regarding ', str(l_flaggedadbs[i][1]),' to ', bccs)
        cursor.callproc('pushed')
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