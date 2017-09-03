import mysql.connector
from REDCap_to_MySQL_Transfer import transfer
from passwords import valid_mysql, valid_redcap, REDCap_Projects, Mysql_Tables
from redcap import Project

REDCAP_EXPORTED_PROJECT = ''
MYSQL_IMPORTED_DATABASE = ''


if not (valid_redcap(REDCAP_EXPORTED_PROJECT) and valid_mysql(MYSQL_IMPORTED_DATABASE)):
    if not valid_mysql(MYSQL_IMPORTED_DATABASE):
        print("Do not recognize MySQL database")
    else:
        print("Do not recognize REDCap project")

else:
    # REDCap Project
    print("Loading REDCap project '" + REDCAP_EXPORTED_PROJECT + "' ...")

    URL = REDCap_Projects[REDCAP_EXPORTED_PROJECT][0]
    TOKEN = REDCap_Projects[REDCAP_EXPORTED_PROJECT][1]

    project = Project(
        url=URL,
        token=TOKEN,
        verify_ssl=False
    )
    print("Done")

    # MySQL Database
    print("Loading MySQL Database '" + MYSQL_IMPORTED_DATABASE + "' ...")

    USER = Mysql_Tables[MYSQL_IMPORTED_DATABASE][0]
    PASSWORD = Mysql_Tables[MYSQL_IMPORTED_DATABASE][1]
    HOST = Mysql_Tables[MYSQL_IMPORTED_DATABASE][2]

    database = mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        database=MYSQL_IMPORTED_DATABASE,
        buffered=True
    )
    print("Done\n----\n")

    # Transfer
    transfer(database, project)
    database.close()
