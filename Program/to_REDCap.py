from MySQL_to_REDCap_Transfer import transfer
from passwords import valid_mysql, valid_redcap, REDCap_Projects, Mysql_Tables
from redcap import Project
import mysql.connector

MYSQL_EXPORTED_DATABASE = ''
REDCAP_IMPORTED_PROJECT = ''


if not (valid_redcap(REDCAP_IMPORTED_PROJECT) and valid_mysql(MYSQL_EXPORTED_DATABASE)):
    print("Do not recognize MySQL database") if not valid_mysql(MYSQL_EXPORTED_DATABASE) else \
        print("Do not recognize REDCap project")

else:
    # REDCap Project
    print("Loading REDCap project '" + REDCAP_IMPORTED_PROJECT + "'"),

    URL = REDCap_Projects[REDCAP_IMPORTED_PROJECT][0]
    TOKEN = REDCap_Projects[REDCAP_IMPORTED_PROJECT][1]

    project = Project(
        url=URL,
        token=TOKEN,
        verify_ssl=False
    )
    print("Done")

    # MySQL Database
    print("Loading MySQL Database '" + MYSQL_EXPORTED_DATABASE + "'")

    USER = Mysql_Tables[MYSQL_EXPORTED_DATABASE][0]
    PASSWORD = Mysql_Tables[MYSQL_EXPORTED_DATABASE][1]
    HOST = Mysql_Tables[MYSQL_EXPORTED_DATABASE][2]

    database = mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        database=MYSQL_EXPORTED_DATABASE,
        buffered=True
    )
    print("Done\n\n----\n")

    # Transfer
    transfer(database, project)
    database.close()
