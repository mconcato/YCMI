"""
Michael Concato
Information and password storage for REDCap and MySQL
 
"""


Mysql_Tables = dict()


"""
To add new MySQL database, copy and past next line below last MySQL database above, and fill in
Mysql_Tables[''] = ["", "", ""]
Mysql_Tables[(database name] = [(user), (password), (server name)]
"""

REDCap_Projects = dict()


"""
To create new, copy and paste next line below last REDCap project aboce, and fill in accordingly
REDCap_Projects[''] = ["",""]
REDCap_Projects[(project name] = [(project url), (project token)]
"""

def valid_mysql(database):
    return Mysql_Tables.get(database) is not None


def valid_redcap(project):
    return REDCap_Projects.get(project) is not None
