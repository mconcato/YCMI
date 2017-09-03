import logging
logging.captureWarnings(True)


# MARK: Database Info


# MARK: Database Functions
# To import one record from mysql to REDCap, change to correct format
def import_record(columns, values):
    """
    :param columns: columns names of mysql table
    :param values: values for a mysql row
    :return: dictionary mapping variable to data
    """
    # Each record will be a dictionary (within a list), mapping variable to data
    response = {}

    if len(columns.split(",")) != len(values):  # Make sure columns and values are equal length
        print("Error: Column length: " + str(len(columns.split(","))) + ", value length: " + str(len(values)))
    else:
        # Go through values to be added and add to dictionary
        for i in range(len(columns.split(","))):
            if values[i] == 'None':
                values[i] = ''
            elif 'bytearray' in str(values[i]):
                temp = str(values[i]).split("'")
                values[i] = temp[1]
            response[columns.split(",")[i].strip()] = values[i]

    return response


# MARK: API Functions

# Get variables of REDCap project in the order the are in the project
def get_variable_order(meta, form):
    var_order = []

    # Go through each variable and add to list
    for detail in meta:
        if detail['form_name'] == form:
            # If checkbox, only one field but multiple variables
            if detail['field_type'] == "checkbox":
                for i in range(len(detail['select_choices_or_calculations'].split("|"))):
                    new_var = detail['field_name'] + "___" + str(i + 1)
                    var_order.append(new_var)
            else:
                var_order.append(detail['field_name'])

    if var_order[0] == 'study_id':
        return var_order
    else:
        var_order.insert(0, 'study_id')
        return var_order


# MARK: Actions

def check_for_errors(redcap_vars, mysql_vars):
    temp = list(mysql_vars).copy()
    bad_vars = list()
    errors = False
    error_list = list()

    # If length of variables are off, error
    if len(redcap_vars) != len(mysql_vars):
        error_list.append("The number of variables in the MySQL table and REDCap instrument are not equal")
        errors = True

    # If any two corresponding variables are off, error
    for i in range(len(redcap_vars)):
        temp[i] = mysql_vars[i]
        if redcap_vars[i] != mysql_vars[i]:
            error_list.append("MySQL column '" + str(mysql_vars[i]) + "' does not match REDCap variable '"
                              + str(redcap_vars[i]) + "'")
            bad_vars.append(mysql_vars[i])
            bad_vars.append(redcap_vars[i])

    print("\n----\n")

    if len(bad_vars) > 0:
        print("Errors:")
        for error in error_list:
            print(error)

        print("\nWould you like to make the following variable changes? y/n")
        for i in range(0, len(bad_vars), 2):
            print("" + bad_vars[i] + " --> " + bad_vars[i + 1] + "")
        change = ''
        while not (change == 'y' or change == 'n'):
            y = 'y'
            n = 'n'
            change = input("\nWould you like to make the variable changes? y/n\n")
            if change == 'y':
                for i in range(len(mysql_vars)):
                    if mysql_vars[i] in bad_vars:
                        temp[i] = redcap_vars[i]
                print("\n----\n")
            else:
                errors = True

    # If any errors, can't complete transfer
    if errors:
        exit(1)
    return ", ".join(temp)


def execute(curs, table, form, redcap):
    """
    :param curs: cursor for mysql database
    :param table: name of mysql table being transferred
    :param form: REDCap form being imported to
    :param redcap: REDCap project being imported to
    :return:
    """

    # Load the MySQL table that is being transferred
    query_all = "SELECT * FROM "
    query_all += str(table)
    curs.execute(query_all)

    # Load MySQL variables and REDCap variables
    variables = get_variable_order(redcap.metadata, form)
    # Make sure the columns in MySQL and REDCap match before proceeding
    mysql_column_names = check_for_errors(variables, curs.column_names)

    # List of new records, which will be individual dictionaries
    new_records = []
    print("Gathering records from '" + str(table) + "'")

    # For each row in database, get into correct format, add to list
    for line in curs:
        new_line = []
        for j in range(len(variables)):
            new_line.append(line[j])
        response = import_record(mysql_column_names, new_line)
        new_records.append(response)

    # Import records to REDCap
    print("Importing records to '" + str(form) + "'")
    print(redcap.import_records(new_records))
    print("Done")


def get_table(tables):
    table = ''
    table = input("Which table would you like to transfer?\n" + str(tables) + '\n')
    while not (table in tables):
        print("Did not find that table.")
        table = input(str(tables) + "\n")
    return table


def get_form(forms, table):
    form = ''
    form = input("Which form would like to import '" + str(table) + "' to?\n" + str(list(forms)) + "\n")
    while not (form in forms):
        print("Did not find that form.")
        form = input(str(list(forms)) + "\n")
    return form


def transfer(mysql_database_name, redcap_project_name):
    """
    mysql_database_name = mysql database
    redcap_project_name = REDCap project
    """

    curs = mysql_database_name.cursor()
    curs.execute("SHOW TABLES")

    tables = list()
    [tables.append(name) for (name,) in curs]
    if len(tables) > 1:
        num_transfers = ''
        num_transfers = input("How many tables would you like to transfer?\n")
        while not num_transfers.isdigit():
            num_transfers = input("Please enter number of transfers.\n")
        num_transfers = int(num_transfers)

        if num_transfers == 1:
            table = get_table(tables)
            form = get_form(redcap_project_name.forms, table)
            execute(curs, table, form, redcap_project_name)
        else:
            export_tables = []
            import_forms = []
            for i in range(num_transfers):
                print("\nTransfer " + str(i + 1) + ":")
                table = get_table(tables)
                export_tables.append(table)
                import_forms.append(get_form(redcap_project_name.forms, table))

            for i in range(num_transfers):
                execute(curs, export_tables[i], import_forms[i], redcap_project_name)
    else:
        execute(curs, tables[0], redcap_project_name.forms[0], redcap_project_name)

    curs.close()
