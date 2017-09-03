import mysql.connector
import sys
import logging
logging.captureWarnings(True)

# Dictionary mapping REDCap data types to MySQL data types
mysql_field_type = {'text': 'varchar', 'notes': 'varchar', 'dropdown': 'varchar',
                    'radio': 'varchar', 'checkbox': 'varchar', 'file': '', 'calc': '',
                    'sql': '', 'descriptive': '', 'slider': 'int',
                    'yesno': 'varchar', 'truefalse': 'varchar'}

# MARK: Database Functions


# Insert a row, or multiple rows, into mysql
def insert_index(data, curs, table, values, meta, var_order):
    """
    data = database being used
    curs = cursor for database
    table = name of mysql table being added to
    values = string of values being added, separated by '///'
    meta = metadata from REDCap project
    var_order = order of variables in REDCap project of current form
    """

    # Make sure columns and values are equal length
    if len(values.split("///")) == len(var_order):
        new_values = []
        var_index = 0
        metadata_index = 0

        # Go through REDCap metadata and load into query for mysql
        while var_index < len(values.split("///")):
            # What will follow 'VALUES'
            var_type = meta[metadata_index]['field_type']

            # Go through values and change to correct mysql format
            if mysql_field_type.get(var_type) == 'varchar':
                new_str = '"' + str(values.split('///')[var_index]) + '"'
                if len(new_str) == 0:
                    new_str = 'null'
                var_index += 1

            else:
                new_str = str(values.split('///')[var_index])
                var_index += 1

            metadata_index += 1
            new_values.append(new_str)

        # Add new values back into query
        new_values = ", ".join(new_values)
        insert = ("INSERT INTO `" + table + "`(" + ", ".join(var_order) + ") VALUES(" + str(new_values) + ")")

        # Execute insert query
        try:
            curs.execute(insert)
            data.commit()
        except mysql.connector.IntegrityError as err:
            print(err)
            print(insert)
            exit(0)

    else:
        print("Values entered does not equal the number of columns")


# Insert entire database
def add_all_indices(data, curs, table, meta, records, var_order, key, meta_index):
    """
    :param data: database being used
    :param curs: cursor for database
    :param table: name of mysql table being added to
    :param meta: metadata from REDCap project
    :param records: records from REDCap project
    :param var_order: order of variables in REDCap project of current form
    :param key: primary key of REDCap project
    :param meta_index: dictionary of variables mapped to their metadata index
    :return:
    """

    # Helper function for displaying progress of transfer
    def display_progress(percent_done, by_every, symbol):
        num_spaces = 10
        if percent_done < 10:
            num_spaces = num_spaces
        elif 9 < percent_done < 99:
            num_spaces -= 1
        else:
            num_spaces -= 2
        sys.stdout.write("\r" + str(percent_done) + "%" + (" " * num_spaces) + "[" +
                         str(symbol * int(percent_done / by_every)) +
                         (" " * (int(100 / by_every) - int(percent_done / by_every))) + "]")
    # Info for displaying progress
    max_index = int(records[len(records) - 1][key])
    percentages = set(range(0, 101))

    curs.execute("SELECT * FROM " + table)

    # REDCap data types that do not translate exactly to MySQL
    exceptions = ['radio', 'checkbox', 'dropdown', 'yesno', 'truefalse']

    # Loop through each record
    for record_num in range(len(records)):
        # Turn tuple record into string
        for var in var_order:
            field_type = str(meta[meta_index.get(var)]['field_type'])

            # If field type is in the exceptions, change data slightly
            if field_type in exceptions:
                choice = records[record_num][var]

                if field_type == 'radio' or field_type == 'dropdown':
                    choices = str(meta[meta_index.get(var)]['select_choices_or_calculations']).split("|")
                    for option in choices:
                        option = option.split(",")
                        if field_type == 'radio' or field_type == 'dropdown':
                            if option[0].strip() == str(choice):
                                description = "".join(c for c in option[1:])
                                description = description.strip() + " (" + str(choice) + ")"
                                records[record_num][var] = description
                elif field_type == 'checkbox':
                    if choice == '1':
                        records[record_num][var] = "Checked (1)"
                    else:
                        records[record_num][var] = "Unchecked (0)"
                elif field_type == 'truefalse':
                    if choice == '1':
                        records[record_num][var] = "True (1)"
                    else:
                        records[record_num][var] = "False (0)"
                elif field_type == 'yesno':
                    if choice == '1':
                        records[record_num][var] = "Yes (1)"
                    else:
                        records[record_num][var] = "No (0)"

        # Turn each row of data into string to be inserted into a MySQL query
        row_info = "///".join(records[record_num][var] for var in var_order)
        # Insert index into table
        insert_index(data, curs, table, row_info, meta, var_order)

        # Show progress
        index = int(row_info.split("///")[0])
        progress = int(round(index / max_index, 2) * 100)
        if progress in percentages:
            display_progress(progress, 5, "*")
            percentages.remove(progress)


# Create a mysql table
def create_table(curs, tbl_name, variables, key, meta, meta_index):
    """
    curs = cursor for database
    tbl_name = name of table in mysql being created
    variables =  variables in REDCap project
    key = primary key of REDCap project
    meta = metadata of REDCap project
    meta_index = dictionary mapping variables to index of order
    """

    # Begin query
    table = "CREATE TABLE `" + tbl_name + "` ("

    # Go through REDCap variables and translate them to mysql
    for curr_var in variables:
        var_type = meta[meta_index[str(curr_var)]]['field_type']
        field_type = ""

        # Add to query appropriate details based on variable and type
        if str(curr_var) == key:  # If primary key
            field_type = " int(10) unsigned NOT NULL AUTO_INCREMENT,"
        else:
            # Look up MySQL data type in dictionary
            if mysql_field_type.get(var_type) == 'varchar':
                field_type = " varchar(255) DEFAULT NULL,"
            elif mysql_field_type.get(var_type) == 'int':
                field_type = " int(3) DEFAULT NULL,"

        # Add to query
        table += "`" + str(curr_var) + "`" + field_type

    # Finish query
    table += "PRIMARY KEY (`" + key + "`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
    curs.execute(table)


# MARK: API Functions

# From REDCap

# Get variables of REDCap project current form in order
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

    # Make sure list of variables for each form would have the primary id first
    if var_order[0] == 'study_id':
        return var_order
    else:
        var_order.insert(0, 'study_id')
        return var_order


# Get variables from all forms in REDCap project
def get_all_variables(meta):
    # Get forms in order
    forms = list()
    for field in meta:
        if field['form_name'] not in forms:
            forms.append(field['form_name'])

    # Go through each form, add its variables to temporary list
    temp = list()
    for form in forms:
        temp.append(get_variable_order(meta, form))

    # Temporary list will be a list of lists, so go through each and all to new list
    all_variables = list()
    for form_list in temp:
        for var in form_list:
            if var not in all_variables:
                all_variables.append(var)

    return all_variables


# Get dictionary of variables to their index
def get_metadata_index_dict(meta, var_order):
    meta_index_dict = {}

    # Because of checkboxes, variables and metadata variables may not be the same, keep track of both
    var_index = 0
    metadata_index = 0
    while var_index < len(var_order):
        field = var_order[var_index]

        # If field is checkbox, special case: go through all choices before increasing metadata index, increase variable
        #   index by the number of choices. Otherwise just assign index
        if meta[metadata_index]['field_type'] == "checkbox":
            choices = len(meta[metadata_index]['select_choices_or_calculations'].split("|"))
            for c in range(choices):
                field = var_order[var_index + c]
                meta_index_dict[field] = metadata_index
            var_index += choices
        else:
            meta_index_dict[field] = metadata_index
            var_index += 1
        metadata_index += 1

    return meta_index_dict


# MARK: Actions

def determine_forms_and_overwrite(forms, tables):
    existing = []
    for form in forms:
        if form in tables:
            existing.append(form)
    overwrite = []
    if len(existing) > 0:
        for table in existing:
            print("Table '" + str(table) + "' already exists")
            response = ''
            y = 'y'
            n = 'n'
            while not (response == 'y' or response == 'n'):
                response = str(input("Would you like to overwrite '" + str(table) + "'? y/n\n"))
            if response == 'y':
                overwrite.append(table)

    new_forms = []
    for form in forms:
        if form not in existing:
            new_forms.append(form)

    if len(new_forms) == 0 or len(overwrite) == 0:
        if len(new_forms) == 0:
            if len(overwrite) == 0:
                print("No new tables and no overwriting.")
                proceed = 'q'
            else:
                proceed = ' '
                proceed = input("\nYou would like to overwrite '" + ", ".join(overwrite) +
                                "'. Press enter to continue or 'q' to stop.\n")
        else:
            proceed = ' '
            proceed = input("\nYou would like to write '" + ", ".join(new_forms) + "'. "
                            "Press enter to continue or 'q' to stop.\n")
    else:
        proceed = ' '
        proceed = input("\nYou would like to write '" + ", ".join(new_forms) + "' and overwrite '" +
                        ", ".join(overwrite) + "'. Press enter to continue or 'q' to stop.\n")

    if proceed == 'q':
        exit(0)

    return overwrite, existing


def transfer(mysql_database_name, redcap_project_name):
    """
    :param mysql_database_name: name of mysql database being transferred to
    :param redcap_project_name: name of redcap project being transferred
    :return:
    """

    # Load database cursor, project metadata, project primary key, project records, project forms
    curs = mysql_database_name.cursor()
    meta = redcap_project_name.metadata
    key = redcap_project_name.def_field
    records = redcap_project_name.export_records()
    forms = list()
    for field in meta:
        form = field['form_name']
        if form not in forms:
            forms.append(form)

    curs.execute("SHOW TABLES")
    tables = list()
    [tables.append(name) for (name,) in curs]

    overwrite, existing = determine_forms_and_overwrite(forms, tables)

    # If only one REDCap form, only look to transfer that
    if len(redcap_project_name.forms) == 1:
        mysql_table_name = str(forms[0])
        form_variables = get_variable_order(meta, mysql_table_name)
        meta_index = get_metadata_index_dict(meta, form_variables)

        # If MySQL has table with name already, ask to overwrite
        if mysql_table_name in overwrite:
            curs.execute('drop table ' + mysql_table_name)
            # Create the table
            create_table(curs, mysql_table_name, form_variables, key, meta, meta_index)
            print("Updating table '" + mysql_table_name + "' ...")
            # Add all indices to new table
            add_all_indices(mysql_database_name, curs, mysql_table_name, meta, records,
                            form_variables, key, meta_index)
            print(" Done\n")

        # If no table exists with the name, create table and add all indices
        else:
            create_table(curs, mysql_table_name, form_variables, key, meta, meta_index)
            print("Writing table '" + mysql_table_name + "' ...")
            add_all_indices(mysql_database_name, curs, mysql_table_name, meta, records, form_variables, key, meta_index)
            print(" Done\n")

    # If multiple REDCap forms, determine which to transfer
    else:
        all_variables = get_all_variables(meta)
        meta_index = get_metadata_index_dict(meta, list(all_variables))

        for form in forms:
            form_variables = get_variable_order(meta, form)

            # For each form, see if a MySQL table already exists, if so, ask to overwrite
            if form in existing:
                if form in overwrite:
                    curs.execute("drop table " + str(form))
                    create_table(curs, form, form_variables, key, meta, meta_index)
                    print("Updating table '" + form + "' ...")
                    add_all_indices(mysql_database_name, curs, form, meta, records, form_variables, key, meta_index)
                    print(" Done\n")

            # No existing table, create table and add all indices
            else:
                create_table(curs, form, form_variables, key, meta, meta_index)
                print("Writing table '" + str(form) + "' ...")
                add_all_indices(mysql_database_name, curs, form, meta, records, form_variables, key, meta_index)
                print(" Done\n")

    print("Finished")
    curs.close()
