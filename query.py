#!/usr/bin/env python3

import mysql.connector

# Establishing the connection
connection = mysql.connector.connect(
    host='mysql-devel.igs.umaryland.edu',
    user='owhite',
    password='TaritRagi83',
    database='nemo_assets_devel'
)

# Creating a cursor object using the cursor() method
cursor = connection.cursor()

sql_query = """

    SELECT
       project.short_name,
       project.title,
       project.description,
       program.name,
       project.url_knowledgebase,
       project.comment,
       lab.lab_name,
       contributor.name,
       grant_info.grant_number,
       grant_info.funding_agency,
       grant_info.description_url,
       grant_info.start_date,
       grant_info.end_date,
       contributor.name
    FROM
       project
    INNER JOIN 
       program ON project.program_id = program.id
    INNER JOIN 
       grant_info ON project.id = grant_info.project_id
    INNER JOIN 
       project_has_contributor ON project.id = project_has_contributor.project_id
    INNER JOIN 
       contributor ON project_has_contributor.contrib_id = contributor.id
    INNER JOIN 
       lab ON contributor.lab_lab_id = lab.id
"""

print (sql_query1)

try:
    # Executing the SQL command
    cursor.execute(sql_query1)

    # Fetching all rows from the result set
    result = cursor.fetchall()

    # Printing each row
    for row in result:
        print(row)

except mysql.connector.Error as error:
    print("Error reading data from MySQL table:", error)

# Closing the cursor
cursor.close()

# Closing the connection
connection.close()
