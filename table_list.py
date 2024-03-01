#!/usr/bin/env python3

# https://docs.google.com/spreadsheets/d/1B1w7Rw_jkkneBYINoVJj-XBC1FlRZXuI3Y400rPBHKk/edit#gid=0

import sys
import re
from my_queries import pull_project_rows
from PyQt5.QtWidgets import QMainWindow, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout, QMessageBox
import mysql.connector

# An instance is something that has several rows, each row containing fields from a set of tables. 
#   It's not the same thing as a table, it represents fields from multiple tables.
#   There can be multiple types of instances, they are in the self.instance_types list
#   This class handles making a pretty interface to filter the rows of a single instance
class InstanceFilter(QMainWindow):
    def __init__(self, conn, name, instance, parent=None):
        super(InstanceFilter, self).__init__(parent)
        self.conn = conn
        self.name = name
        self.instance = instance
        self.setWindowTitle('Tables List')
        self.setGeometry(300, 100, 2000, 300)

        # instances are associated with a bunch of tables
        # get a unique list of db tables from this instance:
        self.tables = []
        for d in self.instance:
            for row in list(d.keys()):
                if 'table' in d[row]:
                    self.tables.append(d[row]['table'])

        self.tables = list(set(self.tables) - {None})

        # now get piles of information for each sql table, 
        #  e.g., "DESCRIBE {table_name}"
        #  useful for later
        if self.conn:
            d = self.get_schema_details(self.tables)

        self.initUI()

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)


        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.layout = QVBoxLayout()
        self.central_widget.setLayout(self.layout)

        self.table_widget = QTableWidget()
        self.layout.addWidget(self.table_widget)

        self.load_button = QPushButton("Load Data")
        # self.load_button.clicked.connect(self.load_data)
        self.layout.addWidget(self.load_button)

        self.save_button = QPushButton("Save Data")
        # self.save_button.clicked.connect(self.save_data)
        self.layout.addWidget(self.save_button)

        #self.table_widget.cellClicked.connect(self.cell_clicked)  # Connect cellClicked signal to cell_clicked function

        self.setWindowTitle(f"{self.name} Form")
        self.move(100, 100)
        self.setCentralWidget(central_widget)  

    # for a list of tables make a big structure that holds the contents of each table
    #  basically ingesting info from "DESCRIBE table"
    def get_schema_details(self, tables):
        d = {}
        for table_name in tables:
            schema = self.get_table_details(table_name)
            if schema:
                d[table_name] = {}
                for field in schema:
                    d[table_name][field] = {}
                    converted_type, size = self.convert_field(field[1])
                    if converted_type.startswith('enum'):
                        d[table_name][field]['type'] = 'enum'
                        d[table_name][field]['size'] = None
                        d[table_name][field]['list'] = self.extract_enum_values(converted_type)
                    else:
                        d[table_name][field]['type'] = converted_type
                        d[table_name][field]['size'] = size
            else:
                print(f"No schema found for table '{table_name}'")
        return d

    def get_table_details(self, table_name):
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            schema = cursor.fetchall()
            cursor.close()
            return schema
        except mysql.connector.Error as e:
            print(f"Error getting table schema: {e}")
            return []

    def convert_field(self, data_type):
        # Regular expression to match 'tinyint(n)' pattern
        pattern = r'tinyint\((\d+)\)'
        match = re.search(pattern, data_type)

        if match:
            size = match.group(1)
            return 'int', size

        # Regular expression to match 'int(n)' pattern
        pattern = r'int\((\d+)\)'
        match = re.search(pattern, data_type)

        if match:
            size = match.group(1)
            return 'int', size

        # Regular expression to match 'int(n)' pattern
        pattern = r'varchar\((\d+)\)'
        match = re.search(pattern, data_type)

        if match:
            size = match.group(1)
            return 'char', size
        else:
            return data_type, None

    def extract_enum_values(self, enum_string):
        pattern = r"'(.*?)'"
        enum_values = re.findall(pattern, enum_string)
        return enum_values

# The InstancesWindow creates a list 
#   This class sets up the content of each instance, and shows the user all available instances
#   On the interface, in general instances are referred to as "forms"
class InstancesWindow(QMainWindow):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

        # these map to the list of the sheets Project, Lab, Contributor, Technique, Atrribute, IC_form
        #  which are sheets on the google spread sheet. I just made three of them so far. 
        # each form contains a list of fields, and the fields have attributes, e.g., a corresponding sql table names and table fields
        self.instance_types = {
            'Project':
            [
                {'Short_name': {'table': 'project', 'field': 'short_name', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Description': {'table': 'project', 'field': 'description', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Program': {'table': 'program', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Knowledgebase URL': {'table': 'project', 'field': 'url_knowledgebase', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Comment': {'table': 'project', 'field': 'comment', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Project type': {'table': None, 'field': None, 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, 
                {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Contributors': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Is grant?': {'table': None, 'field': None, 'optional': True, 'searchable': False, 'list': ['yes', 'no']}}, 
                {'Grant number?': {'table': 'grant_info', 'field': 'grant_number', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Funding agency': {'table': 'grant_info', 'field': 'funding_agency', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Description URL': {'table': 'grant_info', 'field': 'description_url', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Start date': {'table': 'grant_info', 'field': 'start_date', 'optional': True, 'searchable': False, 'list': None}}, 
                {'End date': {'table': 'grant_info', 'field': 'end_date', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Lead PI Contributor ID': {'table': 'contributor', 'field': 'id', 'optional': True, 'searchable': True, 'list': None}}
            ],
            'Lab':
            [
                {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': False, 'searchable': False, 'list': None}}, 
                {'Lab PI Contributor ID': {'table': 'lab', 'field': 'lab_pi_contrib_id', 'optional': True, 'searchable': False, 'list': None}}
            ],
            'Contributor':
            [
                {'Name': {'table': 'contributor', 'field': 'name', 'optional': False, 'searchable': False, 'list': None}}, 
                {'Email': {'table': 'contributor', 'field': 'email', 'optional': True, 'searchable': False, 'list': None}}, 
                {'ORCID ID': {'table': 'contributor', 'field': 'orchid_id', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Organization': {'table': 'contributor', 'field': 'organization', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Aspera Username': {'table': 'contributor', 'field': 'aspera_uname', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Lab ID': {'table': 'contributor', 'field': 'lab_lab_id', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Last name': {'table': 'contributor', 'field': 'lname', 'optional': True, 'searchable': False, 'list': None}}
            ]
        }

        # now perform layout of this window
        self.initUI()

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel('FORMS', self)
        layout.addWidget(label)

        for form in list(self.instance_types.keys()):
            row_widget = QWidget()  # Create a new widget for each row
            row_layout = QHBoxLayout(row_widget)
            row_layout.setSpacing(0)
            row_layout.setContentsMargins(10, 5, 0, 0)

            row_layout.addWidget(QLabel(form, row_widget))

            b = QPushButton('Edit', row_widget)
            b.setFixedWidth(80)
            b.clicked.connect(lambda checked, name=form: self.edit_button_clicked(name))
            row_layout.addWidget(b)
            layout.addWidget(row_widget)

        self.setWindowTitle('Tables List')
        self.move(100, 100)
        self.setCentralWidget(central_widget)  

    # if we're here we want to launch the instance filter window
    def edit_button_clicked(self, name):
        print (f"edit button {name}")
        window2 = InstanceFilter(self.conn, name, self.instance_types[name], self)
        window2.show()

class LoginWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('MySQL Login')
        self.setGeometry(100, 100, 300, 150)

        self.label_username = QLabel('Username:', self)
        self.input_username = QLineEdit(self)
        self.input_username.setText('owhite')

        self.label_password = QLabel('Password:', self)
        self.input_password = QLineEdit(self)
        self.input_password.setEchoMode(QLineEdit.Password)
        self.input_password.setText('TaritRagi83')

        self.button_login = QPushButton('Login', self)
        self.button_login.clicked.connect(self.login)

        layout = QVBoxLayout()
        layout.addWidget(self.label_username)
        layout.addWidget(self.input_username)
        layout.addWidget(self.label_password)
        layout.addWidget(self.input_password)
        layout.addWidget(self.button_login)

        self.setLayout(layout)

    def login(self):
        username = self.input_username.text()
        password = self.input_password.text()

        try:
            # Connect to the MySQL database
            conn = mysql.connector.connect(
                host='mysql-devel.igs.umaryland.edu',
                user=username,
                password=password,
                database='nemo_assets_devel'
            )
        
            if conn.is_connected():
                print("Connected to MySQL database")
                # Here you can do something with the connection, like execute queries
                self.open_instances_window(conn)
            else:
                self.show_error_message("Error", "Invalid username or password")

        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")

    def open_instances_window(self, conn):
        self.instances_window = InstancesWindow(conn)
        self.instances_window.show()
        self.close()

    def show_error_message(self, title, message):
        msg_box = QMessageBox()
        msg_box.setIcon(QMessageBox.Warning)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.exec_()


def manual_login():
    print ("skipping window-based login")
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host='mysql-devel.igs.umaryland.edu',
            user='owhite',
            password='TaritRagi83',
            database='nemo_assets_devel'
        )
        
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")

    return(conn)


if __name__ == '__main__':
    app = QApplication(sys.argv)

    if False:
        login_window = LoginWindow()
        login_window.show()
    else:
        conn = manual_login()
        window = InstancesWindow(conn)
        window.show()

    sys.exit(app.exec_())
