#!/usr/bin/env python3

import sys
import re
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout, QMessageBox
import mysql.connector

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
                self.open_table_window(conn)
            else:
                self.show_error_message("Error", "Invalid username or password")

        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")

    def open_table_window(self, conn):
        self.table_window = TableWindow(conn)
        self.table_window.show()
        self.close()

    def show_error_message(self, title, message):
        msg_box = QMessageBox()
        msg_box.setIcon(QMessageBox.Warning)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.exec_()


class TableWindow(QWidget):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
        self.setWindowTitle('Tables List')
        self.setGeometry(100, 100, 300, 300)

        self.tables = [
            "program", "project", "project_assoc_project", "cohort",
            "project_assoc_lab", "project_attributes", "grant_info",
            "project_has_contributor", "lab"
        ]

        if self.conn:
            d = self.get_schema_details(self.tables)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)  # Set layout margins to zero
        layout.setSpacing(0)

        label = QLabel('TABLE OPERATIONS', self)
        layout.addWidget(label)

        for table in self.tables:
            row_widget = QWidget()  # Create a new widget for each row
            row_layout = QHBoxLayout(row_widget)
            row_layout.setSpacing(0)
            row_layout.setContentsMargins(10, 5, 0, 0)  # Set layout margins to zero

            label_table = QLabel(table, row_widget)
            row_layout.addWidget(label_table)

            b = QPushButton('Edit', row_widget)
            b.setFixedWidth(80)
            b.clicked.connect(self.null)
            row_layout.addWidget(b)

            b = QPushButton('Link', row_widget)
            b.setFixedWidth(80)
            b.clicked.connect(self.null)
            row_layout.addWidget(b)

            layout.addWidget(row_widget)

        self.setLayout(layout)

    def null(self):
        pass

    def get_table_schema(self, table_name):
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

    def get_schema_details(self, tables):
        cursor = self.conn.cursor()
        d = {}
        for table_name in tables:
            schema = self.get_table_schema(table_name)
            if schema:
                d[table_name] = {}
                for field in schema:
                    converted_type, size = self.convert_field(field[1])
                    if converted_type.startswith('enum'):
                        d[table_name]['type'] = 'enum'
                        d[table_name]['size'] = None
                        d[table_name]['list'] = self.extract_enum_values(converted_type)
                    else:
                        d[table_name]['type'] = converted_type
                        d[table_name]['size'] = size
            else:
                print(f"No schema found for table '{table_name}'")
        cursor.close()
        return d

if __name__ == '__main__':
    app = QApplication(sys.argv)
    login_window = LoginWindow()
    login_window.show()
    sys.exit(app.exec_())
