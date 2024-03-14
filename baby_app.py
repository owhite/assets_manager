#!/usr/bin/env python3

import sys
import re
import math
from my_queries import pull_project_rows
from PyQt5.QtWidgets import QMainWindow, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox, QMenu, QTextEdit
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QComboBox, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout, QMessageBox
from PyQt5 import QtCore
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QFont, QFontMetrics
import mysql.connector
from functools import partial

class ThingyWindow(QMainWindow):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
        self.setGeometry(400, 100, 300, 150)

        # now perform layout of this window
        self.initUI()

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel('THINGIES!', self)
        layout.addWidget(label)

        self.list = ["List Projects", "u01_devhu"]
        for form in self.list:
            row_widget = QWidget()  # Create a new widget for each row
            row_layout = QHBoxLayout(row_widget)
            row_layout.setSpacing(0)
            row_layout.setContentsMargins(10, 5, 0, 0)

            row_layout.addWidget(QLabel(form, row_widget))

            b = QPushButton('Edit', row_widget)
            b.setFixedWidth(80)
            b.clicked.connect(lambda checked, name=form: self.button_clicked(name))
            row_layout.addWidget(b)
            layout.addWidget(row_widget)

        self.setWindowTitle('Do things')
        self.setCentralWidget(central_widget)  

    # if we're here we want to launch the instance filter window
    def button_clicked(self, name):
        if self.list.index(name) == 0:
            ## user wants a query that goes to the base class and gets all the rows similar to this spreadsheet:
            #  https://docs.google.com/spreadsheets/d/1B1w7Rw_jkkneBYINoVJj-XBC1FlRZXuI3Y400rPBHKk/edit#gid=0
            print(self.list.index(name), name)

        if self.list.index(name) == 1:
            ## user wants a query that goes to the assets modules and gets all the contributors associated with
            #    project.short_name = "u01_devhu"
            print(self.list.index(name), name)

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
        self.instances_window = ThingyWindow(conn)
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

    if True:
        login_window = LoginWindow()
        login_window.show()

    else:
        conn = manual_login()
        window = InstancesWindow(conn)
        # window.show()

        (i, t) = hack()
        w2 = InstanceEditor(conn, "HACK", i, t)
        w2.show()

    sys.exit(app.exec_())
