#!/usr/bin/env python3


import sys
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout
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

                conn.close()
                print("Connection closed")

        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = LoginWindow()
    window.show()
    sys.exit(app.exec_())
