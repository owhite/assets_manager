#!/usr/bin/env python3

# https://docs.google.com/spreadsheets/d/1B1w7Rw_jkkneBYINoVJj-XBC1FlRZXuI3Y400rPBHKk/edit#gid=0

import sys
import re
from my_queries import pull_project_rows
from PyQt5.QtWidgets import QMainWindow, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout, QMessageBox
import mysql.connector

class InstanceFilter(QMainWindow):
    def __init__(self, name, parent=None):
        super(InstanceFilter, self).__init__(parent)
        self.name = name
        self.setWindowTitle('Tables List')
        self.setGeometry(300, 100, 200, 300)
        self.initUI()

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel(f'Editing {self.name}', self)
        layout.addWidget(label)
        self.setCentralWidget(central_widget)  


class InstancesWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.filter_window = None  # Initialize the InstanceFilter window variable

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel('FORMS', self)
        layout.addWidget(label)

        for form in ['apples', 'pumpkins', 'peaches']:
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

    def edit_button_clicked(self, name):
        print(f"edit button {name}")
        self.filter_window = InstanceFilter(name, self)
        self.filter_window.show()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = InstancesWindow()
    window.show()
    sys.exit(app.exec_())
