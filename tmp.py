#!/usr/bin/env python3

import sys
from PyQt5 import QtGui, QtCore
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel
from PyQt5.QtWidgets import QMainWindow, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox
from PyQt5.QtCore import QTimer

class Second(QMainWindow):
    def __init__(self, parent=None):
        super(Second, self).__init__(parent)
        self.setWindowTitle('Tables List')
        self.setGeometry(300, 100, 2000, 300)
        self.initUI()

    def initUI(self):
        central_widget = QWidget()  # Create a central widget
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel(f'Editing', self)
        layout.addWidget(label)
        self.setCentralWidget(central_widget)  
 
 
class First(QMainWindow):
    def __init__(self, parent=None):
        super(First, self).__init__(parent)
        self.pushButton = QPushButton("click me")
 
        self.setCentralWidget(self.pushButton)
 
        self.pushButton.clicked.connect(self.on_pushButton_clicked)
        self.dialogs = list()
 
    def on_pushButton_clicked(self):
        dialog = Second(self)
        dialog.show()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = First()
    main_window.show()
    sys.exit(app.exec_())
