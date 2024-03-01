#!/usr/bin/env python3

import sys
import pandas as pd
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox
from PyQt5.QtCore import Qt


class SpreadsheetForm(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Spreadsheet to Form")
        self.setGeometry(100, 100, 800, 600)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.layout = QVBoxLayout()
        self.central_widget.setLayout(self.layout)

        self.table_widget = QTableWidget()
        self.layout.addWidget(self.table_widget)

        self.load_button = QPushButton("Load Data")
        self.load_button.clicked.connect(self.load_data)
        self.layout.addWidget(self.load_button)

        self.save_button = QPushButton("Save Data")
        self.save_button.clicked.connect(self.save_data)
        self.layout.addWidget(self.save_button)

        self.table_widget.cellClicked.connect(self.cell_clicked)  # Connect cellClicked signal to cell_clicked function

    def load_data(self):
         items = ["program", "project", "project_assoc_project", "project_assoc_lab", "project_attributes", "grant_info", "project_has_contributor", "lab"]

         headers = ["Item", "ADD", "EDIT", "LINK"]
         # Set table dimensions based on data
         self.table_widget.setColumnCount(len(headers))
         self.table_widget.setHorizontalHeaderLabels(headers)

         self.table_widget.setRowCount(len(items))

         for row, item in enumerate(items):
             item_name = QTableWidgetItem(item)
             self.table_widget.setItem(row, 0, item_name)

             for col in range(1, self.table_widget.columnCount()):
                 attribute_item = QTableWidgetItem(f"{headers[col]}")
                 self.table_widget.setItem(row, col, attribute_item)

    def save_data(self):
        if not hasattr(self, 'data'):
            return

        for i in range(self.table_widget.rowCount()):
            for j in range(self.table_widget.columnCount()):
                item = self.table_widget.item(i, j)
                if item is not None:
                    self.data.iat[i, j] = item.text()

        file_path, _ = QFileDialog.getSaveFileName(self, "Save CSV File", "", "CSV Files (*.csv)")
        if not file_path:
            return

        self.data.to_csv(file_path, index=False)
        QMessageBox.information(self, "Success", "Data saved successfully.")

    def cell_clicked(self, row, column):
        item = self.table_widget.item(row, column)
        if item is not None:
            cell_value = item.text()
            print(f"Cell clicked at row {row}, column {column}. Value: {cell_value}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpreadsheetForm()
    window.show()
    sys.exit(app.exec_())
