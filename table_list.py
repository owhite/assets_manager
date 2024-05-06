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
from nemoassets import grant, project, lab, contributor

type_to_assets_module = {
    'Project' : {"module" : project.Project(), "function" : project.Project().get_projects},
    'Grant' : {"module" : grant.Grant(), "function" : grant.Grant().get_grants},
    'Lab' : {"module" : lab.Lab(), "function" : lab.Lab().get_labs},
    'Contributor' : {"module" : contributor.Contributor(), "function" : contributor.Contributor().get_contributors}
}

# A pretty (generic) form so the user can edit a single instance
# instance contains information that is described elsewhere
# items is basically the content associated with the instance that we will edit
class InstanceEditor(QMainWindow):
    def __init__(self, conn, name, instance, items, parent=None):
        super(InstanceEditor, self).__init__(parent)
        # self.parent = parent
        self.conn = conn
        self.name = name
        self.instance = instance
        self.items = items
        self.setWindowTitle('Instance Editor')
        # self.move(self.parent.x(), self.y() + self.parent.height())

        # instances are associated with a bunch of tables
        #  get a unique list of db tables from this instance:
        self.tables = []
        for d in self.instance:
            for row in list(d.keys()):
                if 'table' in d[row]:
                    self.tables.append(d[row]['table'])

        self.tables = list(set(self.tables) - {None})

        # we have the tables, go get a pile of information for each sql table
        #  e.g., "DESCRIBE {table_name}", this has for example:
        # This creates a dictionary of the form:
        # {'project': 'is_grant': {'type': 'int', 'size': '4', 'list': None}}
        # and enables getting the type / width of fields in the database
        if self.conn:
            self.deets = self.get_schema_details(self.tables)

        # the form will have a set of labels derived from the instance
        # go through all the items our instance, and get max number of characters
        # created by "table.name"
        self.label_width = 0
        for n, i in enumerate(self.instance):
            k = next(iter(self.instance[n]))
            self.label_width = max(self.label_width, len(k))

        self.initUI()

    def initUI(self): 
        self.central_widget = QWidget() 
        self.setWindowTitle(f"{self.name} Form")
        self.setCentralWidget(self.central_widget)  
        self.layout = QHBoxLayout(self.central_widget)

        self.form_layout = QVBoxLayout()
        self.table_layout = QVBoxLayout()

        # label top of form layout
        label = QLabel(self.name)
        label.setFont(QFont("Futura", 14, QFont.Bold))
        self.form_layout.addWidget(label)

        # use for rest of the layout
        self.font = QFont("Futura", 14, QFont.Normal) 
        font_metrics = QFontMetrics(self.font)
        text_height = font_metrics.height()  # pixel height of our font
        text_width = font_metrics.width('X') # pixel width
        left_label_len = (text_width * self.label_width) + 10

        for n, item in enumerate(self.items):
            box = QHBoxLayout()
            box.setAlignment(Qt.AlignLeft) 

            name = next(iter(self.instance[n]))
            inst_spec = self.instance[n][name]

            label = SearchableLabel(name, self.font, left_label_len)
            if inst_spec['searchable']:
                label.setSearchable()
                w = int(self.deets[inst_spec['table']][inst_spec['field']]['size'])
                label.clicked.connect(partial(self.label_clicked, name, inst_spec['table'], inst_spec['field'], w * text_width))
            box.addWidget(label)

            w = self.create_entry_widget(self.deets, text_width, text_height, item, inst_spec)
            box.addWidget(w)

            self.form_layout.addLayout(box)

        box = QHBoxLayout()
        box.setAlignment(Qt.AlignRight) 
        self.add_buttons(box, self.form_layout)

        self.layout.addLayout(self.form_layout)
        self.layout.addLayout(self.table_layout)
        
        g = self.geometry()
        self.form_width = g.width() # useful for later


    # more notes about self.instance
    #  it is in the form:
    #  {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, 
    #  {'Project type': {'table': 'project', 'field': 'project_type', 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, 
    #  It has human-readable tag (e.g. 'Title) with the relevant table and field name
    #  'table' and 'field' should never be None, but it may have a 'list' or 'dict' which contains business logic
    #  This function receives inst_spec, which has 'table', 'field', 'optional', etc in self.instance
    def create_entry_widget(self, deets, text_width, text_height, item, inst_spec):

        table = inst_spec['table']
        field = inst_spec['field']
        searchable = inst_spec['searchable']
        instance_list = inst_spec['list']
        instance_dict = None
        table_list = self.deets[table][field]['list']
        if inst_spec.get('dict'):
            instance_dict = inst_spec['dict']

        # it seems strange sometimes the database knows we're working with a list and sometimes it gets
        #   specified by the instance -- need to confirm if that makes sense
        if instance_dict is not None:
            s = str(deets[table][field]['list'])
            (w, h) = self.get_size_from_string(s)
                
            combo_box = QComboBox(self)
            l = list(instance_dict.values())
            combo_box.addItems(l)
            if item in l:
                combo_box.setCurrentText(item)
            return(combo_box)
        elif instance_list is not None:
            s = str(deets[table][field]['list'])
            (w, h) = self.get_size_from_string(s)
            
            combo_box = QComboBox(self)
            combo_box.addItems(instance_list)
            if item in instance_list:
                combo_box.setCurrentText(item)
            return(combo_box)
        elif table_list is not None:
            s = str(deets[table][field]['list'])
            (w, h) = self.get_size_from_string(s)
            # print(F"LIST: {w} {h}")
                
            combo_box = QComboBox(self)
            combo_box.addItems(table_list)
            if item in table_list:
                combo_box.setCurrentText(item)
            return(combo_box)
        else:
            t = QTextEdit(self)
            size = int(deets[table][field]['size'])
            t.setText(item)

            pixel_lim = 500
            w = text_width * size
            h = (math.ceil(w / pixel_lim) * text_height) + 10
            h = min(140, h) # some field sizes are not aesthetic length
            if w < pixel_lim:
                t.setFixedSize(w, h) 
            else:
                t.setFixedSize(pixel_lim, h) 

            t.setAlignment(Qt.AlignLeft) 
            return(t)


    def label_clicked(self, name, table, field, table_width):
        print (F"Display {table} {field} for user")
        g = self.geometry()

        if hasattr(self, 'table_widget'):
            # If table_widget already exists, remove it
            self.table_layout.removeWidget(self.table_widget)
            table_width = self.table_widget.viewport().size().width()
            self.table_widget.deleteLater()
            del self.table_widget

            self.table_layout.removeWidget(self.table_button)
            self.table_button.deleteLater() 

            self.resize(g.width() - table_width, g.height())
        else:
            # table_widget does not exist, create one
            self.table_widget = QTableWidget()
            self.table_widget.setColumnCount(1)
            self.table_widget.setRowCount(100)

            col = 0
            for row in range(100):
                item = QTableWidgetItem(f"Row {row}, Col {col}")
                self.table_widget.setItem(row, col, item)

            # controls table width, did not get this to work very well
            self.table_widget.setHorizontalHeaderLabels([name])
            header = self.table_widget.horizontalHeader()
            header.setSectionResizeMode(0, header.Fixed)
            header.resizeSection(0, table_width)

            self.table_layout.addWidget(self.table_widget)
            self.table_button = QPushButton('Save')
            self.table_button.setFixedWidth(80)
            self.table_button.clicked.connect(self.save_button)
            self.table_layout.addWidget(self.table_button)

            self.resize(g.width() + table_width, g.height())


    def add_buttons(self, box, layout):
        b = QPushButton('Save')
        b.setFixedWidth(80)
        b.clicked.connect(self.save_button)
        box.addWidget(b)

        b = QPushButton('Save & Close')
        b.setFixedWidth(100)
        b.clicked.connect(self.save_and_close_button)
        box.addWidget(b)

        b = QPushButton('Close')
        b.setFixedWidth(100)
        b.clicked.connect(self.exit_button)
        box.addWidget(b)
        layout.addLayout(box)

    def save_button(self):
        print ("saving")

    def save_and_close_button(self):
        print ("saving / closing")
        reply = QMessageBox.question(self, 'Confirmation', 'Save and close?', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            print("Save...")

    def exit_button(self):
        print ("exiting")
        reply = QMessageBox.question(self, 'Confirmation', 'Exit?', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            print("Exit.")

    def get_size_from_string(self, text):
        font_metrics = QFontMetrics(self.font)
        width = font_metrics.horizontalAdvance(text)
        height = font_metrics.height()
        return(width, height)

    # send list of tables, make a big structure that holds the contents of each table
    #  basically ingesting info from "DESCRIBE table"
    def get_schema_details(self, tables):
        d = {}
        for table_name in tables:
            schema = self.get_table_details(table_name)
            if not schema:
                print(f"No schema found for table '{table_name}'")
                break
            if table_name not in d:
                d[table_name] = {}
            for result in schema:
                field = result[0]
                if field not in d[table_name]:
                    d[table_name][field] = {}
                d2 = {}
                converted_type, size = self.convert_field(result[1])
                if converted_type.startswith('enum'):
                    d2['type'] = 'enum'
                    d2['size'] = None
                    d2['list'] = self.extract_enum_values(converted_type)
                else:
                    d2['type'] = converted_type
                    d2['size'] = size
                    d2['list'] = None
                d[table_name][field] = d2
        return d

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

        # Regular expression to match 'date' pattern
        pattern = r'date'
        match = re.search(pattern, data_type)

        if match:
            return ('date', 32)

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


class SearchableLabel(QLabel):
    clicked = pyqtSignal()

    def __init__(self, text = "", font = "", label_len = "", parent=None):
        super().__init__(text, parent)
        self.setFont(font)
        self.setFixedWidth(label_len)
        self.setAlignment(Qt.AlignLeft) 

    def setSearchable(self):
        self.setMouseTracking(True)
        self.setStyleSheet("border: 1px solid black; padding: 5px;")

    def mousePressEvent(self, event):
        self.clicked.emit()

# An instance is something that has several rows, each row containing fields from a set of tables. 
#   It's not the same thing as a database table, it represents fields from multiple database tables.
#   This different instances are in the self.instance_types
#   This class handles making a pretty interface to filter the rows of a single instance
class InstanceFilter(QMainWindow):
    def __init__(self, conn, name, instance, parent=None):
        super(InstanceFilter, self).__init__(parent)
        self.parent = parent
        self.conn = conn
        self.name = name
        self.instance = instance
        self.setWindowTitle('Instance List')
        self.setGeometry(self.parent.x() + self.parent.width(), self.y(), 1200, 300)
        # print(F"INSTANCE FILTER {self.parent.x() + self.parent.width()} {self.y()}")

        # instances are associated with a bunch of tables
        # get a unique list of db tables from this instance:
        self.tables = []
        for d in self.instance:
            for row in list(d.keys()):
                if 'table' in d[row]:
                    self.tables.append(d[row]['table'])

        self.tables = list(set(self.tables) - {None})

        self.initUI()

    def initUI(self):
        self.setContextMenuPolicy(QtCore.Qt.CustomContextMenu) # helps with launching stuff with right click
        self.customContextMenuRequested.connect(self.rightclick_context_menu) # ditto

        self.central_widget = QWidget()  # Create a central widget
        self.setWindowTitle(f"{self.name} Form")
        self.setCentralWidget(self.central_widget)  

        layout = QVBoxLayout(self.central_widget)
        layout.setContentsMargins(10, 10, 0, 0)
        layout.setSpacing(0)

        label = QLabel(self.name)
        layout.addWidget(label)

        # self.instance is based on the self.instance_types InstancesWindow class 
        #  when this gets laid out, the headers will be based on that

        headers = []
        for d in self.instance:
            headers.append(list(d.keys())[0])

        # each is coming from the database, and now we have to fold in the fields
        #   that came from the database with self.instance. This has the complication
        #   that a few things from self.instance arent actually in the database
        # e.g., 'Is grant?': is just a list: ['yes', 'no']

        # print("NAME: ", self.name) 
        # Get a handle to the correct assets module
        assets_util = type_to_assets_module[self.name]["module"]
        func = type_to_assets_module[self.name]["function"]

        # print("UTIL:", assets_util, "\nFUNC", func)

        # Check if this module has any associations defined
        if getattr(assets_util, "ASSOCIATIONS", None):
            # if so, get records with all available association types
            all_associations = list(assets_util.ASSOCIATIONS.keys())
            assets_objects = func(assoc=all_associations)
            print(F"ASSOCIATIONS FOUND:\n {assets_objects}")
        else:
            print("NO ASSOCIATIONS")
            # if not, get records without associations
            assets_objects = func()

        # print("ASSOCIATIONS:\n", all_associations)
        # print("OBJECTS:\n", assets_objects)

        # project_rows = self.run_query_load_struct(pull_project_rows)

        self.table_widget = QTableWidget(len(assets_objects) + 1, len(headers))

        self.populate_table(self.table_widget, headers, assets_objects)

        # Create input line edits for filtering
        self.input_edits = [QLineEdit() for _ in range(self.table_widget.columnCount())]
        for col, edit in enumerate(self.input_edits):
            edit.textChanged.connect(lambda text, col=col: self.filter_table())
            self.table_widget.setCellWidget(0, col, edit)

        layout.addWidget(self.table_widget)

    def rightclick_context_menu(self, pos):
        menu = QMenu(self)

        # Get the selected cell from the table widget
        selected_items = self.table_widget.selectedItems()
        if selected_items:
            selected_item = selected_items[0]
            row = selected_item.row()
            col = selected_item.column()

            action1 = menu.addAction("Edit")
            # action2 = menu.addAction("Action 2") # leave these here in case we want other actions

            action1.triggered.connect(lambda: self.handle_action(row, col, "Edit"))
            # action2.triggered.connect(lambda: self.handle_action(row, col, "Action 2"))

        menu.exec_(self.mapToGlobal(pos))

    # xxx
    def handle_action(self, row, col, action_text):
        print(f"col {col}: perform {action_text} on row {row}")
        self.table_widget.clearSelection()
        # light up the whole row w/ selected = True
        #  and collect the contents in each cell
        items = []
        for i in range(self.table_widget.columnCount()):
            item = self.table_widget.item(row, i)
            if item is None:
                item = QTableWidgetItem()  
                self.table_widget.setItem(row, i, item)
            item.setSelected(True)
            items.append(item.text())

        print("YOU HAVE TO FIX WHEN ITEMS RECEIVES A LIST")
        # get ready
        edit_form = InstanceEditor(self.conn, self.name, self.instance, items, self)
        edit_form.show()
        
    def filter_table(self):
        filter_texts = [edit.text().lower() for edit in self.input_edits]
        for row in range(1, self.table_widget.rowCount()):
            row_visible = True
            for col in range(self.table_widget.columnCount()):
                item = self.table_widget.item(row, col)
                if item is not None and filter_texts[col]:
                    if filter_texts[col] not in item.text().lower():
                        row_visible = False
                        break
            self.table_widget.setRowHidden(row, not row_visible)

    def get_assoc_name(self, associations, table, field):
        for assoc_name in associations:
            # this is really bad. Clearly, the modules need to return things in a more straightforward kind of way.
            if (associations[assoc_name]["table"] == table and field in associations[assoc_name]["cols"]) \
                or ("ref_join" in associations[assoc_name] and associations[assoc_name]["ref_join"]["ref_table"] == table \
                    and associations[assoc_name]["ref_join"]["readable_field"] == field):
                return assoc_name
        return None
    
    def populate_table(self, table, headers, assets_obj_list):
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(assets_obj_list))
        
        print ("POPULATE TABLE...")
        # print(assets_obj_list)

        # note that this leaves the first row blannk
        for obj_num, assets_obj in enumerate(assets_obj_list): # rows come from the database 
            for col, item in enumerate(headers):  # columns from self.instance
                i_field = headers[col]                
                d = self.get_dict_by_key(self.instance, i_field)
                if d:
                    # if rows[row].get(d.get('table', ''), {}).get(d.get('field', '')):
                        # content = rows[row][d['table']][d['field']]
                    
                    # Check if this field's table is the same as the assets_obj.
                    # If not, the field is coming from an association (not the object directly)
                    if d['table'] != assets_obj.TABLE:

                        # if this field came from another table, then it's from an associated table.
                        # find the association that corresponds to the table in question
                        assoc_name = self.get_assoc_name(assets_obj.ASSOCIATIONS, d['table'], d['field'])
                        
                        if assoc_name:
                            content = getattr(assets_obj, assoc_name)
                            if isinstance(content, dict):
                                content = content[d['field']]
                            elif isinstance(content, list):
                                if isinstance(content[0], dict):
                                    content = [assoc[d['field']] for assoc in content]
                                if len(content) == 1:
                                    content = content[0]
                        else:
                            print(f"Couldn't find an association from {assets_obj.TABLE} to table: {d['table']}")

                    else:
                        content = getattr(assets_obj, d['field'])
                    if d.get('list'):
                        # print(F"LIST1: {d['table']} {d['field']} {d['list']} :: {content}")
                        item_name = QTableWidgetItem(str(content))
                        table.setItem(obj_num+1, col, item_name)
                    elif d.get('dict'):
                        # print(F"LIST2: {d['table']} {d['field']} {d['dict']} :: {content}")
                        if d['dict'].get(content):
                            item_name = QTableWidgetItem(str(d['dict'][content]))
                            table.setItem(obj_num+1, col, item_name)
                        else:
                            print(F"populate_table() cant meet business logic")
                    elif content:
                        # if here it means just use what is in the database
                        item_name = QTableWidgetItem(str(content))
                        table.setItem(obj_num+1, col, item_name)
                    else:
                        # if here the database field was empty, which is okay
                        # print(F"UNEXPECTED in populate_table()")
                        pass
                else:
                    print(f"unclear why there is no self.instance for {i_field}")



    def get_dict_by_key(self, dictionary_list, key):
        for dictionary in dictionary_list:
            if key in dictionary:
                return dictionary[key]
        return None 

    def run_query_load_struct(self, query):
        # different than just running a query, it also gets the column
        #   names from the query itself, then loads into the struct
        # note: this is highly dependent on each table and field being
        #   parsable from the query string
        column_names = re.findall(r"\b\w+\.\w+\b", pull_project_rows)

        struct = {}

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            for row, content in enumerate(rows):
                for col, item in enumerate(content):
                    x = column_names[col]
                    (table, field) = x.split('.')
                    if row not in struct:
                        struct[row] = {}
                    if table not in struct[row]:
                        struct[row][table] = {}
                    struct[row][table][field] = item
            return (struct)
        except mysql.connector.Error as e:
            print(f"Error getting table schema: {e}")
            return ()

# The InstancesWindow creates a list 
#   This class sets up the content of each instance, and shows the user all available instances
#   On the interface, in general instances are referred to as "forms"
class InstancesWindow(QMainWindow):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
        self.setGeometry(400, 100, 300, 150)

        # these map to the list of the sheets Project, Lab, Contributor, Technique, Atrribute, IC_form
        #  which are sheets on the google spread sheet. I just made three of them so far. 
        # each form contains a list of fields, and the fields have attributes, e.g., a corresponding sql table names and table fields
        # see: https://docs.google.com/spreadsheets/d/1B1w7Rw_jkkneBYINoVJj-XBC1FlRZXuI3Y400rPBHKk/edit#gid=0
        self.instance_types = {
            'Grant':
            [
                {'Short_name': {'table': 'project', 'field': 'short_name', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Description': {'table': 'project', 'field': 'description', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Program': {'table': 'program', 'field': 'name', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Knowledgebase URL': {'table': 'project', 'field': 'url_knowledgebase', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Comment': {'table': 'project', 'field': 'comment', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Project type': {'table': 'project', 'field': 'project_type', 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, 
                {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Contributors': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Is grant?': {'table': 'project', 'field': 'is_grant', 'optional': True, 'searchable': False, 'list': None, 'dict': {1: 'yes', 0: 'no'}}}, 
                {'Grant number?': {'table': 'grant_info', 'field': 'grant_number', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Funding agency': {'table': 'grant_info', 'field': 'funding_agency', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Description URL': {'table': 'grant_info', 'field': 'description_url', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Start date': {'table': 'grant_info', 'field': 'start_date', 'optional': True, 'searchable': False, 'list': None}}, 
                {'End date': {'table': 'grant_info', 'field': 'end_date', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Lead PI Contributor ID': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}
            ],
            'Project':
            [
                {'Short_name': {'table': 'project', 'field': 'short_name', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Description': {'table': 'project', 'field': 'description', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Program': {'table': 'program', 'field': 'name', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Knowledgebase URL': {'table': 'project', 'field': 'url_knowledgebase', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Comment': {'table': 'project', 'field': 'comment', 'optional': True, 'searchable': False, 'list': None}}, 
                {'Project type': {'table': 'project', 'field': 'project_type', 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, 
                {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Contributors': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, 
                {'Is grant?': {'table': 'project', 'field': 'is_grant', 'optional': True, 'searchable': False, 'list': None, 'dict': {1: 'yes', 0: 'no'}}}, 
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
                {'ORCID ID': {'table': 'contributor', 'field': 'orcid_id', 'optional': True, 'searchable': False, 'list': None}}, 
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

        self.setWindowTitle('Instances List')
        self.setCentralWidget(central_widget)  

    # if we're here we want to launch the instance filter window
    def edit_button_clicked(self, name):
        print(name)
        print(self.instance_types[name])
        window = InstanceFilter(self.conn, name, self.instance_types[name], self)
        window.show()

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

        print("LOGIN")

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

def hack(conn, window):
    name = "Project"
    instance = [{'Short_name': {'table': 'project', 'field': 'short_name', 'optional': True, 'searchable': False, 'list': None}}, {'Title': {'table': 'project', 'field': 'title', 'optional': True, 'searchable': False, 'list': None}}, {'Description': {'table': 'project', 'field': 'description', 'optional': True, 'searchable': False, 'list': None}}, {'Program': {'table': 'program', 'field': 'name', 'optional': True, 'searchable': False, 'list': None}}, {'Knowledgebase URL': {'table': 'project', 'field': 'url_knowledgebase', 'optional': True, 'searchable': False, 'list': None}}, {'Comment': {'table': 'project', 'field': 'comment', 'optional': True, 'searchable': False, 'list': None}}, {'Project type': {'table': 'project', 'field': 'project_type', 'optional': True, 'searchable': False, 'list': ['grant', 'study']}}, {'Lab name': {'table': 'lab', 'field': 'lab_name', 'optional': True, 'searchable': True, 'list': None}}, {'Contributors': {'table': 'contributor', 'field': 'name', 'optional': True, 'searchable': True, 'list': None}}, {'Is grant?': {'table': 'project', 'field': 'is_grant', 'optional': True, 'searchable': False, 'list': None, 'dict': {1: 'yes', 0: 'no'}}}]

    window = InstanceFilter(conn, name, instance, window)
    window.show()

if __name__ == '__main__':
    app = QApplication(sys.argv)

    if False:
        login_window = LoginWindow()
        login_window.show()

    else:
        conn = manual_login()
        window = InstancesWindow(conn)
        # window.show()
        hack(conn, window)


    sys.exit(app.exec_())
