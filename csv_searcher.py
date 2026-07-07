"""
CSV Parallel Search Tool for Danish Census Data
Søger i tusindvis af CSV-filer samtidigt med multiprocessing
"""

import os
import sys
import csv
import json
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
import threading
from queue import Queue
import time

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QLabel,
    QComboBox, QSpinBox, QFileDialog, QProgressBar, QStatusBar,
    QCheckBox, QMessageBox, QTabWidget, QDialog, QScrollArea
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread
from PyQt5.QtGui import QFont, QColor


class SearchWorker(QObject):
    """Worker thread for searching CSV files"""
    progress = pyqtSignal(int)
    results = pyqtSignal(list)
    finished = pyqtSignal()
    status_update = pyqtSignal(str)
    
    def __init__(self, csv_folder: str, search_params: Dict):
        super().__init__()
        self.csv_folder = csv_folder
        self.search_params = search_params
        self.stop_flag = False
        self.executor = None
        
    def run(self):
        """Execute the search"""
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            total_files = len(csv_files)
            
            if total_files == 0:
                self.status_update.emit("Ingen CSV-filer fundet!")
                self.finished.emit()
                return
            
            self.status_update.emit(f"Søger i {total_files} filer...")
            
            all_results = []
            processed = 0
            
            # Use ThreadPoolExecutor for I/O-bound CSV reading
            self.executor = ThreadPoolExecutor(max_workers=8)
            futures = {
                self.executor.submit(self.search_file, csv_file): csv_file 
                for csv_file in csv_files
            }
            
            for future in as_completed(futures):
                if self.stop_flag:
                    break
                    
                try:
                    file_results = future.result()
                    if file_results:
                        all_results.extend(file_results)
                    
                    processed += 1
                    progress_pct = int((processed / total_files) * 100)
                    self.progress.emit(progress_pct)
                    
                    if processed % 50 == 0:
                        self.status_update.emit(
                            f"Behandlet {processed}/{total_files} filer - "
                            f"Fundet {len(all_results)} resultater"
                        )
                except Exception as e:
                    print(f"Fejl ved søgning: {e}")
            
            self.results.emit(all_results)
            self.status_update.emit(
                f"Søgning færdig! Fundet {len(all_results)} resultater"
            )
        
        except Exception as e:
            self.status_update.emit(f"Fejl: {str(e)}")
        
        finally:
            if self.executor:
                self.executor.shutdown(wait=False)
            self.finished.emit()
    
    def search_file(self, csv_file: Path) -> List[Dict]:
        """Search a single CSV file"""
        results = []
        
        try:
            # Tip: Prøv evt. 'utf-8-sig' hvis 'latin-1' giver mærkelige tegn i kolonnenavnene
            with open(csv_file, 'r', encoding='latin-1', errors='replace') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                rows = list(reader)
            
            if not rows:
                return results
            
            df = pd.DataFrame(rows)
            
            # FJERN BOM (Byte Order Mark) eller usynlige tegn fra kolonnenavne hvis de findes
            df.columns = df.columns.str.strip()
            
            filtered_df = self.apply_filters(df)
            
            if len(filtered_df) > 0:
                filtered_df['_fil'] = csv_file.name
                filtered_df['_fulsti'] = str(csv_file)
                filtered_df['_hele_filen'] = rows
                results = filtered_df.to_dict('records')
            
        except Exception as e:
            print(f"Fejl ved læsning af {csv_file.name}: {e}")
        
        return results
    
    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply search filters to dataframe safely"""
        filtered = df.copy()
        
        # Hjælpefunktion til at konvertere numeriske kolonner sikkert uden at crashe på tekst/spørgsmålstegn
        def safe_to_numeric(series):
            return pd.to_numeric(series.astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')
        
        # 1. Fornavn (Tjekker om strengen ikke er tom)
        fornavn = self.search_params.get('fornavn', '')
        if fornavn and str(fornavn).strip():
            fornavn_filter = str(fornavn).strip().lower()
            if 'Kildenavn' in filtered.columns:
                filtered = filtered[
                    filtered['Kildenavn'].astype(str).str.lower().str.contains(
                        fornavn_filter, na=False, regex=False
                    )
                ]
        
        # 2. Efternavn
        efternavn = self.search_params.get('efternavn', '')
        if efternavn and str(efternavn).strip():
            efternavn_filter = str(efternavn).strip().lower()
            if 'Kildenavn' in filtered.columns:
                filtered = filtered[
                    filtered['Kildenavn'].astype(str).str.lower().str.contains(
                        efternavn_filter, na=False, regex=False
                    )
                ]

        # 3. Køn
        køn = self.search_params.get('køn', 'Alle')
        if køn and køn != 'Alle':
            if 'Køn' in filtered.columns:
                filtered = filtered[filtered['Køn'].astype(str).str.strip() == køn]
        
        # 4. Fødeår
        if 'Fødeår' in filtered.columns:
            if self.search_params.get('fødeår_fra') is not None:
                filtered = filtered[safe_to_numeric(filtered['Fødeår']) >= self.search_params['fødeår_fra']]
            if self.search_params.get('fødeår_til') is not None:
                filtered = filtered[safe_to_numeric(filtered['Fødeår']) <= self.search_params['fødeår_til']]
        
        # 5. Alder
        if 'Alder' in filtered.columns:
            if self.search_params.get('alder_fra') is not None:
                filtered = filtered[safe_to_numeric(filtered['Alder']) >= self.search_params['alder_fra']]
            if self.search_params.get('alder_til') is not None:
                filtered = filtered[safe_to_numeric(filtered['Alder']) <= self.search_params['alder_til']]
        
        # 6. Fødested
        fødested = self.search_params.get('fødested', '')
        if fødested and str(fødested).strip():
            birthplace_filter = str(fødested).strip().lower()
            if 'Kildefødested' in filtered.columns:
                filtered = filtered[
                    filtered['Kildefødested'].astype(str).str.lower().str.contains(
                        birthplace_filter, na=False, regex=False
                    )
                ]
        
        # 7. Civilstand
        civilstand = self.search_params.get('civilstand', 'Alle')
        if civilstand and civilstand != 'Alle':
            if 'Civilstand' in filtered.columns:
                filtered = filtered[filtered['Civilstand'].astype(str).str.strip().str.lower() == civilstand.lower()]
        
        return filtered


class CsvSearcherGUI(QMainWindow):
    """Main GUI window for CSV searcher"""
    
    CACHE_FILE = ".csv_searcher_cache.json"
    
    def __init__(self):
        super().__init__()
        self.csv_folder = None
        self.search_thread = None
        self.search_worker = None
        self.current_results = []
        
        self.init_ui()
        self.load_cache()  # Load cached folder path
        self.setWindowTitle("Folketællings-søger - CSV Search Tool")
        self.setGeometry(100, 100, 1200, 700)
    
    def init_ui(self):
        """Initialize the UI"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout()
        
        # Folder selection
        folder_layout = QHBoxLayout()
        folder_layout.addWidget(QLabel("CSV Mappe:"))
        self.folder_label = QLineEdit()
        self.folder_label.setReadOnly(True)
        folder_layout.addWidget(self.folder_label)
        
        self.browse_btn = QPushButton("Vælg Mappe")
        self.browse_btn.clicked.connect(self.browse_folder)
        folder_layout.addWidget(self.browse_btn)
        
        main_layout.addLayout(folder_layout)
        
        # Search parameters
        search_layout = QVBoxLayout()
        search_layout.addWidget(QLabel("Søgeparametre:"))
        
        # Row 1: Name fields
        name_layout = QHBoxLayout()
        
        name_layout.addWidget(QLabel("Fornavn:"))
        self.fornavn_input = QLineEdit()
        self.fornavn_input.setPlaceholderText("fx. 'Jens'")
        name_layout.addWidget(self.fornavn_input)
        
        name_layout.addWidget(QLabel("Efternavn:"))
        self.efternavn_input = QLineEdit()
        self.efternavn_input.setPlaceholderText("fx. 'Hansen'")
        name_layout.addWidget(self.efternavn_input)
        
        # Sex filter
        name_layout.addWidget(QLabel("Køn:"))
        self.sex_combo = QComboBox()
        self.sex_combo.addItems(["Alle", "M", "K"])
        name_layout.addWidget(self.sex_combo)
        
        # Civil status filter
        name_layout.addWidget(QLabel("Civilstand:"))
        self.status_combo = QComboBox()
        self.status_combo.addItems(["Alle", "Gift", "Ugift", "Enkemand", "Enke"])
        name_layout.addWidget(self.status_combo)
        
        search_layout.addLayout(name_layout)
        
        # Row 2: Age and year filters
        age_year_layout = QHBoxLayout()
        
        age_year_layout.addWidget(QLabel("Alder fra:"))
        self.age_from = QSpinBox()
        self.age_from.setRange(0, 150)
        age_year_layout.addWidget(self.age_from)
        
        age_year_layout.addWidget(QLabel("til:"))
        self.age_to = QSpinBox()
        self.age_to.setRange(0, 150)
        self.age_to.setValue(150)
        age_year_layout.addWidget(self.age_to)
        
        age_year_layout.addWidget(QLabel("Fødeår fra:"))
        self.year_from = QSpinBox()
        self.year_from.setRange(1700, 2024)
        self.year_from.setValue(1700)
        age_year_layout.addWidget(self.year_from)
        
        age_year_layout.addWidget(QLabel("til:"))
        self.year_to = QSpinBox()
        self.year_to.setRange(1700, 2024)
        self.year_to.setValue(2024)
        age_year_layout.addWidget(self.year_to)
        
        search_layout.addLayout(age_year_layout)
        
        # Row 3: Birthplace filter
        birthplace_layout = QHBoxLayout()
        birthplace_layout.addWidget(QLabel("Fødested:"))
        self.birthplace_input = QLineEdit()
        self.birthplace_input.setPlaceholderText("fx. 'København' eller 'Ribe'")
        birthplace_layout.addWidget(self.birthplace_input)
        birthplace_layout.addStretch()
        
        search_layout.addLayout(birthplace_layout)
        
        main_layout.addLayout(search_layout)
        
        # Search button
        self.search_btn = QPushButton("Start Søgning")
        self.search_btn.clicked.connect(self.start_search)
        self.search_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 10px;")
        main_layout.addWidget(self.search_btn)
        
        # Debug/Info button
        debug_btn = QPushButton("ℹ️ Info & Diagnostics")
        debug_btn.clicked.connect(self.show_diagnostics)
        debug_btn.setStyleSheet("background-color: #2196F3; color: white; padding: 10px;")
        main_layout.addWidget(debug_btn)
        
        # File browser button
        file_btn = QPushButton("📁 Se Filer")
        file_btn.clicked.connect(self.show_file_browser)
        file_btn.setStyleSheet("background-color: #FF9800; color: white; padding: 10px;")
        main_layout.addWidget(file_btn)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        # Results table
        results_label = QLabel("Resultater (dobbeltklik for detaljer):")
        results_label.setFont(QFont("Arial", 11, QFont.Bold))
        main_layout.addWidget(results_label)
        
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(8)
        self.results_table.setHorizontalHeaderLabels([
            "Fil", "Navn", "Køn", "Alder", "Civilstand", 
            "Fødested", "Fødeår", "Stilling"
        ])
        self.results_table.setColumnWidth(1, 180)
        self.results_table.setColumnWidth(5, 150)
        self.results_table.doubleClicked.connect(self.show_details)
        main_layout.addWidget(self.results_table)
        
        # Export buttons
        export_layout = QHBoxLayout()
        export_layout.addWidget(QLabel("Eksporter resultater:"))
        
        export_excel_btn = QPushButton("📊 Excel")
        export_excel_btn.clicked.connect(lambda: self.export_results("xlsx"))
        export_layout.addWidget(export_excel_btn)
        
        export_csv_btn = QPushButton("📋 CSV")
        export_csv_btn.clicked.connect(lambda: self.export_results("csv"))
        export_layout.addWidget(export_csv_btn)
        
        export_pdf_btn = QPushButton("📄 PDF")
        export_pdf_btn.clicked.connect(lambda: self.export_results("pdf"))
        export_layout.addWidget(export_pdf_btn)
        
        export_html_btn = QPushButton("🌐 HTML")
        export_html_btn.clicked.connect(lambda: self.export_results("html"))
        export_layout.addWidget(export_html_btn)
        
        export_layout.addStretch()
        main_layout.addLayout(export_layout)
        
        central_widget.setLayout(main_layout)
        
        # Status bar
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("Klar til søgning...")
    
    def browse_folder(self):
        """Browse for CSV folder"""
        folder = QFileDialog.getExistingDirectory(self, "Vælg mappe med CSV-filer")
        if folder:
            self.csv_folder = folder
            self.folder_label.setText(folder)
            csv_count = len(list(Path(folder).glob("**/*.csv")))
            self.statusBar.showMessage(f"Mappen indeholder {csv_count} CSV-filer")
            self.save_cache()  # Save folder to cache
    
    def load_cache(self):
        """Load last used folder from cache"""
        try:
            if os.path.exists(self.CACHE_FILE):
                with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cached_folder = cache_data.get('last_folder', '')
                    
                    if cached_folder and os.path.exists(cached_folder):
                        self.csv_folder = cached_folder
                        self.folder_label.setText(cached_folder)
                        csv_count = len(list(Path(cached_folder).glob("**/*.csv")))
                        self.statusBar.showMessage(f"Mappen indeholder {csv_count} CSV-filer (gendannet fra cache)")
        except Exception as e:
            print(f"Fejl ved indlæsning af cache: {e}")
    
    def save_cache(self):
        """Save current folder to cache"""
        try:
            cache_data = {
                'last_folder': self.csv_folder
            }
            with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Fejl ved gemning af cache: {e}")
    
    def start_search(self):
        """Start the search"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        # Collect search parameters
        search_params = {
            'fornavn': self.fornavn_input.text(),
            'efternavn': self.efternavn_input.text(),
            'køn': self.sex_combo.currentText(),
            'civilstand': self.status_combo.currentText(),
            'alder_fra': self.age_from.value() if self.age_from.value() > 0 else None,
            'alder_til': self.age_to.value() if self.age_to.value() < 150 else None,
            'fødeår_fra': self.year_from.value() if self.year_from.value() > 1700 else None,
            'fødeår_til': self.year_to.value() if self.year_to.value() < 2024 else None,
            'fødested': self.birthplace_input.text(),
        }
        
        # Disable search button and show progress
        self.search_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.results_table.setRowCount(0)
        self.current_results = []
        
        # Kill previous thread if it exists
        if self.search_thread is not None and self.search_thread.isRunning():
            self.search_worker.stop_flag = True
            self.search_thread.quit()
            self.search_thread.wait()
        
        # Create and start worker thread
        self.search_thread = QThread()
        self.search_worker = SearchWorker(self.csv_folder, search_params)
        self.search_worker.moveToThread(self.search_thread)
        
        self.search_thread.started.connect(self.search_worker.run)
        self.search_worker.progress.connect(self.update_progress)
        self.search_worker.results.connect(self.display_results)
        self.search_worker.status_update.connect(self.update_status)
        self.search_worker.finished.connect(self.search_finished)
        
        self.search_thread.start()
    
    def update_progress(self, value: int):
        """Update progress bar"""
        self.progress_bar.setValue(value)
    
    def update_status(self, message: str):
        """Update status bar"""
        self.statusBar.showMessage(message)
    
    def display_results(self, results: List[Dict]):
        """Display search results in table"""
        self.current_results = results
        self.results_table.setRowCount(len(results))
        
        # Column mappings for display
        column_mapping = {
            0: '_fil',
            1: 'Kildenavn',
            2: 'Køn',
            3: 'Alder',
            4: 'Civilstand',
            5: 'Kildefødested',
            6: 'Fødeår',
            7: 'Stilling_i_husstanden'
        }
        
        for row, result in enumerate(results):
            for col, key in column_mapping.items():
                value = str(result.get(key, '')).strip()
                item = QTableWidgetItem(value)
                self.results_table.setItem(row, col, item)
    
    def show_file_browser(self):
        """Show all files in the folder"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        # Create file browser window
        file_window = QDialog(self)
        file_window.setWindowTitle("📁 Filoversigt")
        file_window.setGeometry(100, 100, 1000, 600)
        
        layout = QVBoxLayout()
        
        # Title
        title = QLabel(f"<h2>CSV-filer i {self.csv_folder}</h2>")
        layout.addWidget(title)
        
        # Search/filter box
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Søg i filnavn:"))
        file_search = QLineEdit()
        file_search.setPlaceholderText("fx. 'S0515' eller 'D6682'")
        search_layout.addWidget(file_search)
        layout.addLayout(search_layout)
        
        # File table
        file_table = QTableWidget()
        file_table.setColumnCount(4)
        file_table.setHorizontalHeaderLabels(["Filnavn", "Sti", "Størrelse", "Status"])
        file_table.setColumnWidth(0, 200)
        file_table.setColumnWidth(1, 400)
        file_table.setColumnWidth(2, 100)
        file_table.setColumnWidth(3, 100)
        
        # Get all files
        csv_files = sorted(list(Path(self.csv_folder).glob("**/*.csv")))
        
        def update_table(search_text=""):
            """Update file table with search filter"""
            filtered_files = []
            if search_text:
                filtered_files = [f for f in csv_files if search_text.lower() in f.name.lower()]
            else:
                filtered_files = csv_files
            
            file_table.setRowCount(len(filtered_files))
            
            for row, file_path in enumerate(filtered_files):
                # Filnavn
                name_item = QTableWidgetItem(file_path.name)
                file_table.setItem(row, 0, name_item)
                
                # Sti
                path_item = QTableWidgetItem(str(file_path))
                file_table.setItem(row, 1, path_item)
                
                # Størrelse
                try:
                    size_kb = file_path.stat().st_size / 1024
                    size_item = QTableWidgetItem(f"{size_kb:.1f} KB")
                    file_table.setItem(row, 2, size_item)
                except:
                    pass
                
                # Status (check if file had results)
                files_with_results = set([r.get('_fil', '') for r in self.current_results])
                if file_path.name in files_with_results:
                    status_item = QTableWidgetItem("✓ Resultater")
                    status_item.setBackground(QColor("#c8e6c9"))
                    file_table.setItem(row, 3, status_item)
                else:
                    status_item = QTableWidgetItem("Ingen")
                    file_table.setItem(row, 3, status_item)
        
        # Connect search
        file_search.textChanged.connect(lambda text: update_table(text))
        
        # Initial population
        update_table()
        
        layout.addWidget(file_table)
        
        # Stats
        stats_text = f"""
        <h3>📊 Statistik:</h3>
        <ul>
            <li><b>Antal filer:</b> {len(csv_files)}</li>
            <li><b>Søgt i filer med resultater:</b> {len(set([r.get('_fil', '') for r in self.current_results]))}</li>
            <li><b>Total resultater:</b> {len(self.current_results)}</li>
        </ul>
        """
        stats_label = QLabel(stats_text)
        layout.addWidget(stats_label)
        
        # Close button
        close_btn = QPushButton("Luk")
        close_btn.clicked.connect(file_window.close)
        layout.addWidget(close_btn)
        
        file_window.setLayout(layout)
        file_window.exec_()
    
    def show_details(self):
        """Show detailed view for selected person and their household"""
        current_row = self.results_table.currentRow()
        if current_row < 0 or current_row >= len(self.current_results):
            return
        
        person = self.current_results[current_row]
        
        # Create detail window
        detail_window = QDialog(self)
        detail_window.setWindowTitle(f"Detaljer - {person.get('Kildenavn', 'Ukendt')}")
        detail_window.setGeometry(50, 50, 1400, 800)
        
        layout = QVBoxLayout()
        
        # Create tab widget for person details and household
        tabs = QTabWidget()
        
        # Tab 1: Person details (all columns)
        person_scroll = QScrollArea()
        person_scroll.setWidgetResizable(True)
        person_widget = QWidget()
        person_layout = QVBoxLayout()
        
        # Person title
        person_title = QLabel(f"<h2>{person.get('Kildenavn', 'Ukendt')}</h2>")
        person_layout.addWidget(person_title)
        
        # Person info table
        person_html = "<table border='1' cellpadding='8' cellspacing='0' style='width:100%; background-color:#f9f9f9;'>"
        person_html += "<tr style='background-color:#4CAF50; color:white;'><th style='text-align:left;'>Felt</th><th style='text-align:left;'>Værdi</th></tr>"
        
        for key, value in sorted(person.items()):
            if not key.startswith('_') and pd.notna(value):
                value_str = str(value).strip()
                if value_str:
                    # Format key with better names
                    display_key = key.replace('_', ' ')
                    # Alternate row colors
                    bg_color = "#f0f0f0" if len(person_html) % 2 == 0 else "#ffffff"
                    person_html += f"<tr style='background-color:{bg_color};'><td style='font-weight:bold; width:30%;'>{display_key}:</td><td>{value_str}</td></tr>"
        
        person_html += "</table>"
        
        person_html += f"<br><h3>Kildeoplysninger:</h3>"
        person_html += f"<p><b>Fil:</b> {person.get('_fil', 'Ukendt')}</p>"
        person_html += f"<p><b>Sti:</b> {person.get('_fulsti', 'Ukendt')}</p>"
        
        person_label = QLabel(person_html)
        person_label.setWordWrap(True)
        person_layout.addWidget(person_label)
        person_layout.addStretch()
        
        person_widget.setLayout(person_layout)
        person_scroll.setWidget(person_widget)
        tabs.addTab(person_scroll, "Personlig Info")
        
        # Tab 2: Household members (if household number exists)
        husstands_nr = str(person.get('Husstands/familienr.', '')).strip()
        
        if husstands_nr and husstands_nr != 'nan' and husstands_nr != '':
            household_scroll = QScrollArea()
            household_scroll.setWidgetResizable(True)
            household_widget = QWidget()
            household_layout = QVBoxLayout()
            
            # Find all household members
            hele_filen = person.get('_hele_filen', [])
            household_members = []
            
            if hele_filen:
                for member in hele_filen:
                    if str(member.get('Husstands/familienr.', '')).strip() == husstands_nr:
                        household_members.append(member)
            
            if household_members:
                household_title = QLabel(f"<h2>Husstanden ({len(household_members)} personer)</h2>")
                household_layout.addWidget(household_title)
                
                # Create table of household members
                household_table = QTableWidget()
                household_table.setColumnCount(12)
                household_table.setHorizontalHeaderLabels([
                    "Navn", "Køn", "Alder", "Fødested", "Fødeår", "Civilstand",
                    "Erhverv", "Stilling", "Bopæl", "Født (dato)", "KIPnr", "Lbr"
                ])
                household_table.setRowCount(len(household_members))
                
                for row, member in enumerate(household_members):
                    columns = [
                        'Kildenavn', 'Køn', 'Alder', 'Kildefødested', 'Fødeår', 'Civilstand',
                        'Kildeerhverv', 'Stilling_i_husstanden', 'Kildestednavn', 'Født kildedato', 'KIPnr', 'Løbenr'
                    ]
                    
                    for col, field in enumerate(columns):
                        value = str(member.get(field, '')).strip()
                        item = QTableWidgetItem(value)
                        if row % 2 == 0:
                            item.setBackground(QColor("#f0f0f0"))
                        household_table.setItem(row, col, item)
                
                # Auto-resize columns
                household_table.horizontalHeader().setStretchLastSection(True)
                household_layout.addWidget(household_table)
                
                # All household members details
                household_layout.addWidget(QLabel("<h3>Detaljer om husstandsmedlemmer:</h3>"))
                
                details_scroll = QScrollArea()
                details_scroll.setWidgetResizable(True)
                details_widget = QWidget()
                details_layout = QVBoxLayout()
                
                for idx, member in enumerate(household_members):
                    member_html = f"<h4>{idx + 1}. {member.get('Kildenavn', 'Ukendt')}</h4>"
                    member_html += "<table border='1' cellpadding='5' style='width:100%;'>"
                    
                    for key, value in sorted(member.items()):
                        if not key.startswith('_') and pd.notna(value):
                            value_str = str(value).strip()
                            if value_str:
                                display_key = key.replace('_', ' ')
                                member_html += f"<tr><td style='font-weight:bold; width:25%;'>{display_key}:</td><td>{value_str}</td></tr>"
                    
                    member_html += "</table><br>"
                    member_label = QLabel(member_html)
                    member_label.setWordWrap(True)
                    details_layout.addWidget(member_label)
                
                details_layout.addStretch()
                details_widget.setLayout(details_layout)
                details_scroll.setWidget(details_widget)
                household_layout.addWidget(details_scroll)
                
            else:
                no_household = QLabel(f"<p>Ingen andre medlemmer fundet i husstanden {husstands_nr}</p>")
                household_layout.addWidget(no_household)
                household_layout.addStretch()
            
            household_widget.setLayout(household_layout)
            household_scroll.setWidget(household_widget)
            tabs.addTab(household_scroll, f"Husstanden ({len(household_members)} personer)")
        
        layout.addWidget(tabs)
        
        # Close button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        close_btn = QPushButton("Luk")
        close_btn.clicked.connect(detail_window.close)
        close_btn.setMinimumWidth(100)
        button_layout.addWidget(close_btn)
        
        layout.addLayout(button_layout)
        
        detail_window.setLayout(layout)
        detail_window.exec_()
    
    def search_finished(self):
        """Called when search is finished"""
        self.search_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
    
    def closeEvent(self, event):
        """Handle window close event - clean up threads"""
        # Stop any running search
        if self.search_worker:
            self.search_worker.stop_flag = True
        
        # Wait for thread to finish
        if self.search_thread and self.search_thread.isRunning():
            self.search_thread.quit()
            self.search_thread.wait(timeout=2000)
        
        event.accept()
    
    def show_diagnostics(self):
        """Show diagnostics window with dataset info"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        # Create diagnostics window
        diag_window = QDialog(self)
        diag_window.setWindowTitle("Diagnostics & Søgetips")
        diag_window.setGeometry(100, 100, 1200, 700)
        
        layout = QVBoxLayout()
        
        # Create tabs
        tabs = QTabWidget()
        
        # Tab 1: Dataset Overview
        overview_scroll = QScrollArea()
        overview_scroll.setWidgetResizable(True)
        overview_widget = QWidget()
        overview_layout = QVBoxLayout()
        
        info_text = "<h2>📊 Dataset Oversigt</h2>"
        
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            info_text += f"<p><b>Antal CSV-filer:</b> {len(csv_files)}</p>"
            
            # Sample a few files to get statistics
            total_rows = 0
            sample_data = []
            
            for csv_file in csv_files[:10]:  # Sample first 10 files
                try:
                    with open(csv_file, 'r', encoding='latin-1') as csvfile:
                        reader = csv.DictReader(csvfile, delimiter=';')
                        rows = list(reader)[:5]  # Get first 5 rows
                    
                    if rows:
                        df = pd.DataFrame(rows)
                        total_rows += len(df)
                        sample_data.append(df)
                except:
                    pass
            
            # Estimate total
            estimated_total = int((total_rows / 10) * len(csv_files)) if csv_files else 0
            info_text += f"<p><b>Estimeret antal personer:</b> ~{estimated_total:,}</p>"
            
            # Show sample names
            info_text += "<h3>📝 Eksempler på navne i datasættet:</h3>"
            info_text += "<ul style='max-height: 300px; overflow-y: auto;'>"
            
            if sample_data:
                seen_names = set()
                for df in sample_data:
                    if 'Kildenavn' in df.columns:
                        for name in df['Kildenavn'].dropna().unique()[:20]:
                            name_str = str(name).strip()
                            if name_str and name_str not in seen_names and name_str != 'nan':
                                info_text += f"<li>{name_str}</li>"
                                seen_names.add(name_str)
                                if len(seen_names) >= 30:
                                    break
                        if len(seen_names) >= 30:
                            break
            
            info_text += "</ul>"
            
        except Exception as e:
            info_text += f"<p style='color: red;'>Fejl ved læsning: {str(e)}</p>"
        
        overview_label = QLabel(info_text)
        overview_label.setWordWrap(True)
        overview_layout.addWidget(overview_label)
        overview_layout.addStretch()
        
        overview_widget.setLayout(overview_layout)
        overview_scroll.setWidget(overview_widget)
        tabs.addTab(overview_scroll, "Dataset Info")
        
        # Tab 2: Search Tips
        tips_scroll = QScrollArea()
        tips_scroll.setWidgetResizable(True)
        tips_widget = QWidget()
        tips_layout = QVBoxLayout()
        
        tips_html = """
        <h2>🔍 Søgetips & Fejlfinding</h2>
        
        <h3>❌ Ingen resultater? Prøv dette:</h3>
        
        <h4>1. Stavefejl?</h4>
        <ul>
            <li>Prøv at søge uden mellemrum: "JensHansen" → "Jens" eller "Hansen"</li>
            <li>Danske bogstaver: æ, ø, å virker - men prøv varianter</li>
            <li>Søg efter del af navn: "Hans" matcher "Hansen", "Hanson", etc.</li>
        </ul>
        
        <h4>2. Formatering?</h4>
        <ul>
            <li>Navn kan være formateret som "Fornavn Mellemnavn Efternavn"</li>
            <li>Søg efter fornavn ELLER efternavn separat</li>
            <li>Eksempel: søg "Jens" + Efternavn, eller bare "Hansen"</li>
        </ul>
        
        <h4>3. Fødeår?</h4>
        <ul>
            <li>Denne folketal er fra 1940 - personer kan være 0-150 år gamle</li>
            <li>Hvis bedstefar er 90 år i 1940 → født ca. 1850</li>
            <li>Sæt fødeår range bredt: fx 1800-1900</li>
        </ul>
        
        <h4>4. Ikke i datasættet?</h4>
        <ul>
            <li>Datasættet er fra 1940 og kan være ufuldstændigt</li>
            <li>Nogle personer kan være udeladt eller stavemig anderledes</li>
            <li>Læg mærke til at nogle filer kan være fejlformaterede</li>
        </ul>
        
        <h3>✅ Tips til succesfulsøgning:</h3>
        
        <ul>
            <li><b>Start bredt:</b> søg kun efternavn først</li>
            <li><b>Brug filtre:</b> køn, alder, fødested hjælper</li>
            <li><b>Prøv dele af navn:</b> "sen" matcher "Hansen", "Jensen", "Sørensen"</li>
            <li><b>Case-insensitive:</b> "hansen" = "HANSEN" = "Hansen"</li>
            <li><b>Se eksempler:</b> scroll liste ovenfor for inspiraiton</li>
        </ul>
        
        <h3>📋 Datakolonner:</h3>
        <ul>
            <li><b>Kildenavn:</b> Personens navn</li>
            <li><b>Køn:</b> M eller K</li>
            <li><b>Alder:</b> Alder i 1940</li>
            <li><b>Fødeår:</b> Årtal for fødsel</li>
            <li><b>Fødested:</b> By eller region</li>
            <li><b>Civilstand:</b> Gift, Ugift, etc.</li>
            <li><b>Stilling i husstanden:</b> Husfader, Husassistent, Barn, etc.</li>
        </ul>
        """
        
        tips_label = QLabel(tips_html)
        tips_label.setWordWrap(True)
        tips_layout.addWidget(tips_label)
        tips_layout.addStretch()
        
        tips_widget.setLayout(tips_layout)
        tips_scroll.setWidget(tips_widget)
        tabs.addTab(tips_scroll, "Søgetips")
        
        # Tab 3: Sample Data
        sample_scroll = QScrollArea()
        sample_scroll.setWidgetResizable(True)
        sample_widget = QWidget()
        sample_layout = QVBoxLayout()
        
        sample_html = "<h2>📄 Sample Data fra Første Fil</h2>"
        
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            if csv_files:
                first_file = csv_files[0]
                with open(first_file, 'r', encoding='latin-1') as csvfile:
                    reader = csv.DictReader(csvfile, delimiter=';')
                    rows = list(reader)[:10]
                
                if rows:
                    df = pd.DataFrame(rows)
                else:
                    df = pd.DataFrame()
                
                sample_html += f"<p><b>Fil:</b> {first_file.name}</p>"
                sample_html += f"<p><b>Antal rækker:</b> {len(df)}</p>"
                sample_html += f"<p><b>Kolonner:</b> {', '.join(df.columns.tolist())}</p>"
                sample_html += "<h3>Første 5 personer:</h3>"
                
                sample_html += df[['Kildenavn', 'Køn', 'Alder', 'Fødeår', 'Civilstand']].head(5).to_html(index=False)
        except Exception as e:
            sample_html += f"<p style='color: red;'>Fejl: {str(e)}</p>"
        
        sample_label = QLabel(sample_html)
        sample_label.setWordWrap(True)
        sample_layout.addWidget(sample_label)
        sample_layout.addStretch()
        
        sample_widget.setLayout(sample_layout)
        sample_scroll.setWidget(sample_widget)
        tabs.addTab(sample_scroll, "Sample Data")
        
        layout.addWidget(tabs)
        
        # Close button
        close_btn = QPushButton("Luk")
        close_btn.clicked.connect(diag_window.close)
        layout.addWidget(close_btn)
        
        diag_window.setLayout(layout)
        diag_window.exec_()
    
    def export_results(self, format_type: str):
        """Export results to file"""
        if not self.current_results:
            QMessageBox.warning(self, "Fejl", "Ingen resultater at eksportere!")
            return
        
        # Ask user for file location
        file_dialog = QFileDialog()
        file_dialog.setDefaultSuffix(format_type)
        
        if format_type == "xlsx":
            filename, _ = file_dialog.getSaveFileName(
                self, "Gem Excel fil", "", "Excel filer (*.xlsx)"
            )
            if filename:
                self.export_to_excel(filename)
        
        elif format_type == "csv":
            filename, _ = file_dialog.getSaveFileName(
                self, "Gem CSV fil", "", "CSV filer (*.csv)"
            )
            if filename:
                self.export_to_csv(filename)
        
        elif format_type == "pdf":
            filename, _ = file_dialog.getSaveFileName(
                self, "Gem PDF fil", "", "PDF filer (*.pdf)"
            )
            if filename:
                self.export_to_pdf(filename)
        
        elif format_type == "html":
            filename, _ = file_dialog.getSaveFileName(
                self, "Gem HTML fil", "", "HTML filer (*.html)"
            )
            if filename:
                self.export_to_html(filename)
    
    def export_to_excel(self, filename: str):
        """Export results to Excel file"""
        try:
            df = pd.DataFrame(self.current_results)
            # Remove internal columns
            df = df[[col for col in df.columns if not col.startswith('_')]]
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Resultater')
                # Auto-adjust column width
                worksheet = writer.sheets['Resultater']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            QMessageBox.information(self, "Succes", f"Resultater eksporteret til Excel:\n{filename}")
            self.statusBar.showMessage(f"Eksporteret til Excel: {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Fejl", f"Fejl ved export til Excel:\n{str(e)}")
    
    def export_to_csv(self, filename: str):
        """Export results to CSV file"""
        try:
            df = pd.DataFrame(self.current_results)
            # Remove internal columns
            df = df[[col for col in df.columns if not col.startswith('_')]]
            
            df.to_csv(filename, index=False, sep=';', encoding='utf-8')
            QMessageBox.information(self, "Succes", f"Resultater eksporteret til CSV:\n{filename}")
            self.statusBar.showMessage(f"Eksporteret til CSV: {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Fejl", f"Fejl ved export til CSV:\n{str(e)}")
    
    def export_to_pdf(self, filename: str):
        """Export results to PDF file"""
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            
            # Create PDF
            doc = SimpleDocTemplate(filename, pagesize=landscape(A4), leftMargin=10, rightMargin=10)
            elements = []
            
            # Title
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=16,
                textColor=colors.HexColor('#4CAF50'),
                spaceAfter=12,
            )
            elements.append(Paragraph(f"Søgeresultater - {len(self.current_results)} personer", title_style))
            elements.append(Spacer(1, 12))
            
            # Create table
            df = pd.DataFrame(self.current_results)
            # Remove internal columns
            df = df[[col for col in df.columns if not col.startswith('_')]]
            
            # Limit columns for PDF (too many columns breaks layout)
            key_columns = ['Kildenavn', 'Køn', 'Alder', 'Fødeår', 'Civilstand', 
                          'Kildefødested', 'KIPnr', '_fil']
            available_cols = [col for col in key_columns if col in df.columns]
            df_limited = df[available_cols]
            
            # Convert to table data
            table_data = [list(df_limited.columns)] + df_limited.values.tolist()
            
            # Create table
            table = Table(table_data, repeatRows=1)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')]),
            ]))
            
            elements.append(table)
            
            # Build PDF
            doc.build(elements)
            QMessageBox.information(self, "Succes", f"Resultater eksporteret til PDF:\n{filename}")
            self.statusBar.showMessage(f"Eksporteret til PDF: {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Fejl", f"Fejl ved export til PDF:\n{str(e)}")
    
    def export_to_html(self, filename: str):
        """Export results to HTML file"""
        try:
            df = pd.DataFrame(self.current_results)
            # Remove internal columns
            df = df[[col for col in df.columns if not col.startswith('_')]]
            
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>Søgeresultater</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    h1 { color: #4CAF50; }
                    table { border-collapse: collapse; width: 100%; }
                    th { background-color: #4CAF50; color: white; padding: 10px; text-align: left; }
                    td { border: 1px solid #ddd; padding: 8px; }
                    tr:nth-child(even) { background-color: #f0f0f0; }
                    tr:hover { background-color: #f9f9f9; }
                </style>
            </head>
            <body>
                <h1>Søgeresultater - Folketal</h1>
                <p>Total resultater: <strong>""" + str(len(self.current_results)) + """</strong></p>
            """
            
            html_content += df.to_html(index=False)
            html_content += """
            </body>
            </html>
            """
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            QMessageBox.information(self, "Succes", f"Resultater eksporteret til HTML:\n{filename}")
            self.statusBar.showMessage(f"Eksporteret til HTML: {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Fejl", f"Fejl ved export til HTML:\n{str(e)}")


def main():
    app = QApplication(sys.argv)
    window = CsvSearcherGUI()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
