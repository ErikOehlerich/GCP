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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
            with open(csv_file, 'r', encoding='latin-1', errors='replace') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                rows = list(reader)
            
            if not rows:
                return results
            
            df = pd.DataFrame(rows)
            df.columns = df.columns.str.strip()
            
            filtered_df = self.apply_filters(df)
            
            if len(filtered_df) > 0:
                # Opret en kopi for at undgå SettingWithCopyWarning
                filtered_df = filtered_df.copy()
                filtered_df['_fil'] = csv_file.name
                filtered_df['_fulsti'] = str(csv_file)
                
                # SIKKER FIX: Ganger listen med rå-rækker op med længden af det filtrerede dataframe
                filtered_df['_hele_filen'] = [rows] * len(filtered_df)
                
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
        self.all_columns = []  # Dynamiske kolonner fra CSV-filer
        
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
        self.year_from.setRange(1700, 2026)
        self.year_from.setValue(1700)
        age_year_layout.addWidget(self.year_from)
        
        age_year_layout.addWidget(QLabel("til:"))
        self.year_to = QSpinBox()
        self.year_to.setRange(1700, 2026)
        self.year_to.setValue(2026)
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
        
        # Results table with search
        results_label = QLabel("Resultater (dobbeltklik for detaljer):")
        results_label.setFont(QFont("Arial", 11, QFont.Bold))
        main_layout.addWidget(results_label)
        
        # Search box for results
        search_results_layout = QHBoxLayout()
        search_results_layout.addWidget(QLabel("Søg i resultater:"))
        self.search_results = QLineEdit()
        self.search_results.setPlaceholderText("Skriv for at filtrere resultater i realtid...")
        self.search_results.textChanged.connect(self.filter_results_table)
        search_results_layout.addWidget(self.search_results)
        main_layout.addLayout(search_results_layout)
        
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(8)
        self.results_table.setHorizontalHeaderLabels([
            "Fil", "Navn", "Køn", "Alder", "Civilstand", 
            "Fødested", "Fødeår", "Stilling"
        ])
        self.results_table.setColumnWidth(1, 180)
        self.results_table.setColumnWidth(5, 150)
        # Enable sorting
        self.results_table.setSortingEnabled(True)
        # Enable column moving/rearranging
        self.results_table.horizontalHeader().setSectionsMovable(True)
        # Enable column resizing
        self.results_table.horizontalHeader().setStretchLastSection(False)
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
    

    def get_all_columns(self) -> List[str]:
        """Discover all unique columns from CSV files"""
        if not self.csv_folder:
            return []
        
        columns = set()
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            for csv_file in csv_files[:20]:  # Sample first 20 files
                try:
                    with open(csv_file, 'r', encoding='latin-1', errors='replace') as f:
                        reader = csv.DictReader(f, delimiter=';')
                        if reader.fieldnames:
                            columns.update([col.strip() for col in reader.fieldnames])
                except:
                    pass
        except:
            pass
        
        return sorted(list(columns))

    def browse_folder(self):
        """Browse for CSV folder"""
        folder = QFileDialog.getExistingDirectory(self, "Vælg mappe med CSV-filer")
        if folder:
            self.csv_folder = folder
            self.folder_label.setText(folder)
            csv_count = len(list(Path(folder).glob("**/*.csv")))
            self.statusBar.showMessage(f"Mappen indeholder {csv_count} CSV-filer")
            self.save_cache()
    
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
            cache_data = {'last_folder': self.csv_folder}
            with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Fejl ved gemning af cache: {e}")
    
    def start_search(self):
        """Start the search"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        search_params = {
            'fornavn': self.fornavn_input.text(),
            'efternavn': self.efternavn_input.text(),
            'køn': self.sex_combo.currentText(),
            'civilstand': self.status_combo.currentText(),
            'alder_fra': self.age_from.value() if self.age_from.value() > 0 else None,
            'alder_til': self.age_to.value() if self.age_to.value() < 150 else None,
            'fødeår_fra': self.year_from.value() if self.year_from.value() > 1700 else None,
            'fødeår_til': self.year_to.value() if self.year_to.value() < 2026 else None,
            'fødested': self.birthplace_input.text(),
        }
        
        self.search_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.results_table.setRowCount(0)
        self.current_results = []
        
        if self.search_thread is not None and self.search_thread.isRunning():
            self.search_worker.stop_flag = True
            self.search_thread.quit()
            self.search_thread.wait()
        
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
        self.progress_bar.setValue(value)
    
    def update_status(self, message: str):
        self.statusBar.showMessage(message)
    
    def display_results(self, results: List[Dict]):
        """Display search results in table with all columns"""
        self.current_results = results
        # Clear search filter when new results are displayed
        if hasattr(self, 'search_results'):
            self.search_results.blockSignals(True)
            self.search_results.clear()
            self.search_results.blockSignals(False)
        
        # Find all unique columns from results, filtering out complex types and metadata
        all_cols = set()
        for result in results:
            for k, v in result.items():
                # Skip internal metadata columns
                if k.startswith('_'):
                    continue
                # Skip complex data types (lists, dicts, etc) - only keep simple types
                if isinstance(v, (list, dict, tuple)):
                    continue
                # Skip if value looks like serialized data (starts with [ or { or contains lots of special chars)
                v_str = str(v).strip()
                if v_str.startswith('[') or v_str.startswith('{') or len(v_str) > 500:
                    continue
                all_cols.add(k)
        
        # Sort columns, with common ones first
        priority_cols = ['Kildenavn', 'Køn', 'Alder', 'Civilstand', 'Fødeår', 
                        'Kildefødested', 'Stilling_i_husstanden', 'Kildeerhverv']
        sorted_cols = [c for c in priority_cols if c in all_cols]
        sorted_cols += sorted([c for c in all_cols if c not in priority_cols])
        
        self.all_columns = sorted_cols
        self.results_table.setColumnCount(len(sorted_cols))
        self.results_table.setHorizontalHeaderLabels(sorted_cols)
        self.results_table.setRowCount(len(results))
        
        # Auto-resize columns
        for col_idx, col_name in enumerate(sorted_cols):
            self.results_table.setColumnWidth(col_idx, max(100, len(col_name) * 8))
        
        for row, result in enumerate(results):
            self._populate_table_row(row, result)
    
    def show_file_browser(self):
        """Show all files in the folder"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        file_window = QDialog(self)
        file_window.setWindowTitle("📁 Filoversigt")
        file_window.setGeometry(100, 100, 1000, 600)
        
        layout = QVBoxLayout()
        title = QLabel(f"<h2>CSV-filer i {self.csv_folder}</h2>")
        layout.addWidget(title)
        
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Søg i filnavn:"))
        file_search = QLineEdit()
        file_search.setPlaceholderText("fx. 'S0515' eller 'D6682'")
        search_layout.addWidget(file_search)
        layout.addLayout(search_layout)
        
        file_table = QTableWidget()
        file_table.setColumnCount(4)
        file_table.setHorizontalHeaderLabels(["Filnavn", "Sti", "Størrelse", "Status"])
        file_table.setColumnWidth(0, 200)
        file_table.setColumnWidth(1, 400)
        file_table.setColumnWidth(2, 100)
        file_table.setColumnWidth(3, 100)
        
        csv_files = sorted(list(Path(self.csv_folder).glob("**/*.csv")))
        
        def update_table(search_text=""):
            filtered_files = [f for f in csv_files if search_text.lower() in f.name.lower()] if search_text else csv_files
            file_table.setRowCount(len(filtered_files))
            
            files_with_results = set([r.get('_fil', '') for r in self.current_results])
            
            for row, file_path in enumerate(filtered_files):
                file_table.setItem(row, 0, QTableWidgetItem(file_path.name))
                file_table.setItem(row, 1, QTableWidgetItem(str(file_path)))
                
                try:
                    size_kb = file_path.stat().st_size / 1024
                    file_table.setItem(row, 2, QTableWidgetItem(f"{size_kb:.1f} KB"))
                except:
                    file_table.setItem(row, 2, QTableWidgetItem("N/A"))
                
                if file_path.name in files_with_results:
                    status_item = QTableWidgetItem("✓ Resultater")
                    status_item.setBackground(QColor("#c8e6c9"))
                    file_table.setItem(row, 3, status_item)
                else:
                    file_table.setItem(row, 3, QTableWidgetItem("Ingen"))
        
        file_search.textChanged.connect(lambda text: update_table(text))
        update_table()
        
        layout.addWidget(file_table)
        
        stats_text = f"""
        <h3>📊 Statistik:</h3>
        <ul>
            <li><b>Antal filer:</b> {len(csv_files)}</li>
            <li><b>Søgt i filer med resultater:</b> {len(set([r.get('_fil', '') for r in self.current_results]))}</li>
            <li><b>Total resultater:</b> {len(self.current_results)}</li>
        </ul>
        """
        layout.addWidget(QLabel(stats_text))
        
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
        
        detail_window = QDialog(self)
        detail_window.setWindowTitle(f"Detaljer - {person.get('Kildenavn', 'Ukendt')}")
        detail_window.setGeometry(50, 50, 1400, 800)
        
        layout = QVBoxLayout()
        tabs = QTabWidget()
        
        # Tab 1: Personlig Info
        person_scroll = QScrollArea()
        person_scroll.setWidgetResizable(True)
        person_widget = QWidget()
        person_layout = QVBoxLayout()
        
        person_layout.addWidget(QLabel(f"<h2>{person.get('Kildenavn', 'Ukendt')}</h2>"))
        
        person_html = "<table border='1' cellpadding='8' cellspacing='0' style='width:100%; background-color:#f9f9f9;'>"
        person_html += "<tr style='background-color:#4CAF50; color:white;'><th style='text-align:left;'>Kategori</th><th style='text-align:left;'>Værdi</th></tr>"
        
        # Vis alle kolonner sorteret, men filtrer komplekse datatyper
        all_keys = sorted([k for k in person.keys() if not k.startswith('_')])
        row_count = 0
        for key in all_keys:
            value = person.get(key, '')
            # Skip complex data types
            if isinstance(value, (list, dict, tuple)):
                continue
            if value is not None:
                value_str = str(value).strip()
                if value_str and value_str.lower() != 'nan':
                    display_key = key.replace('_', ' ')
                    bg_color = "#f0f0f0" if row_count % 2 == 0 else "#ffffff"
                    person_html += f"<tr style='background-color:{bg_color};'><td style='font-weight:bold; width:30%;'>{display_key}:</td><td>{value_str}</td></tr>"
                    row_count += 1
        
        person_html += "</table><br><h3>Kildeoplysninger:</h3>"
        person_html += f"<p><b>Fil:</b> {person.get('_fil', 'Ukendt')}</p>"
        person_html += f"<p><b>Sti:</b> {person.get('_fulsti', 'Ukendt')}</p>"
        
        person_label = QLabel(person_html)
        person_label.setWordWrap(True)
        person_layout.addWidget(person_label)
        person_layout.addStretch()
        
        person_widget.setLayout(person_layout)
        person_scroll.setWidget(person_widget)
        tabs.addTab(person_scroll, "Personlig Info")
        
        # Tab 2: Husstandsmedlemmer
        husstands_nr = str(person.get('Husstands/familienr.', '')).strip()
        
        if husstands_nr and husstands_nr != 'nan' and husstands_nr != '':
            household_scroll = QScrollArea()
            household_scroll.setWidgetResizable(True)
            household_widget = QWidget()
            household_layout = QVBoxLayout()
            
            hele_filen = person.get('_hele_filen', [])
            household_members = []
            
            if hele_filen:
                for member in hele_filen:
                    if str(member.get('Husstands/familienr.', '')).strip() == husstands_nr:
                        household_members.append(member)
            
            if household_members:
                household_layout.addWidget(QLabel(f"<h2>Husstanden ({len(household_members)} personer)</h2>"))
                
                household_table = QTableWidget()
                # Dynamisk kolonner til husstanden
                household_cols = set()
                for member in household_members:
                    household_cols.update([k for k in member.keys() if not k.startswith('_')])
                
                priority_household_cols = ['Kildenavn', 'Køn', 'Alder', 'Kildefødested', 'Fødeår', 'Civilstand',
                                          'Kildeerhverv', 'Stilling_i_husstanden', 'Kildestednavn', 'Født kildedato']
                sorted_household_cols = [c for c in priority_household_cols if c in household_cols]
                sorted_household_cols += sorted([c for c in household_cols if c not in priority_household_cols])
                
                household_table.setColumnCount(len(sorted_household_cols))
                household_table.setHorizontalHeaderLabels(sorted_household_cols)
                household_table.setRowCount(len(household_members))
                
                columns = sorted_household_cols
                
                for row, member in enumerate(household_members):
                    for col, field in enumerate(columns):
                        value = str(member.get(field, '')).strip()
                        item = QTableWidgetItem(value)
                        if row % 2 == 0:
                            item.setBackground(QColor("#f0f0f0"))
                        household_table.setItem(row, col, item)
                
                household_table.horizontalHeader().setStretchLastSection(True)
                household_layout.addWidget(household_table)
                
                # Fulde HTML detaljer per medlem i bunden
                household_layout.addWidget(QLabel("<h3>Detaljer om husstandsmedlemmer:</h3>"))
                details_layout = QVBoxLayout()
                
                for idx, member in enumerate(household_members):
                    member_html = f"<h4>{idx + 1}. {member.get('Kildenavn', 'Ukendt')}</h4>"
                    member_html += "<table border='1' cellpadding='5' style='width:100%;'>"
                    for key, value in sorted(member.items()):
                        # Skip internal columns and complex data types
                        if key.startswith('_'):
                            continue
                        if isinstance(value, (list, dict, tuple)):
                            continue
                        if pd.notna(value):
                            value_str = str(value).strip()
                            if value_str and value_str.lower() != 'nan':
                                member_html += f"<tr><td style='font-weight:bold; width:25%;'>{key.replace('_', ' ')}:</td><td>{value_str}</td></tr>"
                    member_html += "</table><br>"
                    lbl = QLabel(member_html)
                    lbl.setWordWrap(True)
                    details_layout.addWidget(lbl)
                
                details_widget = QWidget()
                details_widget.setLayout(details_layout)
                household_layout.addWidget(details_widget)
            else:
                household_layout.addWidget(QLabel(f"<p>Ingen andre medlemmer fundet i husstanden {husstands_nr}</p>"))
            
            household_layout.addStretch()
            household_widget.setLayout(household_layout)
            household_scroll.setWidget(household_widget)
            tabs.addTab(household_scroll, f"Husstanden ({len(household_members)} personer)")
            
        layout.addWidget(tabs)
        
        close_btn = QPushButton("Luk")
        close_btn.clicked.connect(detail_window.close)
        layout.addWidget(close_btn, 0, Qt.AlignmentFlag.AlignRight)
        
        detail_window.setLayout(layout)
        detail_window.exec_()
        
    def export_results(self, fmt: str):
        """Eksport-funktion til Excel, CSV, HTML eller PDF via pandas"""
        if not self.current_results:
            QMessageBox.warning(self, "Ingen data", "Der er ingen resultater at eksportere endnu.")
            return
            
        file_filter = f"{fmt.upper()} Filer (*.{fmt})"
        filepath, _ = QFileDialog.getSaveFileName(self, "Gem Rapport", f"folketælling_rapport.{fmt}", file_filter)
        
        if not filepath:
            return
            
        try:
            # Fjern interne metadata-kolonner inden eksport
            clean_results = []
            for r in self.current_results:
                clean_r = {k: v for k, v in r.items() if not k.startswith('_')}
                clean_r['Kilde_Filnavn'] = r.get('_fil', '')
                clean_results.append(clean_r)
                
            df = pd.DataFrame(clean_results)
            
            if fmt == 'csv':
                df.to_csv(filepath, index=False, sep=';', encoding='utf-8-sig')
            elif fmt == 'xlsx':
                df.to_excel(filepath, index=False)
            elif fmt == 'html':
                df.to_html(filepath, index=False, classes='table table-striped')
            elif fmt == 'pdf':
                # Simpel tabel-dump som HTML før konvertering eller advarsel
                QMessageBox.information(self, "PDF Eksport", "For fuld PDF-generering anbefales det at gemme som HTML og printe som PDF via din browser for bedst formatering.")
                df.to_html(filepath.replace('.pdf', '.html'), index=False)
                
            QMessageBox.information(self, "Succes", f"Rapporten blev gemt succesfuldt i: {filepath}")
        except Exception as e:
            QMessageBox.critical(self, "Eksport Fejl", f"Kunne ikke gemme filen:\n{str(e)}")

    def filter_results_table(self, search_text: str):
        """Filter results table based on search text"""
        search_text = search_text.lower().strip()
        
        if not hasattr(self, 'results_table') or not self.current_results:
            return
        
        # If search is empty, show all rows
        if not search_text:
            self.results_table.setRowCount(len(self.current_results))
            for row, result in enumerate(self.current_results):
                self._populate_table_row(row, result)
            return
        
        # Filter results that contain search text
        filtered_results = []
        for result in self.current_results:
            # Search in all visible columns
            found = False
            for value in result.values():
                if value and search_text in str(value).lower():
                    found = True
                    break
            if found:
                filtered_results.append(result)
        
        # Update table with filtered results
        self.results_table.setRowCount(len(filtered_results))
        for row, result in enumerate(filtered_results):
            self._populate_table_row(row, result)
    
    def _populate_table_row(self, row: int, result: Dict):
        """Helper method to populate a table row with result data"""
        if not hasattr(self, 'all_columns'):
            return
        
        for col_idx, col_name in enumerate(self.all_columns):
            raw_value = result.get(col_name, '')
            # Skip complex data types
            if isinstance(raw_value, (list, dict, tuple)):
                value = ''
            else:
                value = str(raw_value).strip()
                # Skip if looks like serialized data
                if value.startswith('[') or value.startswith('{'):
                    value = ''
            item = QTableWidgetItem(value)
            self.results_table.setItem(row, col_idx, item)

    def search_finished(self):
        """Called when search is finished"""
        self.search_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
    
    def closeEvent(self, event):
        if self.search_worker:
            self.search_worker.stop_flag = True
        if self.search_thread and self.search_thread.isRunning():
            self.search_thread.quit()
            self.search_thread.wait(timeout=2000)
        event.accept()
    
    def show_diagnostics(self):
        """Show diagnostics window with dataset info"""
        if not self.csv_folder:
            QMessageBox.warning(self, "Fejl", "Vælg først en mappe med CSV-filer!")
            return
        
        diag_window = QDialog(self)
        diag_window.setWindowTitle("Diagnostics & Søgetips")
        diag_window.setGeometry(100, 100, 900, 600)
        
        layout = QVBoxLayout()
        tabs = QTabWidget()
        
        # Tab 1: System status
        overview_scroll = QScrollArea()
        overview_scroll.setWidgetResizable(True)
        overview_widget = QWidget()
        overview_layout = QVBoxLayout()
        
        info_text = "<h2>📊 Dataset Oversigt & Diagnostik</h2>"
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            info_text += f"<p><b>Valgt Mappe:</b> {self.csv_folder}</p>"
            info_text += f"<p><b>Antal CSV-filer fundet:</b> {len(csv_files)}</p>"
            if csv_files:
                info_text += f"<p><b>Eksempel på filstruktur:</b> {csv_files[0].name}</p>"
        except Exception as e:
            info_text += f"<p style='color:red;'>Kunne ikke hente statistikker: {e}</p>"
            
        lbl = QLabel(info_text)
        lbl.setWordWrap(True)
        overview_layout.addWidget(lbl)
        overview_layout.addStretch()
        overview_widget.setLayout(overview_layout)
        overview_scroll.setWidget(overview_widget)
        
        tabs.addTab(overview_scroll, "System status")
        
        # Tab 2: Tilgængelige kolonner
        cols_scroll = QScrollArea()
        cols_scroll.setWidgetResizable(True)
        cols_widget = QWidget()
        cols_layout = QVBoxLayout()
        
        cols_text = "<h2>Tilgængelige kolonner i dataene</h2>"
        try:
            csv_files = list(Path(self.csv_folder).glob("**/*.csv"))
            if csv_files:
                all_columns = set()
                with open(csv_files[0], 'r', encoding='latin-1', errors='replace') as f:
                    import csv
                    reader = csv.DictReader(f, delimiter=';')
                    if reader.fieldnames:
                        all_columns.update([col.strip() for col in reader.fieldnames])
                
                if all_columns:
                    cols_text += f"<p><b>Kolonner fundet ({len(all_columns)}):</b></p><ul>"
                    for col in sorted(all_columns):
                        cols_text += f"<li>{col}</li>"
                    cols_text += "</ul>"
                else:
                    cols_text += "<p style='color:red;'>Ingen kolonner fundet</p>"
        except Exception as e:
            cols_text += f"<p style='color:red;'>Fejl: {e}</p>"
        
        cols_lbl = QLabel(cols_text)
        cols_lbl.setWordWrap(True)
        cols_layout.addWidget(cols_lbl)
        cols_layout.addStretch()
        cols_widget.setLayout(cols_layout)
        cols_scroll.setWidget(cols_widget)
        
        tabs.addTab(cols_scroll, "Kolonner")
        layout.addWidget(tabs)
        
        close_btn = QPushButton("Luk")
        close_btn.clicked.connect(diag_window.close)
        layout.addWidget(close_btn)
        
        diag_window.setLayout(layout)
        diag_window.exec_()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CsvSearcherGUI()
    window.show()
    sys.exit(app.exec())