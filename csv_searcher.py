"""
CSV Parallel Search Tool for Danish Census Data
Søger i tusindvis af CSV-filer samtidigt med multiprocessing
"""

import os
import sys
import csv
import json
import re
import unicodedata
import warnings
import hashlib
from difflib import SequenceMatcher
import pandas as pd
from pandas.errors import ParserWarning
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple, Set
import threading
from queue import Queue
import time

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QLabel,
    QComboBox, QSpinBox, QFileDialog, QProgressBar, QStatusBar,
    QCheckBox, QMessageBox, QTabWidget, QDialog, QScrollArea, QPlainTextEdit
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject, QThread
from PyQt5.QtGui import QFont, QColor


class SearchWorker(QObject):
    """Worker thread for searching CSV files"""
    progress = pyqtSignal(int)
    results = pyqtSignal(list)
    finished = pyqtSignal()
    status_update = pyqtSignal(str)

    COLUMN_ALIASES = {
        'Kildenavn': [
            'kildenavn', 'navn', 'personnavn', 'fuldenavn', 'name'
        ],
        'Fornavn': [
            'fornavn', 'first name', 'firstname', 'givenname', 'givennavn'
        ],
        'Efternavn': [
            'efternavn', 'surname', 'lastname', 'last name', 'familyname', 'slægtsnavn'
        ],
        'Køn': [
            'køn', 'kon', 'sex', 'gender'
        ],
        'Alder': [
            'alder', 'age', 'kildealder'
        ],
        'Civilstand': [
            'civilstand', 'civil status', 'stand', 'maritalstatus'
        ],
        'Fødeår': [
            'fødeår', 'fodear', 'fødselsår', 'fodselsar', 'birthyear'
        ],
        'Kildefødested': [
            'kildefødested', 'fødested', 'fodested', 'birthplace', 'fødestednavn'
        ],
        'Husstands/familienr.': [
            'husstandsfamilienr', 'husstandsfamilienr', 'husstandnr', 'familienr', 'husstand'
        ],
        'Stilling_i_husstanden': [
            'stillingihusstanden', 'stilling i husstanden', 'rolleihusstanden', 'role'
        ],
        'Kildeerhverv': [
            'kildeerhverv', 'erhverv', 'occupation', 'job'
        ],
        'Kildestednavn': [
            'kildestednavn', 'stednavn', 'lokation', 'location'
        ],
        'Født kildedato': [
            'fødtkildedato', 'fodtkildedato', 'fødselsdato', 'fodselsdato', 'birthdate'
        ],
    }
    
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
            df = self.load_csv_dataframe(csv_file)

            if df is None or df.empty:
                return results

            # Normaliser kolonnenavne og fjern evt. BOM i første kolonne.
            df.columns = df.columns.astype(str).str.replace('\ufeff', '', regex=False).str.strip()
            df = self.canonicalize_columns(df)
            rows = df.to_dict('records')
            
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

    def load_csv_dataframe(self, csv_file: Path) -> Optional[pd.DataFrame]:
        """Load CSV robustly and preserve correct column/value alignment."""
        encodings = ['utf-8-sig', 'latin-1', 'cp1252']
        separators = [';', ',', '\t', '|']

        best_df = None
        best_score = -1

        for encoding in encodings:
            for sep in separators:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore', ParserWarning)
                        df = pd.read_csv(
                            csv_file,
                            sep=sep,
                            dtype=str,
                            keep_default_na=False,
                            encoding=encoding,
                            engine='python',
                            index_col=False,
                            on_bad_lines='skip'
                        )

                    if df is None or df.empty:
                        continue

                    # Normaliser kolonnenavne tidligt så vi kan score på reelle nøglefelter.
                    df.columns = df.columns.astype(str).str.replace('\ufeff', '', regex=False).str.strip()
                    df = self.canonicalize_columns(df)

                    # Heuristik: foretræk parse med flest kolonner, færrest unnamed-felter,
                    # og især at nøglekolonner faktisk indeholder data.
                    col_count = len(df.columns)
                    unnamed_count = sum(str(c).lower().startswith('unnamed:') for c in df.columns)
                    score = (col_count * 10) - unnamed_count

                    key_columns = ['Kildenavn', 'Fornavn', 'Efternavn', 'Kildefødested', 'Født kildedato', 'Fødeår']
                    for key_column in key_columns:
                        if key_column in df.columns:
                            non_empty_count = int(df[key_column].fillna('').astype(str).str.strip().ne('').sum())
                            score += min(non_empty_count, 100)

                    if 'Kildenavn' in df.columns:
                        sample_names = df['Kildenavn'].fillna('').astype(str).str.strip().head(50)
                        if sample_names.ne('').any():
                            score += 500

                    if score > best_score:
                        best_score = score
                        best_df = df
                except Exception:
                    continue

        if best_df is not None:
            return best_df

        # Sidste fallback: auto-detektion i pandas.
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', ParserWarning)
                return pd.read_csv(
                    csv_file,
                    sep=None,
                    dtype=str,
                    keep_default_na=False,
                    encoding='latin-1',
                    engine='python',
                    index_col=False,
                    on_bad_lines='skip'
                )
        except Exception:
            return None

    def detect_delimiter(self, sample: str) -> str:
        """Detect CSV delimiter with sensible fallback."""
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=';,\t|')
            return dialect.delimiter
        except Exception:
            # Fald tilbage til semikolon som default for danske eksportfiler.
            return ';'

    @staticmethod
    def normalize_key(text: str) -> str:
        """Normalize text so differently formatted headers can be matched."""
        normalized = unicodedata.normalize('NFKD', str(text))
        normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.lower()
        normalized = normalized.replace('ø', 'o').replace('å', 'a').replace('æ', 'ae')
        normalized = re.sub(r'[^a-z0-9]+', '', normalized)
        return normalized

    def canonicalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename known variant headers to canonical names used by filters/UI."""
        if df.empty:
            return df

        normalized_alias_lookup = {}
        for canonical_name, aliases in self.COLUMN_ALIASES.items():
            for alias in aliases:
                normalized_alias_lookup[self.normalize_key(alias)] = canonical_name

        rename_map = {}
        used_canonical_names = set()
        for original_col in df.columns:
            normalized_col = self.normalize_key(original_col)
            matched_canonical = normalized_alias_lookup.get(normalized_col)

            if matched_canonical and matched_canonical not in used_canonical_names:
                rename_map[original_col] = matched_canonical
                used_canonical_names.add(matched_canonical)

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    @staticmethod
    def normalize_text(value: str) -> str:
        """Normalize searchable text while preserving letters for user-facing matching."""
        normalized = unicodedata.normalize('NFKC', str(value))
        normalized = ' '.join(normalized.split())
        return normalized.casefold()

    @staticmethod
    def transliterate_search_text(value: str) -> str:
        """Convert Nordic and German letters to a shared ASCII-like search form."""
        normalized = SearchWorker.normalize_text(value)
        return (
            normalized
            .replace('æ', 'ae')
            .replace('ø', 'oe')
            .replace('å', 'aa')
            .replace('ä', 'ae')
            .replace('ö', 'oe')
            .replace('ü', 'ue')
            .replace('ß', 'ss')
        )

    @staticmethod
    def flatten_search_text(value: str) -> str:
        """Convert Nordic and German letters to simple ASCII approximations."""
        transliterated = SearchWorker.transliterate_search_text(value)
        return (
            transliterated
            .replace('ae', 'a')
            .replace('oe', 'o')
            .replace('ue', 'u')
            .replace('aa', 'a')
        )

    @staticmethod
    def expand_search_variants(value: str) -> List[str]:
        """Return equivalent variants for Nordic/German letters and common digraph spellings."""
        normalized = SearchWorker.normalize_text(value)
        transliterated = SearchWorker.transliterate_search_text(value)
        flattened = SearchWorker.flatten_search_text(value)
        variants = [normalized]

        if transliterated not in variants:
            variants.append(transliterated)

        if flattened not in variants:
            variants.append(flattened)

        reverse_variant = (
            transliterated
            .replace('aa', 'å')
            .replace('ae', 'æ')
            .replace('oe', 'ø')
            .replace('ue', 'ü')
            .replace('ss', 'ß')
        )
        if reverse_variant not in variants:
            variants.append(reverse_variant)

        swedish_variant = reverse_variant.replace('æ', 'ä').replace('ø', 'ö')
        if swedish_variant not in variants:
            variants.append(swedish_variant)

        norwegian_variant = transliterated.replace('ae', 'æ').replace('oe', 'ø').replace('aa', 'å')
        if norwegian_variant not in variants:
            variants.append(norwegian_variant)

        return variants

    def build_text_contains_mask(self, series: pd.Series, search_value: str) -> pd.Series:
        """Match text robustly across Nordic/German letter variants and casing."""
        normalized_series = series.fillna('').astype(str).map(self.normalize_text)
        transliterated_series = series.fillna('').astype(str).map(self.transliterate_search_text)
        flattened_series = series.fillna('').astype(str).map(self.flatten_search_text)
        mask = pd.Series(False, index=series.index)

        series_variants = [normalized_series, transliterated_series, flattened_series]

        for query_variant in self.expand_search_variants(search_value):
            for series_variant in series_variants:
                mask = mask | series_variant.str.contains(query_variant, na=False, regex=False)

        return mask

    @staticmethod
    def extract_name_tokens(series: pd.Series) -> pd.Series:
        return series.fillna('').astype(str).map(
            lambda value: [token for token in re.split(r'\s+', SearchWorker.flatten_search_text(value)) if token]
        )

    def build_fuzzy_name_mask(self, series: pd.Series, search_value: str) -> pd.Series:
        query_tokens = [token for token in re.split(r'\s+', self.flatten_search_text(search_value)) if token]
        if not query_tokens:
            return pd.Series(False, index=series.index)

        token_series = self.extract_name_tokens(series)

        def row_matches(tokens: List[str]) -> bool:
            if not tokens:
                return False

            matched_tokens = 0
            for query_token in query_tokens:
                if len(query_token) < 4:
                    continue

                if any(
                    (len(candidate) >= 4 and query_token in candidate)
                    or (len(candidate) >= 4 and SequenceMatcher(None, query_token, candidate).ratio() >= 0.88)
                    for candidate in tokens
                ):
                    matched_tokens += 1

            required_matches = len([token for token in query_tokens if len(token) >= 4])
            return required_matches > 0 and matched_tokens == required_matches

        return token_series.map(row_matches)

    def get_name_search_series(self, df: pd.DataFrame, canonical_name: str) -> Optional[pd.Series]:
        """Return a normalized name series for a canonical name field if present."""
        if canonical_name not in df.columns:
            return None

        return df[canonical_name].fillna('').astype(str)

    def build_name_mask(self, df: pd.DataFrame, search_value: str, canonical_name: str, allow_loose_match: bool = False) -> pd.Series:
        """Build a robust mask for first-name/last-name searches."""
        mask = pd.Series(False, index=df.index)

        direct_series = self.get_name_search_series(df, canonical_name)
        if direct_series is not None:
            mask = mask | self.build_text_contains_mask(direct_series, search_value)
            if allow_loose_match:
                mask = mask | self.build_fuzzy_name_mask(direct_series, search_value)

        full_name_series = self.get_name_search_series(df, 'Kildenavn')
        if full_name_series is not None:
            mask = mask | self.build_text_contains_mask(full_name_series, search_value)
            if allow_loose_match:
                mask = mask | self.build_fuzzy_name_mask(full_name_series, search_value)

        return mask
    
    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply search filters to dataframe safely"""
        filtered = df.copy()
        
        # Hjælpefunktion til at konvertere numeriske kolonner sikkert uden at crashe på tekst/spørgsmålstegn
        def safe_to_numeric(series):
            return pd.to_numeric(series.astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')

        def birth_year_series(dataframe: pd.DataFrame) -> pd.Series:
            """Build a robust birth-year series from Fødeår and Født kildedato."""
            numeric_year = pd.Series(float('nan'), index=dataframe.index, dtype='float64')

            if 'Fødeår' in dataframe.columns:
                numeric_year = safe_to_numeric(dataframe['Fødeår'])

            if 'Født kildedato' in dataframe.columns:
                extracted_year = pd.to_numeric(
                    dataframe['Født kildedato'].astype(str).str.extract(r'((?:18|19|20)\d{2})', expand=False),
                    errors='coerce'
                )
                numeric_year = numeric_year.fillna(extracted_year)

            return numeric_year
        
        # 1. Fornavn (Tjekker om strengen ikke er tom)
        fornavn = self.search_params.get('fornavn', '')
        allow_loose_match = bool(self.search_params.get('løs_stavemåde', False))
        if fornavn and str(fornavn).strip():
            filtered = filtered[self.build_name_mask(filtered, str(fornavn).strip(), 'Fornavn', allow_loose_match)]
        
        # 2. Efternavn
        efternavn = self.search_params.get('efternavn', '')
        if efternavn and str(efternavn).strip():
            filtered = filtered[self.build_name_mask(filtered, str(efternavn).strip(), 'Efternavn', allow_loose_match)]

        # 3. Køn
        køn = self.search_params.get('køn', 'Alle')
        if køn and køn != 'Alle':
            if 'Køn' in filtered.columns:
                filtered = filtered[filtered['Køn'].astype(str).str.strip() == køn]
        
        # 4. Fødeår
        birth_year_values = birth_year_series(filtered)
        if self.search_params.get('fødeår_fra') is not None:
            filtered = filtered[birth_year_values >= self.search_params['fødeår_fra']]
            birth_year_values = birth_year_values.loc[filtered.index]
        if self.search_params.get('fødeår_til') is not None:
            filtered = filtered[birth_year_values <= self.search_params['fødeår_til']]
        
        # 5. Alder
        if 'Alder' in filtered.columns:
            if self.search_params.get('alder_fra') is not None:
                filtered = filtered[safe_to_numeric(filtered['Alder']) >= self.search_params['alder_fra']]
            if self.search_params.get('alder_til') is not None:
                filtered = filtered[safe_to_numeric(filtered['Alder']) <= self.search_params['alder_til']]
        
        # 6. Fødested
        fødested = self.search_params.get('fødested', '')
        if fødested and str(fødested).strip():
            if 'Kildefødested' in filtered.columns:
                filtered = filtered[self.build_text_contains_mask(filtered['Kildefødested'], str(fødested).strip())]
        
        # 7. Civilstand
        civilstand = self.search_params.get('civilstand', 'Alle')
        if civilstand and civilstand != 'Alle':
            if 'Civilstand' in filtered.columns:
                filtered = filtered[
                    filtered['Civilstand'].fillna('').astype(str).map(self.normalize_text) == self.normalize_text(civilstand)
                ]
        
        return filtered


class PeriodScanWorker(QObject):
    """Background worker for period-based name scan with cache and quick file prefilter."""
    progress = pyqtSignal(int)
    status_update = pyqtSignal(str)
    result_ready = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, scan_root: str, period_from: int, period_to: int, search_params: Dict, cache_file: str):
        super().__init__()
        self.scan_root = Path(scan_root)
        self.period_from = min(period_from, period_to)
        self.period_to = max(period_from, period_to)
        self.search_params = search_params
        self.cache_file = Path(cache_file)
        self.stop_flag = False

    def stop(self):
        self.stop_flag = True

    def load_cache(self) -> Dict:
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def save_cache(self, cache: Dict):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False)
        except Exception:
            pass

    def build_query_terms(self) -> Set[str]:
        terms = set()

        for query_key in ['fornavn', 'efternavn']:
            value = str(self.search_params.get(query_key, '')).strip()
            if not value:
                continue

            variants = SearchWorker.expand_search_variants(value)
            for variant in variants:
                for token in re.split(r'\s+', variant):
                    token = token.strip()
                    if len(token) >= 3:
                        terms.add(token)

        return terms

    def file_maybe_contains_terms(self, csv_file: Path, terms: Set[str], cache: Dict, terms_key: str) -> bool:
        if not terms:
            return True

        try:
            stat = csv_file.stat()
            cache_key = f"{csv_file}|{stat.st_size}|{int(stat.st_mtime_ns)}|{terms_key}"
            if cache_key in cache:
                return bool(cache[cache_key])
        except Exception:
            cache_key = f"{csv_file}|{terms_key}"
            if cache_key in cache:
                return bool(cache[cache_key])

        found = False
        try:
            with open(csv_file, 'r', encoding='latin-1', errors='ignore') as f:
                for line in f:
                    if self.stop_flag:
                        break
                    line_folded = line.casefold()
                    if any(term in line_folded for term in terms):
                        found = True
                        break
        except Exception:
            # Ved læsefejl lader vi filen gå videre til fuld parse.
            found = True

        cache[cache_key] = found
        return found

    def build_summary(self, all_results: List[Dict], scanned_count: int) -> str:
        fornavn = str(self.search_params.get('fornavn', '')).strip()
        efternavn = str(self.search_params.get('efternavn', '')).strip()

        if not all_results:
            return (
                f"Ingen træffere for '{fornavn} {efternavn}'.\n"
                f"Periode: {self.period_from}-{self.period_to}\n"
                f"Scannede filer: {scanned_count}"
            )

        unique_people = sorted({result.get('Kildenavn', '').strip() for result in all_results if result.get('Kildenavn', '').strip()})
        unique_files = sorted({result.get('_fil', '').strip() for result in all_results if result.get('_fil', '').strip()})

        period_counts = {}
        for result in all_results:
            period = result.get('_periode', 'ukendt')
            period_counts[period] = period_counts.get(period, 0) + 1

        lines = []
        lines.append(f"Søgestreng: fornavn='{fornavn}' efternavn='{efternavn}'")
        lines.append(f"Periode: {self.period_from}-{self.period_to}")
        lines.append(f"Scannede filer: {scanned_count}")
        lines.append(f"Træffere: {len(all_results)}")
        lines.append(f"Unikke personer: {len(unique_people)}")
        lines.append(f"Filer med træffere: {len(unique_files)}")
        lines.append("")
        lines.append("Træffere pr. år:")
        for year in sorted(period_counts.keys()):
            lines.append(f"- {year}: {period_counts[year]}")
        lines.append("")
        lines.append("Personer (første 50):")
        for person_name in unique_people[:50]:
            lines.append(f"- {person_name}")
        lines.append("")
        lines.append("Eksempel-træffere (første 30):")
        for result in all_results[:30]:
            lines.append(
                f"- [{result.get('_periode', '')}] {result.get('Kildenavn', 'Ukendt')} | "
                f"Født: {result.get('Født kildedato', '')} | Fødeår: {result.get('Fødeår', '')} | Fil: {result.get('_fil', '')}"
            )

        return "\n".join(lines)

    def run(self):
        try:
            if not self.scan_root.exists() or not self.scan_root.is_dir():
                self.result_ready.emit(f"Ugyldig scan-rodmappe: {self.scan_root}")
                return

            fornavn = str(self.search_params.get('fornavn', '')).strip()
            efternavn = str(self.search_params.get('efternavn', '')).strip()
            if not fornavn and not efternavn:
                self.result_ready.emit("Skriv fornavn eller efternavn i hovedvinduet før du kører periodisk navnescan.")
                return

            year_dirs = []
            for folder in self.scan_root.iterdir():
                if folder.is_dir() and re.fullmatch(r'\d{4}', folder.name):
                    year = int(folder.name)
                    if self.period_from <= year <= self.period_to:
                        year_dirs.append((year, folder))

            if not year_dirs:
                self.result_ready.emit(
                    f"Ingen årsmapper fundet i intervallet {self.period_from}-{self.period_to} under {self.scan_root}"
                )
                return

            csv_files = []
            for year, year_dir in sorted(year_dirs):
                for csv_file in year_dir.glob("**/*.csv"):
                    csv_files.append((year, csv_file))

            if not csv_files:
                self.result_ready.emit("Ingen CSV-filer fundet i valgte periode.")
                return

            terms = self.build_query_terms()
            terms_key = hashlib.sha1("|".join(sorted(terms)).encode('utf-8')).hexdigest() if terms else "no-terms"
            cache = self.load_cache()

            self.status_update.emit(
                f"Periodisk navnescan: {len(csv_files)} filer i {self.period_from}-{self.period_to}..."
            )

            search_worker = SearchWorker(str(self.scan_root), self.search_params)
            all_results = []

            for index, (year, csv_file) in enumerate(csv_files, start=1):
                if self.stop_flag:
                    self.status_update.emit("Periodisk navnescan annulleret.")
                    break

                if self.file_maybe_contains_terms(csv_file, terms, cache, terms_key):
                    file_results = search_worker.search_file(csv_file)
                    if file_results:
                        for result in file_results:
                            result['_periode'] = str(year)
                        all_results.extend(file_results)

                progress_pct = int((index / len(csv_files)) * 100)
                self.progress.emit(progress_pct)

                if index % 100 == 0:
                    self.status_update.emit(
                        f"Periodisk navnescan: {index}/{len(csv_files)} filer - {len(all_results)} træffere"
                    )

            self.save_cache(cache)
            self.result_ready.emit(self.build_summary(all_results, len(csv_files)))
            self.status_update.emit(
                f"Periodisk navnescan færdig: {len(all_results)} træffere i {self.period_from}-{self.period_to}"
            )

        except Exception as exc:
            self.result_ready.emit(f"Fejl under periodisk navnescan: {exc}")
        finally:
            self.finished.emit()


class CsvSearcherGUI(QMainWindow):
    """Main GUI window for CSV searcher"""
    
    CACHE_FILE = ".csv_searcher_cache.json"
    PERIOD_SCAN_CACHE_FILE = ".period_scan_cache.json"
    
    def __init__(self):
        super().__init__()
        self.csv_folder = None
        self.search_thread = None
        self.search_worker = None
        self.period_scan_thread = None
        self.period_scan_worker = None
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
        self.loose_spelling_check = QCheckBox("Løs stavemåde")
        self.loose_spelling_check.setChecked(True)
        self.loose_spelling_check.setToolTip("Finder også nært beslægtede stavemåder af navn og sted")
        birthplace_layout.addWidget(self.loose_spelling_check)
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
            'løs_stavemåde': self.loose_spelling_check.isChecked(),
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
        
        # Find all unique columns from results, excluding internal metadata.
        # Kolonnevalg må ikke afhænge af celleværdi-længde, da det kan skjule alle kolonner.
        all_cols = set()
        for result in results:
            for k in result.keys():
                # Skip internal metadata columns
                if k.startswith('_'):
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
        search_text = search_text.strip()
        
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
        search_variants = SearchWorker.expand_search_variants(search_text)
        for result in self.current_results:
            # Search in all visible columns
            found = False
            for value in result.values():
                if not value:
                    continue

                normalized_value = SearchWorker.normalize_text(str(value))
                transliterated_value = SearchWorker.transliterate_search_text(str(value))
                flattened_value = SearchWorker.flatten_search_text(str(value))

                for variant in search_variants:
                    if (
                        variant in normalized_value
                        or variant in transliterated_value
                        or variant in flattened_value
                    ):
                        found = True
                        break

                if found:
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

    def get_period_root_and_years(self) -> Tuple[Path, List[int]]:
        """Find the closest ancestor folder that contains year folders like 1901, 1930, 1940."""
        if not self.csv_folder:
            return Path('.'), []

        current = Path(self.csv_folder)
        for ancestor in [current] + list(current.parents):
            try:
                years = sorted(
                    int(folder.name)
                    for folder in ancestor.iterdir()
                    if folder.is_dir() and re.fullmatch(r'\d{4}', folder.name)
                )
                if years:
                    return ancestor, years
            except Exception:
                continue

        return current, []

    def start_period_name_scan(self, scan_root: Path, period_from: int, period_to: int,
                               output_box: QPlainTextEdit, progress_bar: QProgressBar,
                               run_button: QPushButton, cancel_button: QPushButton):
        """Start asynchronous period scan so UI stays responsive."""
        if self.period_scan_thread and self.period_scan_thread.isRunning():
            QMessageBox.information(self, "Kører allerede", "Der kører allerede en periode-scan.")
            return

        search_params = {
            'fornavn': self.fornavn_input.text().strip(),
            'efternavn': self.efternavn_input.text().strip(),
            'køn': 'Alle',
            'civilstand': 'Alle',
            'alder_fra': None,
            'alder_til': None,
            'fødeår_fra': None,
            'fødeår_til': None,
            'fødested': '',
            'løs_stavemåde': self.loose_spelling_check.isChecked(),
        }

        output_box.setPlainText("Starter periodisk navnescan...")
        progress_bar.setValue(0)
        progress_bar.setVisible(True)
        run_button.setEnabled(False)
        cancel_button.setEnabled(True)

        self.period_scan_thread = QThread(self)
        self.period_scan_worker = PeriodScanWorker(
            str(scan_root),
            period_from,
            period_to,
            search_params,
            str(Path(self.PERIOD_SCAN_CACHE_FILE).resolve()),
        )
        self.period_scan_worker.moveToThread(self.period_scan_thread)

        def safe_set_enabled(widget, value: bool):
            try:
                widget.setEnabled(value)
            except RuntimeError:
                pass

        def safe_set_visible(widget, value: bool):
            try:
                widget.setVisible(value)
            except RuntimeError:
                pass

        def safe_set_value(widget, value: int):
            try:
                widget.setValue(value)
            except RuntimeError:
                pass

        def safe_set_text(widget, text: str):
            try:
                widget.setPlainText(text)
            except RuntimeError:
                pass

        self.period_scan_thread.started.connect(self.period_scan_worker.run)
        self.period_scan_worker.progress.connect(lambda value: safe_set_value(progress_bar, value))
        self.period_scan_worker.status_update.connect(self.statusBar.showMessage)
        self.period_scan_worker.result_ready.connect(lambda text: safe_set_text(output_box, text))

        def on_period_worker_finished():
            safe_set_enabled(run_button, True)
            safe_set_enabled(cancel_button, False)
            safe_set_visible(progress_bar, False)

            if self.period_scan_thread and self.period_scan_thread.isRunning():
                self.period_scan_thread.quit()

        def on_period_thread_finished():
            self.period_scan_thread = None
            self.period_scan_worker = None

        self.period_scan_worker.finished.connect(on_period_worker_finished)
        self.period_scan_thread.finished.connect(on_period_thread_finished)
        self.period_scan_thread.start()

    def cancel_period_name_scan(self):
        if self.period_scan_worker:
            self.period_scan_worker.stop()

    def search_finished(self):
        """Called when search is finished"""
        self.search_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
    
    def closeEvent(self, event):
        if self.search_worker:
            self.search_worker.stop_flag = True
        if self.search_thread and self.search_thread.isRunning():
            self.search_thread.quit()
            self.search_thread.wait(2000)
        if self.period_scan_worker:
            self.period_scan_worker.stop()
        if self.period_scan_thread and self.period_scan_thread.isRunning():
            self.period_scan_thread.quit()
            self.period_scan_thread.wait(2000)
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

        # Tab 3: Aktuel søgning
        search_scroll = QScrollArea()
        search_scroll.setWidgetResizable(True)
        search_widget = QWidget()
        search_layout = QVBoxLayout()

        current_name_values = [
            value for value in [self.fornavn_input.text().strip(), self.efternavn_input.text().strip()] if value
        ]
        search_title = " ".join(current_name_values) if current_name_values else "Aktuel søgning"
        summary_text = f"<h2>Diagnostik for: {search_title}</h2>"
        summary_text += f"<p><b>Løs stavemåde:</b> {'Ja' if self.loose_spelling_check.isChecked() else 'Nej'}</p>"
        summary_text += f"<p><b>Resultater i hukommelsen:</b> {len(self.current_results)}</p>"

        if self.current_results:
            unique_files = sorted({result.get('_fil', '') for result in self.current_results if result.get('_fil', '')})
            unique_names = sorted({result.get('Kildenavn', '') for result in self.current_results if result.get('Kildenavn', '')})
            year_values = sorted({str(result.get('Fødeår', '')).strip() for result in self.current_results if str(result.get('Fødeår', '')).strip()})

            summary_text += f"<p><b>Unikke personer:</b> {len(unique_names)}</p>"
            summary_text += f"<p><b>Filer med træffere:</b> {len(unique_files)}</p>"
            if year_values:
                summary_text += f"<p><b>Fødeår fundet:</b> {', '.join(year_values[:20])}</p>"

            summary_text += "<h3>Første træffere</h3><ul>"
            for name in unique_names[:15]:
                summary_text += f"<li>{name}</li>"
            summary_text += "</ul>"

            summary_text += "<h3>Filer med træffere</h3><ul>"
            for file_name in unique_files[:15]:
                summary_text += f"<li>{file_name}</li>"
            summary_text += "</ul>"
        else:
            summary_text += "<p>Ingen aktuelle resultater. Kør en søgning først for at få navne-, fil- og årsdiagnostik her.</p>"

        search_label = QLabel(summary_text)
        search_label.setWordWrap(True)
        search_layout.addWidget(search_label)
        search_layout.addStretch()
        search_widget.setLayout(search_layout)
        search_scroll.setWidget(search_widget)
        tabs.addTab(search_scroll, "Aktuel søgning")

        # Tab 4: Periodisk navnescan
        period_scroll = QScrollArea()
        period_scroll.setWidgetResizable(True)
        period_widget = QWidget()
        period_layout = QVBoxLayout()

        root_folder, available_years = self.get_period_root_and_years()

        period_layout.addWidget(QLabel("<h2>Periodisk navnescan</h2>"))
        period_layout.addWidget(QLabel("Vælg tidsperiode for hvilke årsmappper der skal gennemsøges."))

        root_layout = QHBoxLayout()
        root_layout.addWidget(QLabel("Rodmappe:"))
        root_input = QLineEdit(str(root_folder))
        root_layout.addWidget(root_input)
        choose_root_btn = QPushButton("Vælg")

        def choose_root_folder():
            chosen = QFileDialog.getExistingDirectory(diag_window, "Vælg rodmappe med årsmapper")
            if chosen:
                root_input.setText(chosen)

        choose_root_btn.clicked.connect(choose_root_folder)
        root_layout.addWidget(choose_root_btn)
        period_layout.addLayout(root_layout)

        years_layout = QHBoxLayout()
        years_layout.addWidget(QLabel("Periode fra:"))
        period_from_input = QSpinBox()
        period_from_input.setRange(1700, 2100)
        years_layout.addWidget(period_from_input)
        years_layout.addWidget(QLabel("til:"))
        period_to_input = QSpinBox()
        period_to_input.setRange(1700, 2100)
        years_layout.addWidget(period_to_input)

        if available_years:
            period_from_input.setValue(min(available_years))
            period_to_input.setValue(max(available_years))
        else:
            period_from_input.setValue(1901)
            period_to_input.setValue(1940)

        period_layout.addLayout(years_layout)

        run_period_scan_btn = QPushButton("Kør navnescan i valgt periode")
        cancel_period_scan_btn = QPushButton("Annuller scan")
        cancel_period_scan_btn.setEnabled(False)

        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(run_period_scan_btn)
        buttons_layout.addWidget(cancel_period_scan_btn)
        buttons_layout.addStretch()
        period_layout.addLayout(buttons_layout)

        period_progress = QProgressBar()
        period_progress.setVisible(False)
        period_layout.addWidget(period_progress)

        period_output = QPlainTextEdit()
        period_output.setReadOnly(True)
        period_output.setPlaceholderText("Resultat af periodisk navnescan vises her...")
        period_layout.addWidget(period_output)

        def run_selected_period_scan():
            scan_root = Path(root_input.text().strip())
            self.start_period_name_scan(
                scan_root,
                period_from_input.value(),
                period_to_input.value(),
                period_output,
                period_progress,
                run_period_scan_btn,
                cancel_period_scan_btn,
            )

        run_period_scan_btn.clicked.connect(run_selected_period_scan)
        cancel_period_scan_btn.clicked.connect(self.cancel_period_name_scan)

        period_widget.setLayout(period_layout)
        period_scroll.setWidget(period_widget)
        tabs.addTab(period_scroll, "Periode-scan")

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