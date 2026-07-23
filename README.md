GCP eller General Census Program er et program som er designet til at fungere som en nøddeknækker, når man ikke ved, hvor en person er.

Man kan sidde og lede i andre hjemmesider og programmer, men man kan kun søge i enkelte sogne efter folk. Dette program er skabt til at gøre processen nemmere.

Da man i stedet for at søge i et enkelt sogn eller amt nu har mulighed for at søge i en hel folketælling på tværs af sogne, amter og regioner.

Man brute-forcer sig igennem problemet, hvilket også er grunden til at jeg kalder programmet for en nøddeknækker, hver søgning tager ca. 15 sekunder,

Men man søger på tværs i en samlet folketælling.


# Folketællings-søger 🇩🇰

Et Python-baseret desktop program til at søge parallelt i tusinder af CSV-filer med danske folketal-data.

## Features ✨

- **Parallel søgning** - Søger i flere CSV-filer samtidigt (ThreadPoolExecutor)
- **Desktop GUI** - PyQt5-baseret grafisk interface
- **Avancerede filtre**:
  - Søg efter navn
  - Filtrer efter køn (M/K)
  - Filtrer efter civilstand
  - Søg efter alder (fra/til)
  - Søg efter fødeår (fra/til)
  - Søg efter fødested
- **Realtids-progress** - Se søgningens fremskridt i procent
- **Resultattabel** - Vis alle resultater i sortérbar tabel
- **Dansk UI** - Alt på dansk

## Installation 📦

### Forudsætninger
- Python 3.8+
- pip

### Setup
```bash
# Klon eller hent koden
cd "e:\GIS\Github Update\Folketållinger"

# Installer dependencies
pip install -r requirements.txt
```

## Brug 🚀

### Start programmet:
```bash
python csv_searcher.py
```

### Sådan søger du:
1. **Vælg mappe** - Klik "Vælg Mappe" og vælg `C:\Users\erikl\Desktop\Folketållinger\ddd-ansi`
2. **Indstil filtre** - Udfyld søgeparametre:
   - Navn: navn eller del af navn (case-insensitive)
   - Køn: M, K eller "Alle"
   - Civilstand: Gift, Ugift, etc.
   - Alder: fra og til værdi
   - Fødeår: fra og til værdi
   - Fødested: by navn eller region
3. **Klik "Start Søgning"** - Programmet søger nu gennem alle CSV-filer
4. **Se resultater** - Resultaterne vises i tabellen

### Eksempler:
- Find alle med navn "Hansen": Navn = "Hansen"
- Find alle mænd født mellem 1880-1900: Køn = "M", Fødeår 1880-1900
- Find gifte kvinder: Køn = "K", Civilstand = "Gift"

## Arkitektur 🏗️

```
csv_searcher.py
├── SearchWorker (QThread worker)
│   ├── run() - Hoved søgnings-logik
│   ├── search_file() - Søger i enkelt CSV
│   └── apply_filters() - Anvender filter
└── CsvSearcherGUI (QMainWindow)
    ├── init_ui() - Opbygger GUI
    ├── start_search() - Starter søgning
    ├── display_results() - Viser resultater
    └── browse_folder() - Folder selector
```

## Ydeevne ⚡

- **2000+ CSV-filer**: ~5-15 sekunder på moderne PC
- **Parallel processing**: 8 worker threads
- **Non-blocking UI**: GUI forbliver responsiv under søgning

## Encoding 🔤

Programmet understøtter dansk tekst (æ, ø, å) gennem:
- `latin-1` encoding for CSV-læsning
- UTF-8 for UI

## Fejlfinding 🐛

**Problem**: "Ingen CSV-filer fundet"
- **Løsning**: Sørg for at mappen indeholder `.csv` filer

**Problem**: Søgning er meget langsom
- **Løsning**: Begrens søgningen med flere filtre

**Problem**: Karaktertegn vises forkert
- **Løsning**: Dette er løst via `latin-1` encoding

## Fremtid 🚀

Mulige forbedringer:
- [ ] Export til Excel/CSV
- [ ] Gemte søgninger
- [ ] Søg efter relaterede personer (familie)
- [ ] Kort-integration for stednavne
- [ ] Statistik og analyse
- [ ] Batch-operationer

## Licens
MIT

## Kontakt
Erik Oehlerich - ErikOehlerich
