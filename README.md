# 🥫 Open Food Facts — NOVA Group Classifier

> Klasifikasi tingkat pemrosesan makanan berbasis Open Food Facts dataset untuk mendukung **SDG 12: Responsible Consumption and Production**.

---

## 📌 Latar Belakang

Makanan ultra-proses (NOVA group 4) berkontribusi pada masalah kesehatan global sekaligus meninggalkan jejak lingkungan yang signifikan. Proyek ini membangun **pipeline big data berbasis arsitektur Lakehouse** menggunakan Apache Spark dan Delta Lake untuk memproses dan menganalisis dataset Open Food Facts, serta membangun model klasifikasi NOVA group.

---

## 🎯 Tujuan

- Membangun pipeline big data berbasis Medallion Architecture (Bronze → Silver → Gold)
- Membangun model klasifikasi NOVA group (1–4) dengan target F1-score ≥ 80%
- Menganalisis korelasi antara tingkat pemrosesan dengan dampak lingkungan (Eco-Score)
- Membandingkan performa pipeline Spark vs pipeline pandas sekuensial

---

## 👥 Tim Pengembang (Kelompok 13)

Proyek ini dikembangkan oleh **Kelompok 13 - Sains Data ITERA 2026** sebagai bagian dari Tugas Besar Analisis Big Data:

| No | Nama | NIM | GitHub |
|----|------|-----|--------|
| 1 | Khairunnisa Maharani | 123450071 | [@KMoex-HZ](https://github.com/KMoex-HZ) |
| 2 | Fadil Prasetyo A | 123450048 | [@dillcipher](https://github.com/dillcipher) |
| 3 | Muhammad Dzikra | 123450124 | [@muhammaddzikra](https://github.com/muhammaddzikra) |
| 4 | Feby Agelina | 123450039 | [@FebyAngelina](https://github.com/FebyAngelina) |

---

## 🛠️ Stack Teknologi

| Komponen | Versi |
|----------|-------|
| Apache Spark | 3.5.1 |
| Delta Lake | 3.1.0 |
| PySpark Notebook | spark-3.5.1 |
| Docker | — |
| Python | 3.11 |

---

## 📁 Struktur Repositori

```
Lakehouse-SDG12-OpenFoodFacts/
├── notebooks/
│   ├── 01_bronze.ipynb       # Bronze Layer — ingestion ke Delta Lake
│   ├── 02_silver.ipynb       # Silver Layer — cleaning & feature engineering
│   ├── 03_gold.ipynb         # Gold Layer — agregasi bisnis
│   ├── 04_ml.ipynb           # Model ML — klasifikasi NOVA group
│   └── 05_benchmark.ipynb    # Benchmark Spark vs pandas
├── docker/
│   └── docker-compose.yml    # Konfigurasi Spark cluster + Jupyter
├── eda.ipynb                  # Exploratory Data Analysis
├── environment.yml            # Conda environment (untuk EDA)
├── README.md
└── .gitignore
```

> **Note:** Dataset `food.parquet` (~7GB) tidak di-commit ke repo. Download dari [HuggingFace](https://huggingface.co/datasets/openfoodfacts/product-database) dan taruh di folder root repo.

---

## 📊 Dataset

| Properti | Detail |
|----------|--------|
| Sumber | [Open Food Facts @ HuggingFace](https://huggingface.co/datasets/openfoodfacts/product-database) |
| Format | `.parquet` (lokal, ~7GB) |
| Ukuran | 4.487.169 produk, 111 kolom |
| Target | `nova_group` (1 = unprocessed, 4 = ultra-processed) |

---

## 🚀 Cara Menjalankan

### 1. Clone repo
```bash
git clone https://github.com/KMoex-HZ/Lakehouse-SDG12-OpenFoodFacts.git
cd Lakehouse-SDG12-OpenFoodFacts
```

### 2. Download dataset
Download `food.parquet` (~7GB) dari [HuggingFace](https://huggingface.co/datasets/openfoodfacts/product-database) dan taruh di folder root repo.

### 3. Jalankan Spark cluster
```bash
cd docker
docker compose up -d
```

### 4. Ambil token Jupyter
```bash
docker logs spark-jupyter 2>&1 | grep token
```

### 5. Buka Jupyter di browser
```
http://localhost:8888/?token=<token>
```

### 6. Jalankan notebook secara berurutan
```
01_bronze.ipynb → 02_silver.ipynb → 03_gold.ipynb → 04_ml.ipynb
```

> **Catatan hardware:** Semua layer dijalankan dalam mode `local[2]` karena keterbatasan RAM (7.6GB). Disarankan minimal 16GB RAM untuk cluster mode penuh.

---

## 📈 Status Pipeline

### EDA ✅
- [x] Schema inspection (111 kolom)
- [x] Total dataset: 4.487.169 produk
- [x] Missing value analysis — `nova_group` hanya 24.9% terisi
- [x] Class imbalance check — NOVA 4 dominan 63.9%
- [x] Distribusi geografis — US & France dominan (geographic bias)
- [x] Investigasi struktur `nutriments` (array of struct)
- [x] Profil nutrisi rata-rata per NOVA group + visualisasi

### Bronze Layer ✅
- [x] Ingestion `food.parquet` → Delta Lake
- [x] 4.487.169 baris teringesti, 112 kolom (111 + ingestion_timestamp)
- [x] Throughput: **12.365 baris/detik**

### Silver Layer ✅
- [x] Filter `nova_group IS NOT NULL` → 1.119.410 baris
- [x] UNNEST `nutriments` → kolom flat (energy, fat, sugars, proteins, salt)
- [x] Cleaning: missing values → median (numerik), 'unknown' (teks)
- [x] Encoding: `nutriscore_grade` A→1..E→5
- [x] Feature engineering: `additives_count`, `has_palm_oil`, `is_vegan`, `is_vegetarian`
- [x] Throughput: **5.895 baris/detik**

### Gold Layer ⏳
- [ ] Agregasi nutrisi per kategori & negara
- [ ] Distribusi Eco-Score & NOVA global

### Model ML ⏳
- [ ] Random Forest klasifikasi NOVA group
- [ ] Target: F1-score macro ≥ 80%

### Benchmark ⏳
- [ ] Spark vs pandas — throughput & latensi

---

## 🎯 Metrik Keberhasilan

| Layer | Metrik | Target | Hasil |
|-------|--------|--------|-------|
| Bronze | Baris teringesti | 4.487.169 | ✅ 4.487.169 |
| Bronze | Throughput | dicatat | ✅ 12.365 baris/dtk |
| Silver | Baris setelah filter | ~1.1 juta | ✅ 1.119.410 |
| Silver | Null di fitur utama | = 0 | ✅ 0 semua |
| Gold | Tabel agregasi tersedia | ✅ | — |
| ML | F1-score macro | ≥ 80% | — |
| Benchmark | Throughput Spark | ≥ 50.000 baris/dtk | — |

---

## 🔗 Referensi

- [Open Food Facts](https://world.openfoodfacts.org/)
- [NOVA Classification System](https://www.fao.org/nutrition/education/food-dietary-guidelines/background/sustainable-diets/en/)
- [Lakehouse Paper — Armbrust et al., 2021](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)
- [Arora et al., 2025 — ML for NOVA Classification](https://arxiv.org/abs/2512.17169)

---

## 📄 Lisensi

Dataset: [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/)  
Kode: MIT