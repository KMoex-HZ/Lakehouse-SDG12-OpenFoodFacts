# 🥫 Open Food Facts — NOVA Group Classifier

> Klasifikasi tingkat pemrosesan makanan berbasis Open Food Facts dataset untuk mendukung **SDG 12: Responsible Consumption and Production**.

---

## 📌 Latar Belakang

Makanan ultra-proses (NOVA group 4) berkontribusi pada masalah kesehatan global sekaligus meninggalkan jejak lingkungan yang signifikan. Proyek ini membangun pipeline machine learning untuk memprediksi **NOVA group** suatu produk makanan berdasarkan kandungan nutrisi, komposisi bahan, dan data kemasan — menggunakan dataset publik [Open Food Facts](https://world.openfoodfacts.org/).

---

## 🎯 Tujuan

- Membangun model klasifikasi NOVA group (1–4) dari fitur nutrisi dan komposisi
- Menganalisis korelasi antara tingkat pemrosesan dengan dampak lingkungan (Eco-Score)
- Mitigasi bias geografis dengan mempertimbangkan distribusi negara asal produk

---

## 📁 Struktur Repositori

```
.
├── eda.ipynb               # Exploratory Data Analysis (tahap awal)
├── environment.yml         # Conda environment
├── README.md
└── .gitignore
```

> **Note:** Dataset tidak di-commit ke repo karena ukurannya >10GB. Data diakses langsung via URL Parquet dari HuggingFace (lihat notebook).

---

## 📊 Dataset

| Properti | Detail |
|----------|--------|
| Sumber | [Open Food Facts @ HuggingFace](https://huggingface.co/datasets/openfoodfacts/product-database) |
| Format | `.parquet` (diakses via DuckDB remote query) |
| Ukuran | ~3 juta produk, 111 kolom |
| Target | `nova_group` (1 = unprocessed, 4 = ultra-processed) |

### Kolom yang Digunakan (30 dari 111)

| Kelompok | Kolom |
|----------|-------|
| Identitas Produk | `code`, `product_name`, `brands`, `categories`, `countries_tags` |
| Target ML | `nova_group`, `nutriscore_grade`, `nutriscore_score` |
| Fitur Nutrisi | `nutriments`, `nutrition_data_per`, `ingredients_text`, `ingredients_n`, `additives_n`, `additives_tags`, `with_sweeteners` |
| SDG 12 / Lingkungan | `environmental_score_grade`, `environmental_score_score`, `environmental_score_data`, `packaging_tags`, `ingredients_from_palm_oil_n` |
| Kualitas Data | `completeness`, `data_quality_errors_tags`, `data_quality_warnings_tags`, `unknown_ingredients_n`, `food_groups_tags`, `labels_tags`, `ingredients_analysis_tags`, `quantity`, `serving_size`, `main_countries_tags` |

---

## 🚀 Cara Menjalankan

### 1. Clone repo
```bash
git clone https://github.com/<username>/<repo-name>.git
cd <repo-name>
```

### 2. Setup environment
```bash
conda env create -f environment.yml
conda activate off-nova-classifier
```

### 3. Jalankan notebook
```bash
jupyter notebook eda.ipynb
```

> **File lokal diperlukan** — download `food.parquet` (~7GB) dari [HuggingFace](https://huggingface.co/datasets/openfoodfacts/product-database), taruh di folder yang sama dengan notebook.

---

## 📈 Status EDA

- [x] Schema inspection (111 kolom)
- [x] Total dataset: 4.487.169 produk
- [x] Missing value analysis — `nova_group` hanya 24.9% terisi
- [x] Class imbalance check — NOVA 4 dominan 63.9%
- [x] Distribusi geografis — US & France dominan (geographic bias)
- [x] Investigasi struktur `nutriments` (array of struct, akses via UNNEST)
- [x] Profil nutrisi rata-rata per NOVA group + visualisasi
- [x] Sample data lengkap
- [ ] Korelasi antar fitur numerik
- [ ] Distribusi `nutriscore_grade` vs `nova_group`

---

## 🔗 Referensi

- [Open Food Facts](https://world.openfoodfacts.org/)
- [NOVA Classification System](https://www.fao.org/nutrition/education/food-dietary-guidelines/background/sustainable-diets/en/)
- [DuckDB Remote Parquet Docs](https://duckdb.org/docs/guides/network_cloud_storage/http_import)

---

## 📄 Lisensi

Dataset: [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/)  
Kode: MIT
