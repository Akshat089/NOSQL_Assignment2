# README – Problem 2 (DF + TF-IDF using Combined File Approach)

## 📌 Overview

This project computes:

* **Part (a): Document Frequency (DF)**
* **Part (b): TF-IDF scores for top 100 terms**

To efficiently handle large datasets, all input files are **combined into a single file (`combined.txt`)** to avoid Hadoop’s small-file problem.

---

# ⚙️ Prerequisites

* Hadoop installed and configured
* Java (JDK 17)
* OpenNLP jar (e.g., `opennlp-tools-1.9.3.jar`)
* Dataset folder containing multiple `.txt` documents

---

# 🚀 STEP 1: Combine all files

Navigate to dataset folder:

```bash
cd <DATASET_FOLDER>
```

Run:

```bash
awk 'FNR==1{print "###DOC### " FILENAME} {print}' *.txt > combined.txt
```

### ✅ Output:

* `combined.txt` with document boundaries marked as:

  ```
  ###DOC### filename.txt
  ```

---

# 🧩 PART (a): Document Frequency (DF)

---

## 🔹 Step 2: Compile DFJob

```bash
cd <PROJECT_FOLDER>

rm *.class

javac -classpath `hadoop classpath`:<PATH_TO_OPENNLP_JAR> DFJob.java

jar cf dfjob.jar DFJob*.class
```

---

## 🔹 Step 3: Remove old output

```bash
rm -r <OUTPUT_DF_FOLDER>
```

---

## 🔹 Step 4: Run DFJob (LOCAL MODE)

```bash
hadoop jar dfjob.jar DFJob <DATASET_FOLDER>/combined.txt <OUTPUT_DF_FOLDER>
```

---

## 🔹 Step 5: Extract DF results

```bash
cp <OUTPUT_DF_FOLDER>/part-r-00000 df.txt
```

---

## 🔹 Step 6: Get Top 100 terms

```bash
sort -k2 -nr df.txt | head -100 > top100.txt
```

---

## ✅ Outputs of Part (a)

* `df.txt` → term and document frequency
* `top100.txt` → top 100 most frequent terms

---

# 🧩 PART (b): TF-IDF Computation

---

## 🔹 Step 7: Compile TFIDFJob

```bash
rm *.class

javac -classpath `hadoop classpath`:<PATH_TO_OPENNLP_JAR> TFIDFJob.java

jar cf tfidf.jar TFIDFJob*.class
```

---

## 🔹 Step 8: Remove old output

```bash
rm -r <OUTPUT_TFIDF_FOLDER>
```

---

## 🔹 Step 9: Run TF-IDF Job

```bash
hadoop jar tfidf.jar TFIDFJob <DATASET_FOLDER>/combined.txt <OUTPUT_TFIDF_FOLDER>
```

---

## 🔹 Step 10: Extract final output

```bash
cp <OUTPUT_TFIDF_FOLDER>/part-r-00000 tfidf.txt
```

---

## ✅ Output of Part (b)

* `tfidf.txt` containing:

  ```
  documentID    term    TF-IDF score
  ```

---

# ⚡ Key Optimization

Instead of:

```text
Many small files → many map tasks → slow ❌
```

We use:

```text
One combined file → few splits → fast ✔
```

---

# 🧠 Notes

* Stopwords are removed in both DF and TF-IDF stages
* Stemming is applied using Porter Stemmer
* Single-character tokens (length = 1) are removed to eliminate noise and non-informative terms
* TF is computed per document
* DF is read from `df.txt`
* Natural logarithm is used

---

# Final Files

| File         | Description         |
| ------------ | ------------------- |
| combined.txt | Combined dataset    |
| df.txt       | Document frequency  |
| top100.txt   | Top 100 terms       |
| tfidf.txt    | Final TF-IDF scores |

---

# Done

This completes:

* DF computation
* TF-IDF scoring
* Efficient handling of large datasets

---
