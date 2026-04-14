# Problem 2: Document Frequency and TF-IDF with Hadoop

## Overview
This repository contains two Hadoop MapReduce jobs:
1. `DFJob` computes document frequency (DF) from a combined corpus file.
2. `TFIDFJob` computes TF-IDF scores for selected terms using DF values.

The workflow uses a single `combined.txt` input with document boundary markers to reduce overhead from many small input files.

## Repository Structure
```text
NOSQL_Assignment2/
├── DFJob.java
├── TFIDFJob.java
├── stopwords.txt
├── df.txt
├── top100.txt
├── tfidf.txt
├── .gitignore
└── README.md
```

## Requirements
- Java 17+
- Hadoop 3.x
- OpenNLP tools jar available on classpath

## Input Format
Create a combined input file where each document begins with a marker line:

```text
###DOC### filename.txt
```

Example command to generate `combined.txt` from a folder of text files:

```bash
cd <DATASET_FOLDER>
awk 'FNR==1{print "###DOC### " FILENAME} {print}' *.txt > combined.txt
```

## Build

```bash
cd <PROJECT_FOLDER>
rm -f *.class
javac -classpath "$(hadoop classpath):<PATH_TO_OPENNLP_JAR>" DFJob.java TFIDFJob.java
jar cf dfjob.jar DFJob*.class
jar cf tfidfjob.jar TFIDFJob*.class
```

## Run Part A: DF Computation

```bash
hadoop fs -rm -r -f <OUTPUT_DF_FOLDER>
hadoop jar dfjob.jar DFJob <DATASET_FOLDER>/combined.txt <OUTPUT_DF_FOLDER> <PROJECT_FOLDER>/stopwords.txt
```

Export DF results locally:

```bash
hadoop fs -getmerge <OUTPUT_DF_FOLDER> df.txt
sort -k2 -nr df.txt | head -100 > top100.txt
```

## Run Part B: TF-IDF Computation

```bash
hadoop fs -rm -r -f <OUTPUT_TFIDF_FOLDER>
hadoop jar tfidfjob.jar TFIDFJob <DATASET_FOLDER>/combined.txt <OUTPUT_TFIDF_FOLDER> <PROJECT_FOLDER>/stopwords.txt <PROJECT_FOLDER>/top100.txt [TOTAL_DOCS]
```

Notes:
- `TOTAL_DOCS` is optional. If omitted, default is `10000`.
- `top100.txt` is expected to contain term and DF value per line.

Export TF-IDF results locally:

```bash
hadoop fs -getmerge <OUTPUT_TFIDF_FOLDER> tfidf.txt
```

## Implementation Notes
- Stopwords are removed during mapping.
- Tokens are normalized to lowercase and stemmed using Porter stemmer.
- Single-character tokens are filtered out.
- The TF-IDF formula is:

$$
\mathrm{TFIDF}(t,d)=\mathrm{TF}(t,d)\cdot\ln\left(\frac{N}{\mathrm{DF}(t)}+1\right)
$$

where $N$ is total document count.

## Professional Cleanup Applied
- Replaced machine-specific cache file paths with runtime arguments in `DFJob` and `TFIDFJob`.
- Added `.gitignore` to exclude generated artifacts.
- Removed generated `bin/` and `target/` directories from the working tree.
- Standardized documentation to `README.md`.
