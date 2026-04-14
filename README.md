# NoSQL Assignment 2 - Hadoop MapReduce

## Overview
This project contains Java MapReduce programs for word frequency analysis and co-occurrence matrix construction using Apache Hadoop.

The current repository includes a 50-article Wikipedia subset in the `input/Wikipedia-50-ARTICLES` folder. The same jobs can be run on a larger corpus by changing the HDFS input path.

## Repository Structure
```text
NOSQL_Assignment2/
├── src/
│   ├── WordCount.java
│   ├── WordCountPairs.java
│   ├── WordCountPairsClassAgg.java
│   ├── WordCountPairsFuncAgg.java
│   ├── WordCountStripes.java
│   ├── WordCountStripesClassAgg.java
│   ├── WordCountStripesFuncAgg.java
│   ├── CustomWordCount.java
│   ├── CustomFileInputFormat.java
│   ├── CustomLineRecordReader.java
│   ├── WordCount2.java
│   └── PosTag.java
├── input/
│   ├── stopwords.txt
│   └── Wikipedia-50-ARTICLES/
├── lib/
│   ├── opennlp-tools-1.9.3.jar
│   └── opennlp-en-ud-ewt-pos-1.0-1.9.3.bin
└── README.md
```

## Prerequisites
- Java JDK 11 or higher
- Apache Hadoop 3.x
- Access to HDFS

## Build and Package
Compile the Hadoop jobs into `bin` and package them into one JAR.

```bash
mkdir -p bin

javac -cp "$(hadoop classpath)" -d bin \
	src/WordCount.java \
	src/WordCountPairs.java \
	src/WordCountPairsClassAgg.java \
	src/WordCountPairsFuncAgg.java \
	src/WordCountStripes.java \
	src/WordCountStripesClassAgg.java \
	src/WordCountStripesFuncAgg.java \
	src/CustomFileInputFormat.java \
	src/CustomLineRecordReader.java \
	src/CustomWordCount.java

jar -cvf NoSQLAssignment2.jar -C bin .
```

## Dataset Setup in HDFS
Update the destination path as needed for your Hadoop user.

```bash
hadoop fs -mkdir -p /user/$USER/input
hadoop fs -put -f input/Wikipedia-50-ARTICLES /user/$USER/input/
hadoop fs -put -f input/stopwords.txt /user/$USER/input/
```

## Execution Guide

### 1) Baseline Word Count
```bash
hadoop jar NoSQLAssignment2.jar WordCount \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/wordcount
```

### 2) Generate Top-50 Word List (for co-occurrence jobs)
Extract the top 50 words from the word count output, then upload the list to HDFS.

```bash
hdfs dfs -cat /user/$USER/output/wordcount/part-r-* \
	| sort -k2 -nr \
	| head -50 \
	| awk '{print $1}' > top50_only.txt

hadoop fs -put -f top50_only.txt /user/$USER/input/top50_only.txt
```

### 3) Co-occurrence Matrix (Pairs)
`distance` controls the neighborhood window size.

```bash
hadoop jar NoSQLAssignment2.jar WordCountPairs \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/pairs_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

### 4) Co-occurrence Matrix (Stripes)
```bash
hadoop jar NoSQLAssignment2.jar WordCountStripes \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/stripes_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

### 5) Local Aggregation Variants (Comparison)

Pairs with class-level aggregation:
```bash
hadoop jar NoSQLAssignment2.jar WordCountPairsClassAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/pairs_classagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Pairs with function-level aggregation:
```bash
hadoop jar NoSQLAssignment2.jar WordCountPairsFuncAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/pairs_funcagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Stripes with class-level aggregation:
```bash
hadoop jar NoSQLAssignment2.jar WordCountStripesClassAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/stripes_classagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Stripes with function-level aggregation:
```bash
hadoop jar NoSQLAssignment2.jar WordCountStripesFuncAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/stripes_funcagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

### 6) Custom InputFormat Word Count (optional)
```bash
hadoop jar NoSQLAssignment2.jar CustomWordCount \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/custom_wordcount
```

## Output Inspection
```bash
hdfs dfs -cat /user/$USER/output/pairs_d1/part-r-* | head -50
hdfs dfs -cat /user/$USER/output/stripes_d1/part-r-* | head -50
```

## Important Notes
- Remove or rename an output directory before re-running a job, because Hadoop does not overwrite existing output paths.
- `WordCount2.java` is present in `src`, but it currently duplicates the `WordCountPairs` class name; prefer running `WordCountPairs` directly.
- `PosTag.java` is a standalone OpenNLP utility and is not part of the Hadoop pipeline above.
