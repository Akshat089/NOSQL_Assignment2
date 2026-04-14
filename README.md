# NoSQL Assignment 2 - Hadoop MapReduce

## Overview
This project contains Java MapReduce programs for:
- word frequency analysis
- co-occurrence matrix construction using both Pairs and Stripes patterns
- comparison of aggregation strategies (class-level vs function-level)

The repository includes a 50-article Wikipedia subset in `input/Wikipedia-50-ARTICLES`.

## Updated Repository Structure
```text
NOSQL_Assignment2/
├── .vscode/
│   └── settings.json
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
│   ├── Wikipedia-50-ARTICLES/
│   └── Wikipedia-50-ARTICLES.tar
├── lib/
│   ├── opennlp-tools-1.9.3.jar
│   └── opennlp-en-ud-ewt-pos-1.0-1.9.3.bin
├── bin/                      # compiled classes (manual/local compile)
├── target/                   # Maven build output
├── pom.xml
├── top50_only.txt
├── wordcount_output.txt
└── README.md
```

## Prerequisites
- Java JDK 11+
- Apache Hadoop 3.x
- Maven 3.6+
- Access to HDFS

## Build (Recommended: Maven)
Use Maven so dependency resolution is consistent with the project setup.

```bash
mvn clean compile
```

Create a jar:

```bash
mvn package
```

Notes:
- `target/classes` contains Maven-compiled classes.
- `bin` may also exist from earlier/manual `javac` builds.

## Build (Manual javac, optional)
If you prefer manual compilation:

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

### 2) Generate Top-50 Word List
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

### 5) Local Aggregation Variants

Pairs (class-level aggregation):
```bash
hadoop jar NoSQLAssignment2.jar WordCountPairsClassAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/pairs_classagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Pairs (function-level aggregation):
```bash
hadoop jar NoSQLAssignment2.jar WordCountPairsFuncAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/pairs_funcagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Stripes (class-level aggregation):
```bash
hadoop jar NoSQLAssignment2.jar WordCountStripesClassAgg \
	/user/$USER/input/Wikipedia-50-ARTICLES \
	/user/$USER/output/stripes_classagg_d1 \
	1 \
	/user/$USER/input/top50_only.txt
```

Stripes (function-level aggregation):
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
- Remove or rename output directories before re-running a job; Hadoop does not overwrite existing output paths.
- `WordCount2.java` exists in `src`, but it duplicates the `WordCountPairs` class name behavior; prefer running `WordCountPairs` directly.
- `PosTag.java` is a standalone OpenNLP utility and is not part of the Hadoop MapReduce execution flow.

## VS Code Classpath Fix (Previously Applied)
The repository has an editor-level classpath update in `.vscode/settings.json`:
- `java.project.referencedLibraries` includes `org/slf4j` jars from local Maven cache.

Why this was needed:
- Without SLF4J jars in VS Code referenced libraries, Hadoop classes such as `FileInputFormat` may show indirect type errors like `org.slf4j.Logger cannot be resolved` even when Maven build succeeds.

If stale diagnostics remain:
- Run `Java: Clean Java Language Server Workspace` from the Command Palette.
- Reload the VS Code window.
