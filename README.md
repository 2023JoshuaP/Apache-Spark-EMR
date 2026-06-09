# EMR Spark Project

Apache Spark data processing project for AWS EMR, implementing various text analysis and data processing tasks.

## Overview

This project contains PySpark scripts for distributed data processing on Amazon EMR, including word count analysis, inverted indexing, and Parquet data format handling.

## Project Structure

```
.
├── script_wordcount.py           # Word frequency counting implementation
├── script_invertedindex.py       # Inverted index creation for text search
├── script_analysis_parquet.py    # Parquet file analysis
├── split_data.py                 # Data splitting utility
├── configuration.txt             # Configuration settings
├── LICENSE                       # MIT License
├── README.md                     # This file
├── logs/                         # Log files from script execution
└── salida_indice/               # Output directory for results
```

## Scripts

### Word Count
`script_wordcount.py` - Computes word frequencies across input documents.

### Inverted Index
`script_invertedindex.py` - Creates inverted indices mapping terms to document locations.

### Parquet Analysis
`script_analysis_parquet.py` - Analyzes and processes data in Parquet format.

### Data Splitting
`split_data.py` - Utility for data partitioning and preparation.

## Requirements

- Apache Spark
- AWS EMR cluster
- Python 3.x

## Usage

Run scripts on EMR cluster:

```bash
spark-submit script_wordcount.py <input_path> <output_path>
spark-submit script_invertedindex.py <input_path> <output_path>
spark-submit script_analysis_parquet.py <input_path> <output_path>
```

## Configuration

Edit `configuration.txt` to adjust script parameters and behavior.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
