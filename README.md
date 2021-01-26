# SQL Query Converter for AWS Services

## Objective
This script is used for convert standard basic "LIKE" SQL statement to be "Regular Expression"-based SQL statement for Amazon Redshift and Amazon Athena

## Pre-requisite
* Python 3.6+

## How-To Use script
`$ python query-converter.py [options]`
* -f, --file : input filename
* -s, --service : Amazon services [redshift or athena]
* -t, --type : convert type
* -o, --output : output file
* -h, --help : help

## Example of Input
```
SELECT id
FROM database.table
WHERE (text LIKE '%word1%')
    OR (text LIKE '%word2%')
    OR (text LIKE '%word3%' AND text LIKE '%word4%')
;
```
## Example of Amazon Redshift Output
**Type 3: convert OR to text ~ 'word1|word2' and leave AND as-is**
```
SELECT id
FROM database.table
WHERE text ~ 'word1|word2'
    OR (text LIKE '%word3%' AND text LIKE '%word4%')
;
```
**Type 4: convert OR to text ~ 'word1|word2' and combine AND with permutation**
```
SELECT id
FROM database.table
WHERE text ~ 'word1|word2|word3.*word4|word4.*word3'
;
```
## Example of Amazon Athena Output
**Type 1: Convert LIKE to regexp**
```
SELECT id
FROM database.table
WHERE (regexp_like(text,'word1'))
    OR (regexp_like(text,'word2'))
    OR ((regexp_like(text,'word3'))
        AND (regexp_like(text,'word4')))
;
```
**Type 2: convert LIKE to regexp and combine AND with permutations**
```
SELECT id
FROM database.table
WHERE (regexp_like(text,'word1'))
    OR (regexp_like(text,'word2'))
    OR (regexp_like(text,'word3.*word4'))
    OR (regexp_like(text,'word4.*word3'))
;
```