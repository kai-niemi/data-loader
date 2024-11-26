[![Java CI with Maven](https://github.com/kai-niemi/volt/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/kai-niemi/volt/actions/workflows/maven.yml)
       
<!-- TOC -->
* [About Volt](#about-volt)
* [Quick Start](#quick-start-)
  * [Prerequisites](#prerequisites)
  * [Running](#running)
* [Building](#building-)
  * [Prerequisites](#prerequisites-1)
  * [Install the JDK](#install-the-jdk)
  * [Build and Run](#build-and-run)
* [How to Use](#how-to-use)
  * [Generate Configuration](#generate-configuration)
    * [Preparation](#preparation)
    * [Generating CSV Files](#generating-csv-files)
    * [Importing CSV Files](#importing-csv-files)
    * [Generating Configuration](#generating-configuration)
    * [Sorting CSV Files](#sorting-csv-files)
* [Configuration](#configuration)
  * [Model](#model)
    * [Tables](#tables)
      * [Columns](#columns)
        * [Constant](#constant)
        * [Expression](#expression)
        * [Gen](#gen)
        * [Each](#each)
        * [Ref](#ref)
        * [Range](#range)
        * [Set](#set)
      * [Options (table level)](#options-table-level)
    * [Options (top level)](#options-top-level)
    * [Import Into](#import-into-)
      * [Options (import into level)](#options-import-into-level)
* [Terms of Use](#terms-of-use)
<!-- TOC -->

# About Volt

<img align="left" src="logo.png" width="128" /> Volt is a flexible, low-effort CSV file 
generator targeting CockroachDB IMPORT and COPY data loading. As such, it generates 
CSV files based on a YAML configuration which in turn can be optionally generated 
from a database schema.

The memory footprint is low when generating very large CSVs since it doesn't build 
much state during CSV data generation. The main exception being 
[cartesian or cross product](https://en.wikipedia.org/wiki/Join_(SQL)#Cross_join)
multi-table relations where aggregation needs to be done in-memory before
writing data (see [Each](#each) below for details). For more typical one-to-many relationships
it uses circular bounded buffers to constrain memory usage.

The tool comes with a command-line interface, an interactive shell and a Hypermedia REST API 
that supports CockroachDB `IMPORT INTO` and `COPY FROM` commands for consuming CSV files. 
The REST API can also stream CSV data on demand without actually writing to local files.

Main use cases:

- Generate large CSV files for the purpose of functional / load testing
- Support `IMPORT INTO` and `COPY FROM` commands via HTTP endpoint
- Merge-sort large CSV files

# Quick Start 

Either download the [release jar](https://github.com/kai-niemi/volt/releases) or build it locally.

## Prerequisites

- Java 17+ JRE
- CockroachDB 23.2+
  - https://www.cockroachlabs.com/docs/releases/

## Running

Run the app with:

    java -jar volt.jar

Type `help` for further guidance.

# Building 

## Prerequisites

- Java 17 (or later) JDK
    - https://openjdk.org/projects/jdk/17/
    - https://www.oracle.com/java/technologies/downloads/#java17
- CockroachDB 23.2 (or later) 
    - https://www.cockroachlabs.com/docs/releases/

## Install the JDK

Ubuntu:

    sudo apt-get install openjdk-17-jdk

MacOS (using sdkman):

    curl -s "https://get.sdkman.io" | bash
    sdk list java
    sdk install java 17.. (pick version)  

## Build and Run

Clone the project:

    git clone git@github.com:cloudneutral/volt.git volt
    cd volt

Build a single, executable JAR:

    ./mvnw clean install

# How to Use

This tutorial shows how you can generate CSV files and `IMPORT INTO` SQL commands
from an existing database schema. 

## Generate Configuration

To create the configuration YAML file, you can either copy and modify one of
the [sample](samples/), or generate a new one from an existing database schema 
by introspection. The generated file can be further fine-tuned afterwards to use 
proper randomization functions and more.

### Preparation

For the purpose of this tutorial, first create a `volt` database with a 
sample schema using [create-default.sql](samples/create-default.sql):

    cockroach sql --insecure --host=localhost -e "CREATE database volt"
    cockroach sql --insecure --host=localhost --database volt < samples/create-default.sql

### Generating CSV Files

The `application-default.yml` profile will create three separate CSV files 
with 100 customers, 200 orders and 1,000 order items. This file describes 
the CSV file layouts and relations which can be inferred from an existing 
database schema (see next section).

Generate the CSV files using `csv-generate` shell command:

    echo "csv-generate --quit" > cmd-gen.txt
    java -jar target/volt.jar @cmd-gen.txt

Upon completion, the `.output` directory will now have the following files:

    $ ls -1 .output
    customer.csv
    purchase_order.csv
    purchase_order_item.csv
    import.sql

The `.output/*.csv` files contains random generated CSV data based on the
YAML configuration. The `.output/import.sql` file contains `IMPORT INTO` SQL 
statements in topological order inferred from the table relationships.

### Importing CSV Files

When you run volt with the http mode enabled, CockroachDB can consume the 
CSV files using either `IMPORT` or `COPY`:

    java -jar target/volt.jar --http

Import all the CSVs using `IMPORT INTO`:

    cockroach sql --insecure --host=localhost --database volt < .output/import.sql

> Hint: If your import jobs get stuck you can cancel them with:

    CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE job_type='IMPORT' and status != 'failed');

Since `IMPORT INTO` takes the tables offline, you can alternatively use `COPY .. FROM` instead:

Example commands using local files:

    echo "--insecure --database volt" > credentials.txt
    echo "COPY customer FROM STDIN WITH CSV DELIMITER ',' HEADER;" | cat - .output/customer.csv | cockroach sql $( cat credentials.txt )
    echo "COPY purchase_order FROM STDIN WITH CSV DELIMITER ',' HEADER;" | cat - .output/purchase_order.csv | cockroach sql $( cat credentials.txt )
    echo "COPY purchase_order_item FROM STDIN WITH CSV DELIMITER ',' HEADER;" | cat - .output/purchase_order_item.csv | cockroach sql $( cat credentials.txt )

Example commands using volt as HTTP proxy for the local files:

    echo "COPY customer FROM STDIN WITH CSV DELIMITER ',' HEADER;" > header.csv && curl http://localhost:8090/customer.csv | cat header.csv - | cockroach sql $( cat credentials.txt )
    echo "COPY purchase_order FROM STDIN WITH CSV DELIMITER ',' HEADER;" > header.csv && curl http://localhost:8090/purchase_order.csv | cat header.csv - | cockroach sql $( cat credentials.txt )
    echo "COPY purchase_order_item FROM STDIN WITH CSV DELIMITER ',' HEADER;" > header.csv && curl http://localhost:8090/purchase_order_item.csv | cat header.csv - | cockroach sql $( cat credentials.txt )

### Generating Configuration

The shell command `db-export` will generate configuration YAML file from a database.

    echo "db-export --quit" > cmd-export.txt
    java -jar target/volt.jar @cmd-export.txt

Check out the YAML file:
    
    cat .output/application-default.yml

This command makes a best-effort to produce the CSV table layouts and relations by introspection 
and following foreign key relations. It should print content similar to 
[application-default.yml](samples/application-default.yml). You may need to edit the file
to use more appropriate random functions and more.

### Sorting CSV Files

Ideally when importing a large CSV file to CockroachDB, it should be split into smaller pieces 
(one file per node) and each split should be sorted based on how its stored in the database. 
That saves sorting effort for the database, drastically speeding up the time it takes to
ingest the data. If for example the data is stored by an alphabetic primary key, then the 
CSV should be sorted in the same way. 

There are built-in shell commands for both splitting and sorting large CSV files 
using the [external merge sort](https://en.wikipedia.org/wiki/External_sorting) algorithm. 

The `sort` command will split a single CSV file, sort each part and then merge the 
sorted parts back:

    sort --inputFile ./output/orders.csv

Now you have the `orders.csv` file sorted by all columns by default. The sort command 
doesn't know about the storage order, so you can instruct which columns to sort 
by rather than include all by using the `--columns` parameter.

Next step would be to split the sorted file and adjust your import command to 
point at each part:

    split --inputFile ./output/orders.csv

Lastly, edit the `.output/import.sql` file accordingly:

    IMPORT INTO orders(id,payment_method_id,bill_address1,bill_address2,bill_city,bill_country_code,status,amount,currency,version)
    CSV DATA (
     'http://localhost:8090/orders-001.csv',
     'http://localhost:8090/orders-002.csv',
     'http://localhost:8090/orders-003.csv',
     'http://localhost:8090/orders-004.csv',
     'http://localhost:8090/orders-005.csv',
     'http://localhost:8090/orders-006.csv',
     'http://localhost:8090/orders-007.csv',
     'http://localhost:8090/orders-008.csv'
    ) WITH delimiter = ',', skip = '1', allow_quoted_null = 'null', fields_enclosed_by = '"';

# Configuration

Reference section for the application YAML. 
            
## Model

Top-level entry in `application<-profile>.yml`. 

    model:
      tables:
      outputPath:
      options:
      importInto:

| Field Name  | Optional | Default | Description                                                                                                                  |
|-------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------|
| tables      | No       | -       | Collection of tables to create CSV files for.                                                                                |
| outputPath  | Yes      | .output | Output directory for generated files (created on demand).                                                                    |
| options     | Yes      | -       | CSV format options, reusing the [IMPORT INTO](https://www.cockroachlabs.com/docs/stable/import-into#import-options) options. |
| importInto  | Yes      | -       | Import-into SQL file settings (skipped if omitted).                                                                          |

### Tables

Collection of tables where one table map to one CSV file. Table can be related to other tables 
using `each` or `ref` columns. If such relations exist, volt will buffer ancestor table rows
in memory while generating descendant rows (using bounded cyclic buffers). 

    model:
      tables:
      - name: customer
        count: "100K"
        columns:
          ...
        options:
          ...  

| Field Name | Optional | Default | Description                                                                                                                                                    |
|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name     | No       | -       | Name of the table which maps to CSV file name.                                                                                                                 |
| count    | Yes      | "100"   | Number of rows to create in multiplier syntax (K/M/G suffix like 32G). Refused for tables with `ref` columns since these derive rows from reference(d) tables. |
| columns  | No       | -       | Collection of columns to generate.                                                                                                                             |
| options  | Yes      | -       | CSV format options with precedence over top-level options.                                                                                                     |

#### Columns

    model:
      tables:
        ...
        columns:
        - name: "id"
          hidden: true
          constant: "Abrakadabra"
          expression: "plus(currentDate(), randomInt(1,30), 'DAYS')"
          gen:
            type: "unordered"
            from: 1
            to: 100
            step: 1
            batchSize: 32
          each:
            name: customer
            column: id
            multiplier: 1
          ref:
            name: customer
            column: id
          range:
            type: date
            from: "2023-12-01"
            to: "2024-12-01"
            step: 1
            step-unit: DAYS
          set:  
            values: 1,3,4,5
            weights: [.2,.3,.4,.1]

| Field Name | Optional | Default | Description                                                                                                              |
|------------|---------|---------|--------------------------------------------------------------------------------------------------------------------------|
| name       | No      | -       | Name of the column.                                                                                                      |
| hidden     | Yes     | false   | Column is excluded from CSV.                                                                                             |
| constant   | Yes     | .       | A constant value.                                                                                                        |
| expression | Yes     | .       | A binary expression that resolves to a column value. For a list of functions type `expr-functions`.                      |
| gen        | Yes     | -       | Create value using an ID generator.                                                                                      |
| each       | Yes     | -       | Create value derived from a referenced table column. Also creates at least one row for each row in the referenced table. |
| ref        | Yes     | -       | Create value from a referenced table column.                                                                             |
| range      | Yes     | -       | Create value from a generated series.                                                                                    |
| set        | Yes     | -       | Create random value from a weighted set.                                                                                 |

**Remarks:**

A column much pick exactly one of the following value generator fields:

- constant
- expression
- gen     
- each      
- ref       
- range     
- set       
          
---

##### Constant

Create value using a fixed constant.

    model:
      tables:
        ...
        columns:
        - name: "name"
          constant: "Hello"

| Field Name | Optional | Default | Description |
|------------|----------|---------|-------------|
| constant   | Yes       | -       | The value.  |

---

##### Expression

A binary expression that is parsed and resolved to a column value. For a list of functions and constants
type `expr-functions` in the shell and TAB for code completion.

    model:
      tables:
        ...
        columns:
        - name: "name"
          expression: "plus(currentDate(), randomInt(1,30), 'DAYS')"
          
| Field Name | Optional | Default | Description                                                                                                                    |
|------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------|
| expression | Yes      | -       | A binary expression following the [ExpressionParser](src/main/resources/io/roach/volt/expression/ExpressionParser.g4) grammar. |

**Remarks:**

The logical expressions and support basic arithmetics, string manipulation, function calls and simple 
conditional logic using `IF <condition> THEN <outcome> ELSE <other-outcome>`. For some examples, 
see [ExpressionGrammarTest.java](src/test/java/io/roach/volt/expression/ExpressionGrammarTest.java).

---

##### Gen

Create value using a unique row ID generator.

    model:
      tables:
        ...
        columns:
        - name: "id"
          gen:
            type: "unordered"
            from: 1
            to: 100
            step: 1
            batchSize: 32

| Field Name | Optional | Default                 | Description                                                                                                                                                                                                                                                                                                     |
|------------|----------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type       | No       | -                       | The row ID generator type, one of:<br/>- sequence: local incrementing sequence <br/>- database_sequence: a named database sequence<br/>- ordered: series of values using `unique_rowid()`<br/>- unordered: series of values using `unordered_unique_rowid()`<br/>- uuid: series of values using `random_uuid()` |
| from       | Yes      | 1                       | Start number for `sequence` type.                                                                                                                                                                                                                                                                               |
| to         | Yes      | 2^63-1 (Long.MAX_VALUE) | Stop number for `sequence` type before rolling over.                                                                                                                                                                                                                                                            |
| step       | Yes      | 1                       | Step increments for `sequence` type.                                                                                                                                                                                                                                                                            |
| sequence   | Yes      | -                       | Sequqnece name for `database_sequence` type.                                                                                                                                                                                                                                                                    |
| batchSize  | No       | -                       | Batch fetch size for all types other than `sequence`.                                                                                                                                                                                                                                                           |

**Remarks:**                                         

All generator types except for `sequence` requires a database connection (lazy opened).

---

##### Each

Create value derived from a referenced table column. Also creates at least one row 
for each row in the referenced table. The `multiplier` is used to generate more rows than one, 
like five orders for each customer or ten order items for each order.

    model:
      tables:
      - name: purchase_order
        columns:
        - name: "customer_id"
          each:
            name: customer
            column: id
            multiplier: 1

| Field Name | Optional | Default | Description                                                    |
|------------|----------|---------|----------------------------------------------------------------|
| name       | No       | -       | Name of the referenced table.                                  |
| column     | No       | -       | Column name in the referenced table.                           |
| multiplier | Yes      | 1       | Number of rows to create for each row in the referenced table. |

**Remarks:**

A table that contains at least one `each` column must not have a [row count](#tables). This is 
because the number of rows in the descendant table depends on the number of rows created 
by the referenced ancestor table.

A table with more than one `each` column creates a cartesian (cross) product. Be aware of the size
and memory implications of cartesian products. All permutations of just a 3-table `100x100x100` 
relation results in 1M rows buffered in memory before streaming.

---

##### Ref

Create a value from a referenced table column.

    model:
      tables:
      - name: purchase_order
        columns:
        - name: "customer_name"
          ref:
            name: customer
            column: name

| Field Name | Optional | Default | Description                                                    |
|------------|----------|---------|----------------------------------------------------------------|
| name       | No       | -       | Name of the referenced table.                                  |
| column     | No       | -       | Column name in the referenced table.                           |

**Remarks:**

If a table contains one or more `each` columns, the value for a `ref` column will be picked from
the same row as the `each` column. If not then it will be picked randomly from a rolling window
of cached rows. 

---

##### Range

Create value from a generated series.

    model:
      tables:
      - name: purchase_order
        columns:
        - name: "order_date"
          range:
            type: date
            from: "2023-12-01"
            to: "2024-12-01"
            step: 1
            step-unit: DAYS

| Field Name | Optional | Default | Description                                                                                                                                                                  |
|------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type       | No       | -       | Range type, one of:<br/>- date: an ISO-8601 local date (yyyy-MM-dd)<br/>- time: an ISO 8601 local time (HH:mm:ss)<br/>- datetime: an ISO-8601 datetime (yyyy-MM-dd HH:mm:ss) |
| from       | Yes      | current | Temporal unit start value.                                                                                                                                                   |
| to         | Yes      | -       | Temporal unit end value before rolling over.                                                                                                                                 |
| step       | Yes      | 1       | Step increments.                                                                                                                                                             |
| step-unit  | No       | -       | Step unit depending on type, must be one of `java.time.temporal.ChronoUnit`                                                                                                  |

**Remarks:** 

Supported step units depend on type used. For example `DAYS` will not work for time values.

---

##### Set

Create random value from a weighted set.

    model:
      tables:
      - name: purchase_order
        columns:
        - name: "order_status"
          set:  
            values: "pending","cancelled","delivered"
            weights: [.25,.25,.50]

| Field Name | Optional | Default | Description                                                       |
|------------|----------|---------|-------------------------------------------------------------------|
| values     | No       | -       | Set of values to randomize across, optionally based on weight.    |
| weights    | Yes      | -       | Weight unit per value. Must be <1 and have seme length as values. |

#### Options (table level)

Defines the CSV delimiter, enclosing character and other import options 
for generated import into SQL file. 

    model:
      ...
      tables:
      - name: orders  
        options:
          delimiter: ","
          fields_enclosed_by: "\""
          ...

| Field Name | Optional | Default | Description                                                                                                                                        |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| options    | No       | -       | List of [IMPORT INTO](https://www.cockroachlabs.com/docs/stable/import-into#import-options) options.                                               |

### Options (top level)

Defines the CSV delimiter, enclosing character and other import options 
for generated import into SQL file. This object can be defined at top-level,
table level and import-into level. Lower level options have precedence over
higher level.

    model:
      ...
      options:
        delimiter: ","
        fields_enclosed_by: "(empty)"
        ...

| Field Name | Optional | Default | Description                                                                                                                                        |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| options    | No       | -       | List of [IMPORT INTO](https://www.cockroachlabs.com/docs/stable/import-into#import-options) options.                                               |

**Remarks:**

Default options include:

| Name               | Value   |
|--------------------|---------|
| delimiter          | ,       |
| fields_enclosed_by | (empty) |

### Import Into 

Defines the CSV delimiter, enclosing character and instructions for the generated 
import into SQL file. 

    model:
      ...
      importInto:
        options:
        file: "import.sql"
        prefix: "http://${local-ip}:8090/"

| Field Name | Optional | Default | Description                                                                                                                                        |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| options    | No       | -       | List of [IMPORT INTO](https://www.cockroachlabs.com/docs/stable/import-into#import-options) options.                                               |
| file       | No       | -       | Name of the crated import SQL file in the `outputPath` dir.                                                                                        |
| prefix     | No       | -       | Import file [location](https://www.cockroachlabs.com/docs/stable/import-into#parameters) prefix. Supports `local-ip` and `public-ip` placeholders. |

#### Options (import into level)

    model:
      ...
      importInto:
        options:
          delimiter: ","
          skip: "1"
          allow_quoted_null: "null"

Defines the CSV delimiter, enclosing character and other import options
for generated import into SQL file. 

Default import into options include:

| Name               | Value   |
|--------------------|---------|
| delimiter          | ,       |
| fields_enclosed_by | (empty) |
| skip               | 1       |
| nullif             | (empty) |
| allow_quoted_null  | (empty) |

# Terms of Use

This tool is not supported by Cockroach Labs. Use of this tool is entirely at your
own risk and Cockroach Labs makes no guarantees or warranties about its operation.

See [MIT](LICENSE.txt) for terms and conditions.
