[![Java CI with Maven](https://github.com/kai-niemi/volt/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/kai-niemi/volt/actions/workflows/maven.yml)
       
<!-- TOC -->
* [Getting Started](#getting-started-)
  * [Prerequisites](#prerequisites)
  * [Install the JDK](#install-the-jdk)
  * [Build and Run](#build-and-run)
* [How to Use](#how-to-use)
  * [Generate Configuration](#generate-configuration)
    * [Preparation](#preparation)
    * [Generating](#generating)
* [Configuration](#configuration)
  * [Model](#model)
    * [Tables](#tables)
      * [Columns](#columns)
        * [Row Id](#row-id)
        * [Each](#each)
        * [Ref](#ref)
        * [Range](#range)
        * [Set](#set)
    * [Import](#import)
* [Terms of Use](#terms-of-use)
<!-- TOC -->

# Volt 

<img align="left" src="logo.png" width="128" /> Volt is a simple CSV file generator targeting 
CockroachDB imports for load testing. It can generate very large CSV files based on YAML 
configurations, which in turn can be generated using database schema introspection. 

The memory footprint is very low since it doesn't build state during CSV creation, but 
instead uses pub/sub and bounded ring buffers for related tables (CSVs referring anothers by id). 
The only exception being [cartesian / cross product](https://en.wikipedia.org/wiki/Join_(SQL)#Cross_join) table relations where the aggregation 
needs to be done in-memory (see [Each](#each)).

The tool comes with a command-line interface, an interactive shell and can also operate in 
HTTP proxy mode to support CockroachDB `IMPORT INTO` commands consuming the generated files.

Main use cases:

- Generate CSV files for functional testing and load testing
- Support `IMPORT INTO` command via HTTP endpoint
- Merge-sort very large CSV files

# Getting Started 

Either download the release jar or build it locally (next section).

## Prerequisites

- Java 17+ JRE
- CockroachDB 23.2+
  - https://www.cockroachlabs.com/docs/releases/

## Run

Run the app with:

    java -jar volt.jar

Type `help` for further guidance.

# Building 

## Prerequisites

- Java 17 (or later) JDK
    - https://openjdk.org/projects/jdk/17/
    - https://www.oracle.com/java/technologies/downloads/#java17
- CockroachDB 23.2+ 
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

    git clone git@github.com:kai-niemi/volt.git volt
    cd volt

Build a single, executable JAR:

    ./mvnw clean install

Run the app:

    java -jar target/volt.jar

Type `help` for further guidance.

# How to Use

This tutorial shows how you can generate import CSV files and SQL commands inferred from an existing
database schema. 

## Generate Configuration

To create a configuration YAML file, you can either copy and modify a [template](template/) file or generate
one from an existing database schema. The generated files can be further fine-tuned to use proper randomization
functions and more.

### Preparation

First create a `volt` database with a sample schema using [create-default.sql](samples/create-default.sql):

    cockroach sql --insecure --host=localhost -e "CREATE database volt"
    cockroach sql --insecure --host=localhost --database volt < samples/create-default.sql

### Generating

Next, let's create a configuration YAML file for the schema using the shell `db-export` command followed
by a `quit` that exits the app:

    echo "db-export" > cmd.txt
    echo "quit" >> cmd.txt
    java -jar target/volt.jar @cmd.txt

Check out the YAML file:
    
    cat output/application-default.yml

It should print content similar to [application-default.yml](samples/application-default.yml).

Example:

    model:
      outputPath: ".output"
      tables:
      - name: "customer"
        count: "100"
        files: 1
        columns:
        - name: "id"
          gen:
            type: "unordered"
            batchSize: 512
        - name: "email"
          expression: "randomEmail()"
        - name: "name"
          expression: "randomFullName()"
        - name: "password"
          expression: "randomString(128)"
        - name: "address1"
          expression: "randomString(255)"
        - name: "address2"
          expression: "randomString(255)"
        - name: "postcode"
          expression: "randomString(16)"
        - name: "city"
          expression: "randomCity()"
        - name: "country"
          expression: "randomCountry()"
        - name: "updated_at"
          expression: "randomDateTime()"
          ...

This sample configuration will create three separate CSV files with 100 customers, 100 orders and 100 order items. 
It will also create an `.output/import.sql` file with `IMPORT INTO` SQL statements in toplogical order 
inferred from the foreign key relationships.

Example:

    IMPORT INTO customer(id,email,name,password,address1,address2,postcode,city,country,updated_at)
    CSV DATA (
     'http://192.168.1.113:8090/customer.csv'
    ) WITH delimiter = ',', skip = '1', nullif = '', allow_quoted_null;
    
    IMPORT INTO purchase_order(id,bill_address1,bill_address2,bill_city,bill_country,bill_postcode,bill_to_name,deliv_address1,deliv_address2,deliv_city,deliv_country,deliv_postcode,deliv_to_name,date_placed,status,amount,currency,version,customer_id)
    CSV DATA (
     'http://192.168.1.113:8090/purchase_order.csv'
    ) WITH delimiter = ',', skip = '1', nullif = '', allow_quoted_null;
    
    IMPORT INTO purchase_order_item(order_id,product_id,quantity,status,amount,currency,weight_kg,item_pos)
    CSV DATA (
     'http://192.168.1.113:8090/purchase_order_item.csv'
    ) WITH delimiter = ',', skip = '1', nullif = '', allow_quoted_null;

Next, let's generate all these files using `csv-generate`:

    echo "csv-generate" > cmd.txt
    echo "quit" >> cmd.txt
    java -jar target/volt.jar @cmd.txt

The `.output` directory will now have:

    customer.csv
    purchase_order.csv
    purchase_order_item.csv
    import.sql
    application-default.yml
                                                                     
Lastly, we can run in http proxy mode and let CockroachDB run the actual import:

    echo "db-exec --sql .output/import.sql" > cmd.txt
    echo "quit" >> cmd.txt
    java -jar target/volt.jar --proxy @cmd.txt
                                                               
Optionally, you can launch the interactive shell rather than use a command file:

    java -jar target/volt.jar --proxy
  
# Configuration

Reference section for the application YAML. 
            
## Model

Top-level entry in `application<-profile>.yml`. 

    model:
      outputPath: ".output"
      tables:
      import:

| Field Name | Optional | Default | Description                                   |
|------------|----------|---------|-----------------------------------------------|
| outputPath | Yes      | .output | Output directory for generated files.         |
| tables     | No       | -       | Collection of tables to create CSV files for. |
| import     | No       | -       | Import SQL file settings.                     |

### Tables

Collection of tables where one table map to one CSV file. Table can be related to other tables 
using `each` or `ref` columns. If such relations exist, volt will buffer ancestor table rows
in memory while generating descendant rows (using bounded cyclic buffers). 

    model:
      tables:
      - name: customer
        count: "100K"
        files: 1
        columns:
          ...

| Field Name | Optional | Default | Description                                                                                                                                                    |
|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name     | No       | -       | Name of the table which maps to CSV file name.                                                                                                                 |
| count    | Yes      | "100"   | Number of rows to create in multiplier syntax (K/M/G suffix like 32G). Refused for tables with `ref` columns since these derive rows from reference(d) tables. |
| files    | Yes      | 1       | Number of files per table. If > 1 then the files will be generated in parallel and get a number suffix in the filenames.                                       |
| columns  | No       | -       | Collection of columns to generate.                                                                                                                             |

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
because the number of rows in the descentant table depends on the number of rows created 
by the referenced anscestor table.

A table with more than one `each` column creates a cartesian (cross) product. Be aware of the size
and memory implications of cartesian products. All permutations of just a 3-table `100x100x100` 
relation results in 1M rows buffered in memory.

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
of cached rows. The size of the queue is 8192 by default and configurable with `application.queue-size`. 

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

### Import

Defines the CSV delimiter, enclosing character and also instructions for the generated import SQL file.

    model:
      ...
      import:
        options:
          delimiter: ","
          skip: "1"
          allow_quoted_null: "null"
        file: "import.sql"
        prefix: "http://${local-ip}:8090/"

| Field Name | Optional | Default | Description                                                                                                                                        |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| options    | No       | -       | List of [IMPORT INTO](https://www.cockroachlabs.com/docs/stable/import-into#import-options) options.                                               |
| file       | No       | -       | Name of the crated import SQL file in the `outputPath` dir.                                                                                        |
| prefix     | No       | -       | Import file [location](https://www.cockroachlabs.com/docs/stable/import-into#parameters) prefix. Supports `local-ip` and `public-ip` placeholders. |

**Remarks:**

Default import options are:

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
