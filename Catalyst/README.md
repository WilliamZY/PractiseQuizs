# User Documentation

## Dependency:

- from **apt**: python3-pip python3-psycopg2 

  After install, using pip3 to install other dependencies 

- from **pip3**: numpy pandas getopt re sys 

## Assumptions:

- all the records from the csv file are valid, no empty field in any row or column
- both name and surname can have symbols in it
- prefix of a valid email address can only have [0-9a-zA-Z-_+.] in it, without square brackets, The [reference](https://en.wikipedia.org/wiki/Email_address#Syntax)  I looked up when attempting this.

## Design:

- all data read from the csv file will be **slightly processed** to remove ending spaces/tabs
- Error messages are reported to STDOUT
- **--create_table** will only create table and will prevent changes to the user table after rebuilt
- **-h** PostgreSQL host support [host]:[port] format, and will be validated with regular expression, it will only support two formats: IPv4 address with port, or localhost with ports, and port must be a logically valid port.
  - localhost:5432
  - 0.0.0.0:5432
- When there are duplicate email conflicts during insertion, old information like name and surname will be updated by newer information
- Other information are included in the program and can be viewed through '--help'

