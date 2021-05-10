# Migration RDS to CloudSql

This code automatize the process of migrate several aws rds instances to gcp cloudsql. 

From a CSV file it generates, by each row, a migration job in gcp dms to create a cloud sql instance and create a cdc process to migrate data from rds.

Parameter input: "migration_file.csv"

![alt text](migration_file_example.JPG)

## Installation

```bash
pipenv install
```

## Usage

```bash
python poc-migration-auto.py --migration-file "migration_file.csv"
```

### Optional arguments:

#### Parameter –-migration-file

Indicates the file name where there are the credentials of rds instances.

## License
[MIT](https://choosealicense.com/licenses/mit/)