# Migration RDS to CloudSql

This code automatize the process of migrate several aws rds instances to gcp cloudsql. From a CSV file it generates a migration job by each row in gcp dms to create a cloud sql instance and create a cdc process to migrate data from rds.

![alt text](migration_file_example.JPG)

## Installation

```bash
pipenv install
```

## Usage

```bash
python poc-migration-auto.py --migration-file "migration_file.csv"
```

## License
[MIT](https://choosealicense.com/licenses/mit/)