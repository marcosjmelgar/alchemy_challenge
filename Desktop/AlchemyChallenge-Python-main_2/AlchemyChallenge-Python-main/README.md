# AlkemyChallenge-Python
 Challenge Data Analytics - Python

## 1. Create Virtual ENV

- Create env

```bash
virtualenv env
```

- Activate env

```bash
source env/bin/activate
```
 
## 2. Install libraries

- Create requirements.txt

```bash
touch requirements.txt
```

- Install libraries

```bash
pip3 install -r requirements.txt
```


## 3. Create and connect to the PostgreSQL database server via psql

- Connect to postgres

```bash
sudo -i -u postgres
```

- Access to PostgreSQL

```bash
psql
```

- Create database (db)

```bash
create database gov_db;

    CREATE DATABASE
    postgres=#
```

- List of databases

```bash
\l
```

- Connect to database (db)

```bash
\c gov_db

    You are now connected to database "gov_db" as user "postgres".
```

- List of tables

```bash
\d
```

- If you want to exit postgresql

```bash
exit
```

## 4. Run

- Run on Terminal

```bash
python3 main.py
```
