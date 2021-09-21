#!/usr/bin/python
# -*- coding:UTF-8 -*-
import psycopg2
import sys, re, getopt
import pandas as pd

dryrun = ""
createtable = ""
input_csv = None
validemail_regex = re.compile(r"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")
validhost_regex = re.compile(r"^[0-9]{1,3}(\.[0-9]{1,3}){3}|localhost\:[1-9][0-9]{1,}$")


def showhelp():
    print()
    print(
        "Usage: user_upload.py -u [username] -p [password] -h [host]:[port] --file [csv filename] --create_table --dry_run --help\n"
    )
    print("""-u username """)
    print("""      PostgreSQL username""")
    print("""-p password """)
    print("""      PostgreSQL password""")
    print("""-h host:port """)
    print("""      PostgreSQL host address""")
    print("""--file [csv filename] """)
    print("""      This is the name of the CSV to be parsed""")
    print("""--dry_run """)
    print("""      This will be used with '--file' in case we want to""")
    print("""      run the script but don't want insert the Database""")
    print("""--create_table """)
    print("""      This will ONLY create the users table in PostgreSQL Database""")
    print("""--help """)
    print("""      This will output the above list of directives""")

    print("Examples: ")
    print("user_upload.py -u username -p password -h localhost:5432 --create_table")
    print(
        "user_upload.py -u username -p password -h 0.0.0.0:5432 --create_table --file users.csv --dry_run"
    )
    print("user_upload.py --help")


# Parse cli arguments
try:
    opts, args = getopt.getopt(
        sys.argv[1:], "u:p:h:", ["file=", "create_table", "dry_run", "help"]
    )
except getopt.GetoptError as err:
    sys.stdout.write("Invalid command: %s \n" % str(err).upper())
    sys.stdout.write(
        "Seems like you don't know what to do \nHow  about use '--help' to read the help file?\nExamples: \nuser_upload.py --help"
    )
    sys.exit(2)
for opt, arg in opts:
    if opt in ["-u"]:
        pg_username = arg
    elif opt in ["-p"]:
        pg_userpassword = arg
    elif opt in ["-h"]:
        if not validhost_regex.match(arg):
            sys.stdout.write("Invalid host: %s \n" % arg)
            sys.stdout.write("Please %s!\n" % str("check your input!!").upper())
            sys.exit(2)
        if ":" in arg:
            pg_host, pg_port = arg.split(":")
    elif opt in ["--dry_run"]:
        dryrun = True
    elif opt in ["--create_table"]:
        # Only creates the users table and stop insertion
        createtable = True
    elif opt in ["--file"]:
        input_csv = arg
    elif opt in ["--help"]:
        showhelp()
    else:
        sys.stdout.write(
            "Seems like you don't know what to do \nHow about use '--help' to read the help file?"
        )

if not dryrun:
    try:
        pg_connect = psycopg2.connect(
            database="postgres",
            user=pg_username,
            password=pg_userpassword,
            host=pg_host,
            port=pg_port,
        )
    except (RuntimeError, TypeError, NameError) as err:
        sys.stdout.write("Error: %s \n" % str(err).upper())
        sys.stdout.write("Please %s!\n" % str("check your input!!").upper())
        sys.exit(2)

    pg_connect.autocommit = True
    cursor = pg_connect.cursor()
    if createtable:
        cursor.execute("DROP TABLE IF EXISTS users;")
        cursor.execute(
            "create table users ( name varchar, surname varchar, email varchar(128) not null constraint users_pkey primary key);"
        )
        cursor.execute("alter table users owner to postgres;")
        cursor.execute("create unique index users_email_uindex on users (email);")
        # Stop data insert into users table
        dryrun = True

if input_csv:
    df = pd.read_csv(
        input_csv,
        index_col=None,
    )
    df.columns = ["name", "surname", "email"]

    for index, row in df.iterrows():
        if not validemail_regex.match(df.email[index].replace(" ", "")):
            if not createtable:
                sys.stdout.write("Invalid email found: %s \n" % df.email[index])
        else:
            if not dryrun:
                cursor.execute(
                    "INSERT INTO users (name, surname, email) VALUES (%s,%s,%s) ON CONFLICT (email) DO UPDATE SET Name= Excluded.Name;",
                    (
                        df.name[index].title(),
                        df.surname[index].title(),
                        df.email[index].lower().replace(" ", ""),
                    ),
                )
if not dryrun:
    cursor.close()
    pg_connect.close()
