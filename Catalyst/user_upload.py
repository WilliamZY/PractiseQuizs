# -*- coding:UTF-8 -*-

import psycopg2
import sys, re, getopt
import pandas as pd


def create_table(pg_username, pg_userpassword, pg_host):
    # Connect to Postgres
    # pg_connect = psycopg2.connect(
    #     database="postgres",
    #     user=pg_username,
    #     password=pg_userpassword,
    #     host=pg_host,
    #     port=pg_port,
    # )
    pg_connect = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="123456",
        host="localhost",
        port="54322",
    )
    pg_connect.autocommit = True
    # create a cursor object
    cursor = pg_connect.cursor()
    cursor.execute("DROP TABLE IF EXISTS users;")

    cursor.execute(
        "create table users ( name varchar, surname varchar, email varchar(128) not null constraint users_pkey primary key);"
    )

    cursor.execute("alter table users owner to postgres;")
    cursor.execute("create unique index users_email_uindex on users (email);")


def showhelp():
    print()
    print(
        "Usage: user_upload.py -u [username] -p [password] -h [host]:[port] -file [csv filename] --create_table \n"
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
    print("""      This will create the users table in PostgreSQL Database""")
    print("""--help """)
    print("""      This will output the above list of directives""")

    print("Examples: ")
    print("user_upload.py -u username -p password -h 0.0.0.0:5432 --create_table")
    print(
        "user_upload.py -u username -p password -h 0.0.0.0:5432 --create_table -file users.csv --dry_run"
    )
    print("user_upload.py --help")


def main():
    # Parse cli arguments
    try:
        opts, args = getopt.getopt(
            sys.argv[1:], "u:p:h:", ["file=", "create_table", "dry_run", "help"]
        )
    except getopt.GetoptError as err:
        sys.stdout.write("Invalid email found: %s \n" % str(err).upper())
        return
    for opt, arg in opts:
        if opt in ["-u"]:
            pg_username = arg
        elif opt in ["-p"]:
            pg_userpassword = arg
        elif opt in ["-h"]:
            if ":" in arg:
                pg_host, pg_port = arg.split(":")
        elif opt in ["--create_table"]:
            create_table()
        elif opt in ["--file"]:
            input_csv = arg
        elif opt in ["--dry_run"]:
            dryrun = True
        elif opt in ["--help"]:
            showhelp()


if __name__ == "__main__":
    main()

# email validation
validemail_regex = re.compile(r"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")


def insertToDB(dataFrame, index, dryrun=False):
    cursor.execute(
        "INSERT INTO users (name, surname, email) VALUES (%s,%s,%s) ON CONFLICT (email) DO UPDATE SET Name= Excluded.Name;",
        (
            dataFrame.name[index].title(),
            dataFrame.surname[index].title(),
            dataFrame.email[index].lower().replace(" ", ""),
        ),
    )


def parseCSV(input_csv, dryrun=False):
    df = pd.read_csv(
        input_csv,
        index_col=None,
        # header=None
    )
    df.columns = ["name", "surname", "email"]

    for index, row in df.iterrows():
        if not validemail_regex.match(df.email[index].replace(" ", "")):
            sys.stdout.write("Invalid email found: %s \n" % df.email[index])
        else:
            insertToDB(df, index)


# cursor.execute("select version()")
# use fetchone()to fetch single line
data = cursor.fetchone()
print("pg_Connectection established to: ", data)
# Close connection
pg_connect.close()
