# -*- coding:UTF-8 -*-

import psycopg2
import sys, re, getopt
import pandas as pd

global pg_connect
global dryrun

pg_username, pg_userpassword, pg_host, pg_port = ("", "", "", "")
dryrun = ""


def create_table(pg_username, pg_userpassword, pg_host, pg_port):
    print(dryrun)
    if not dryrun:
        pg_connect = psycopg2.connect(
            database="postgres",
            user=pg_username,
            password=pg_userpassword,
            host=pg_host,
            port=pg_port,
        )
        pg_connect.autocommit = True
        cursor = pg_connect.cursor()
        cursor.execute("DROP TABLE IF EXISTS users;")

        cursor.execute(
            "create table users ( name varchar, surname varchar, email varchar(128) not null constraint users_pkey primary key);"
        )

        cursor.execute("alter table users owner to postgres;")
        cursor.execute("create unique index users_email_uindex on users (email);")
        cursor.close()
        pg_connect.close()
        # cursor.execute("select version()")
        # use fetchone()to fetch single line
        # data = cursor.fetchone()
        # print("pg_Connectection established to: ", data)


def showhelp():
    print()
    print(
        "Usage: user_upload.py -u [username] -p [password] -h [host]:[port] --file [csv filename] --create_table \n"
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
        "user_upload.py -u username -p password -h 0.0.0.0:5432 --create_table --file users.csv --dry_run"
    )
    print("user_upload.py --help")


# # Connect to Postgres
# pg_connect = psycopg2.connect(
#     database="postgres",
#     user=pg_username,
#     password=pg_userpassword,
#     host=pg_host,
#     port=pg_port,
# )
# # pg_connect = psycopg2.connect(
# #     database="postgres",
# #     user="postgres",
# #     password="123456",
# #     host="localhost",
# #     port="54322",
# # )

# # create a cursor object
# cursor = pg_connect.cursor()
# # Close connection
# pg_connect.close()

# email validation
validemail_regex = re.compile(r"^\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")


def insertToDB(pg_username, pg_userpassword, pg_host, pg_port, dataFrame, index):
    print(dryrun)
    if not dryrun:
        Connect to Postgres
        pg_connect = psycopg2.connect(
            database="postgres",
            user=pg_username,
            password=pg_userpassword,
            host=pg_host,
            port=pg_port,
        )
        pg_connect.autocommit = True
        # create a cursor object
        cursor = pg_connect.cursor()
        cursor.execute(
            "INSERT INTO users (name, surname, email) VALUES (%s,%s,%s) ON CONFLICT (email) DO UPDATE SET Name= Excluded.Name;",
            (
                dataFrame.name[index].title(),
                dataFrame.surname[index].title(),
                dataFrame.email[index].lower().replace(" ", ""),
            ),
        )
        # Close connection
        cursor.close()
        pg_connect.close()


def parseCSV(input_csv):
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
            if not dryrun:
                insertToDB(pg_username, pg_userpassword, pg_host, pg_port, df, index)


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
        elif opt in ["--dry_run"]:
            dryrun = True
        elif opt in ["--create_table"]:
            create_table(pg_username, pg_userpassword, pg_host, pg_port)
        elif opt in ["--file"]:
            input_csv = arg
            parseCSV(input_csv)
        elif opt in ["--help"]:
            showhelp()


if __name__ == "__main__":
    main()
