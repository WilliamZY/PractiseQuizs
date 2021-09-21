# -*- coding:UTF-8 -*-

import psycopg2
import sys, re
import pandas as pd


def create_table(pg_username, pg_userpassword, pg_host):
    # Connect to Postgres
    # pg_connect = psycopg2.connect(
    #     database="postgres",
    #     user=pg_username,
    #     password=pg_userpassword,
    #     host=pg_host.split(":")[0],
    #     port=pg_host.split(":")[1],
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


def main():
    # Parse cli arguments
    argList = []
    for i in range(len(sys.argv) - 1):
        argList.append(sys.argv[i + 1])
    # print(f"Arguments of the script : {sys.argv[1:]=}")
    for arg in argList:
        if "--file" in arg:
            input_csv = arg.split("=")[-1]
            parseCSV(input_csv)

        if "--create_table" in arg:
            create_table()  # Todo

        if "--dry_run" in arg:
            dry_run()  # Todo

        if "-u=" in arg:
            pg_username = arg.split("=")[-1]

        if "-p=" in arg:
            pg_userpassword = arg.split("=")[-1]

        if "-h=" in arg:
            pg_host = arg.split("=")[-1]

        if "--help" in arg:
            showhelp()  # Todo


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
