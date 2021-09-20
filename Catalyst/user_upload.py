# -*- coding:UTF-8 -*-

import psycopg2
import sys
import pandas as pd
import string, regex

# Read CSV & Capitalise Names
filename = "users.csv"
df = pd.read_csv(
    filename,
    index_col=None,
    # header=None
)
df.columns = ["name", "surname", "email"]

for index, row in df.iterrows():
    print(df.name[index].title(), df.surname[index].title(), df.email[index].lower())
# save df into a Dict
# email validation

# Parse cli arguments
argList = []
for i in range(len(sys.argv) - 1):
    argList.append(sys.argv[i + 1])
# print(f"Arguments of the script : {sys.argv[1:]=}")
# print(f"Arguments of the script : "+str(argList))

for arg in argList:
    if "--file" in arg:
        input_csv = arg.split("=")[-1]

    if "-u=" in arg:
        pg_username = arg.split("=")[-1]

    if "-p=" in arg:
        pg_userpassword = arg.split("=")[-1]

    if "-h=" in arg:
        pg_host = arg.split("=")[-1]

    if "create_table" in arg:
        create_table()  # Todo

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

# cursor.execute("select version()")
# use fetchone()to fetch single line
data = cursor.fetchone()
print("pg_Connectection established to: ", data)
# Close connection
pg_connect.close()
