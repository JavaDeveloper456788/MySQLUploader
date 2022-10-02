from genericpath import exists
import mysql.connector
import csv

# util function to convert string to int
def get_int(x):
    if x == '':
        return 0
    else:
        return int(x)

def main():
    if not exists("input/database.txt"):
        print("Please copy the database file to the input folder and rename it to 'database.txt'")

    print("database.txt file found.")

    sql_host = input("MySQL Server host: ")
    sql_username = input("MySQL Server Username: ")
    sql_password = input("MySQL Server Password: ")
    sql_database = input("MySQL Server Database Name: ")

    # connecting to mysql server
    cnx = mysql.connector.connect(
        user=sql_username,
        password=sql_password,
        host=sql_host,
        database=sql_database)

    cursor = cnx.cursor()

    # sql query
    truncate_statement = "TRUNCATE TABLE carsdb;"
    insert_statement = (
        "INSERT INTO carsdb(vin, year, make, model, trim, dealer_name, dealer_street, dealer_city, dealer_state, dealer_zip, listing_price, listing_mileage, used, certified, style, driven_wheels, engine, fuel_type, exterior_color, interior_color, seller_website, first_seen_date, last_seen_date, dealer_vdp_last_seen_date, listing_status)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    print("Processing CSV/TXT Database file.")

    cursor.execute(truncate_statement)
    cnx.commit()

    # data import loop
    with open("input/database.txt", mode='r', encoding="utf-8") as csv_data:
        reader = csv.reader(csv_data, delimiter='|')
        next(reader)
        csv_data_list = list(reader)
        print("Starting import job, This will take a long time. DO NOT EXIT.")
        for row in csv_data_list:
            cursor.execute(insert_statement, [row[0], get_int(row[1]), row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], get_int(row[10]), get_int(
                row[11]), row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24]])
    
    cnx.commit()
    cursor.close()
    cnx.close()

    print("THE JOB IS DONE.")


# application entry point
if __name__ == "__main__":
    main()
