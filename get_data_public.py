import psycopg2
import re
import csv
import xml.etree.ElementTree as ET
from xml_merge import XMLAuthorReplacer as AuthRep


def list_tables(cur):
    """
    List all tables in the database.

    Parameters:
    cur (psycopg2.cursor): Database cursor object.
    """
    # SQL query to list all tables
    query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name;
    """

    try:
        # Execute the query
        cur.execute(query)
        # Fetch all rows
        rows = cur.fetchall()
        # Print all tables
        for row in rows:
            print(f"Schema: {row[0]}, Table: {row[1]}")
    except psycopg2.Error as e:
        print(f"Error: {e}")


def write_to_csv(cur):
    """
    Write the contents of a specified table to a CSV file.

    Parameters:
    cur (psycopg2.cursor): Database cursor object.
    """
    # Prompt the user to enter the table name
    table_name = input("Enter the table name: ")

    # Validation to ensure the table name is alphanumeric and safe
    if not re.match("^[A-Za-z0-9_]+$", table_name):
        print(
            "Invalid table name. Only alphanumeric characters and underscores are allowed")
        return

    # SQL query to select all data from a specified table
    query = f"SELECT * FROM {table_name};"

    try:
        # Execute the query
        cur.execute(query)
        # Fetch all rows
        rows = cur.fetchall()
        # Get column names
        colnames = [desc[0] for desc in cur.description]

        # Write data to CSV file
        csv_filename = f"{table_name}.csv"
        with open(csv_filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the column names as the first row
            csvwriter.writerow(colnames)
            # Write all rows of data
            csvwriter.writerows(rows)

        print(
            f"Data from table '{table_name}' has been written to {csv_filename}")

        # Ask user to turn CSV file into an XML format
        convert_to_XML = input(
            f"Turn '{csv_filename}' into an XML file? (y/n): ").lower().strip()
        if convert_to_XML == "y":
            convert_csv_to_xml(csv_filename, f"{table_name}.xml")
    except psycopg2.Error as e:
        print(f"Error: {e}")


def convert_csv_to_xml(csv_file, xml_file):
    """
    Convert a CSV file to an XML file.

    Parameters:
    csv_file (str): Name of the CSV file to convert.
    xml_file (str): Name of the output XML file.
    """
    with open(csv_file, 'r') as f:
        csvreader = csv.reader(f)
        header = next(csvreader)
        root = ET.Element('root')

        for row in csvreader:
            item = ET.SubElement(root, 'item')
            for h, v in zip(header, row):
                ET.SubElement(item, h).text = v

    tree = ET.ElementTree(root)
    tree.write(xml_file, encoding="utf-8", xml_declaration=True)
    print(f"CSV file '{csv_file}' has been converted to XML file '{xml_file}'")


def merge_xml(csv_file, xml_file):
    """
    Merge data from a CSV file into an XML file based on a key-value mapping.

    Parameters:
    csv_file (str): Name of the CSV file to use for merging.
    xml_file (str): Name of the XML file to merge data into.
    """
    # Ask for the value, key, original, and novel column names
    key_column = input("What key do you want from the CSV file? ")
    value_column = input("What value do you want from the CSV file? ")
    original_column = input("What is the original column from the XML file? ")
    novel_column = input(
        "What do you want the new merged column to be called? ")

    # Output file name is just the xml_file name + merged
    output_file = f"{xml_file}_merged.xml"

    # Merge data using the XMLAuthorReplacer class
    merging_file = AuthRep(
        csv_file, xml_file, output_file, value_column, key_column,
        original_column, novel_column
    )
    merging_file.replace_ids()
    print(f"File has been merged to {output_file}")


def main():
    """
    Main function to handle user input and database connection.
    """
    # Database connection parameters
    conn = psycopg2.connect(
        host="kvrx-prod.c0fioghlgn3i.us-east-2.rds.amazonaws.com",
        database=input("database name?: "),
        user=input("user name?: "),
        password=input("password?: ")
    )

    # Create a cursor object
    cur = conn.cursor()

    # While loop for choice
    while True:
        choice = int(input(
            """
            1: Print tables in database
            2: Choose a table to turn into a CSV file
            3: Import data from one CSV into another with a corresponding ID
            0: Quit
            Your choice: """))
        if choice == 1:
            list_tables(cur)
        elif choice == 2:
            write_to_csv(cur)
        elif choice == 3:
            table_name = input("What CSV file would you like to pull from? ")
            tree_name = input("What XML file would you like to pull from? ")
            merge_xml(f"{table_name}.csv", f"{tree_name}.xml")
        else:
            break

    print("Done looking at database. <3")

    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
