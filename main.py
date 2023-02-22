#Lab3.2 Assignment  
#Python & SQLite3 database application for an airline.

import sqlite3
import pandas

#Initialise airline database in airline file.
connection = sqlite3.connect('airline')

#Initialise a cursor object for executing queries
cursor = connection.cursor()

#Flag for data refresh based on .csv files.
refresh = True


#____________________________________________________________________________________________________
# Initialise and create relational tables of the database, based on initial dataset from local files.

if refresh:
  # Table Aircraft
  cursor.execute("CREATE TABLE IF NOT EXISTS Aircraft (aircraft_id INTEGER PRIMARY KEY AUTOINCREMENT, model VARCHAR(30), manufacturer VARCHAR(30), commissioned DATE)")
  # Table Flights
  cursor.execute("CREATE TABLE IF NOT EXISTS Flights (flight_no INTEGER PRIMARY KEY AUTOINCREMENT, aircraft_id INTEGER, departs_on DATE, source VARCHAR(3), FOREIGN KEY(aircraft_id) REFERENCES Aircraft(aircraft_id), FOREIGN KEY(flight_no) REFERENCES Flight_plans(flight_no))")
  # Table Pilot
  cursor.execute("CREATE TABLE IF NOT EXISTS Pilot (employee_id INTEGER PRIMARY KEY AUTOINCREMENT, firstname VARCHAR(30), lastname VARCHAR(30), dob DATE, started DATE)")
  # Table Pilot_schedules
  cursor.execute("CREATE TABLE IF NOT EXISTS Pilot_schedules (schedule_id INTEGER PRIMARY KEY AUTOINCREMENT, employee_id INTEGER, flight_no INTEGER, FOREIGN KEY(flight_no) REFERENCES Flights(flight_no), FOREIGN KEY(employee_id) REFERENCES Pilot(employee_id))")
  # Table Airports
  cursor.execute("CREATE TABLE IF NOT EXISTS Airports (airport_id VARCHAR(3), city VARCHAR(15), country VARCHAR(20), PRIMARY KEY(airport_id))")
  # Table Flight_plans
  cursor.execute("CREATE TABLE IF NOT EXISTS Flight_plans (flight_no INTEGER, destination VARCHAR(20), PRIMARY KEY(flight_no), FOREIGN KEY(destination) REFERENCES Airports(airport_id))")
  
  # Bulk insert all initial data into the tables using pandas module for utility. Duplicates will # be passed with except block
  try:
    #Flight plans
    data = pandas.read_csv("Flight_plans.csv", names = ['flight_no', 'destination'])
    data.to_sql("Flight_plans", connection, if_exists='append', index=False)
    
    #Airports
    data = pandas.read_csv("Airports.csv", names = ['airport_id', 'city', 'country'])
    data.to_sql("Airports", connection, if_exists='append', index=False)
    
    #Pilot
    data = pandas.read_csv("Pilot.csv", names = ['employee_id', 'firstname', 'lastname', 'dob', 'started'])
    data.to_sql("Pilot", connection, if_exists='append', index=False)
    
    #Aircraft
    data = pandas.read_csv("Aircraft.csv", names = ['aircraft_id', 'model', 'manufacturer', 'commissioned'])
    data.to_sql("Aircraft", connection, if_exists='append', index=False)
    
    #Flights
    data = pandas.read_csv("Flights.csv", names = ['flight_no', 'aircraft_id', 'departs_on', 'source' ])
    data.to_sql("Flights", connection, if_exists='append', index=False)
    
    #Pilot_schedules
    data = pandas.read_csv("Pilot_schedules.csv", names = ['employee_id', 'flight_no'])
    data.to_sql("Pilot_schedules", connection, if_exists='append', index=False)
  except:
    #Only the missing values from the default dataset will be added, duplicates are passed.
    pass


#_____________________________________________________________________________________________  
#Methods that provide core application capability

#Get the names of all tables
def get_tables():
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
  table_names = cursor.fetchall()
  return table_names
  
#Get all attributes of all tables to provide the schema for writing queries.
def schema():
  print("See database schema for: Airline\n")
  tables = get_tables()
  for table in tables:
    if table[0] == "sqlite_sequence":
      continue
    print("Table: ", table[0])
    print(headers_output(table[0]))
    print("\n")

#Method to get the column headers of a table in a printable string for output.
def headers_output(table):
  result = ""
  headers = get_headers(table)
  for h in headers:
    result += h
    result += " | "
  return result

#Method to get list of headers for queries and transactions.
def get_headers(table):
  cursor.execute("SELECT * FROM {t}".format(t=table))
  headers = cursor.description
  result = []
  for h in headers:
    result.append(h[0])
  return result

#Method to present output of a list of tuples
def present_query_result(list):
  for row in list:
    print(*row, sep = "\t")
  print("\n")

#Method to display the menu options
def serve_menu():
  print("""
  Enter 1 to view the database schema.
  Enter 2 to search.
  Enter 3 to add a record.
  Enter 4 to update a value.
  Enter 5 to delete a record.
  Enter 6 for statistics.
  Enter 7 for Database dump.
  Enter 8 to enter a custom query.
  Enter 9 to exit.\n""")

#Method to process the commands received in-line with menu options
#Inputs are parsed as strings, match cases against menu methods
def process_command(input):
  match input:
    #Option 1 prints the DB schema
    case "1":
      schema()
    case "2":
      search()
    case "3":
      add_record()
    case "4":
      update_value()
    case "5":
      delete_record()
    case "6":
      statistics()
    case "7":
      db_dump()
    case "8":
      custom_query()
    case "9":
      exit_program()
    case _:
      print("Please enter a valid input.")     


#Method to funnel search requests to appropriate functionality.
def search():
  type = input("Single or multi-tabled search? S/M\n")
  if type == "S":
    single_search()
    
#Method for single-table queries
def single_search():
  print(get_tables(), "\n")
  table = input("Enter table name to search:\n")
  search_by = input("Enter attribute to search by:\n")
  with_value = input("Enter value to search by:\n")
  result = cursor.execute("SELECT * FROM " + table + " WHERE " + search_by + " = '" + with_value + "'").fetchall()
  headers_output(table)
  present_query_result(result)

  
#Method for multi-table queries, to be implemented.

      
#Method to insert a record
def add_record():
  #Display available tables and take input for table to INSERT to.
  print("Available tables to insert a record: \n")
  print(get_tables())
  table = input("Enter table name to insert a record to:\n")

  #Get attributes of table to iterate over and take input for. Use list vals to store inputs           #and insert to table.
  try:
    vals = []
    headers = get_headers(table)
    print("The attributes to define are: ", headers)
  except:
    print("\nInvalid table name, please try again.")
    return

  #create a list of values to be inserted, 
  for attribute in headers:
    value = input("\nEnter value for {a}\n".format(a=attribute))
    vals.append(value)
  insert = tuple(vals)
  #Build query to insert record into table using try-except to prevent errors. commit or rollback.
  #Dynamic generation for number of '?' in the query to represent input values.
  #Use slicing to remove the trailing comma
  try:
    cursor.execute("INSERT INTO " + table + " VALUES (" + ('?,' * len(insert))[:-1] + ")", insert)
    connection.commit()
    #Display the table with new data
    show_results = cursor.execute("SELECT * FROM " + table).fetchall()
    print("\n")
    present_query_result(show_results)
  except sqlite3.Error as e:
    print("Invalid record entered, error: ", e)
    
  
#Method to update a value
def update_value():
  #Display available tables and take input for table to update.
  print("Available tables to update a record: \n")
  print(get_tables())
  
  #Get user input for table and attribute to modify.
  try:
    table = input("Enter table name to update a record in:\n")
    headers = get_headers(table)
    primary_key = headers[0]
    print("primary key is: ", primary_key)
    print("The attributes available to update are: ", headers)
    attr = input("\nEnter attribute to change: \n")
  except:
    print("\nInvalid table/attribute to update, please try again.")
    return

  change_to = input("Enter the new value for the attribute: \n")
  find = input("Enter primary key value of record to update: \n")
  try: 
    cursor.execute("UPDATE " + table + " SET " + attr + "= '" + change_to + "' WHERE " + primary_key + "=" + find)
    connection.commit()
    #Display the table with new data
    show_results = cursor.execute("SELECT * FROM " + table).fetchall()
    print("\n")
    present_query_result(show_results)
  except sqlite3.Error as e:
    print("Invalid update entered, error: ", e)  

  
#Method to delete a record
def delete_record():
  #Display available tables and take input for table to update.
  print("Available tables to delete a record: \n")
  print(get_tables())
  
  #Get user input for table and attribute to modify.
  try:
    table = input("Enter table name to delete a record from:\n")
    headers = get_headers(table)
    primary_key = headers[0]
    print("primary key is: ", primary_key)
  except:
    print("\nInvalid table selected, please try again.")
    return

  find = input("Enter primary key value of record to delete: \n")
  try: 
    cursor.execute("DELETE FROM " + table + " WHERE " + primary_key + "=" + find)
    connection.commit()
    #Display the table with new data
    show_results = cursor.execute("SELECT * FROM " + table).fetchall()
    present_query_result(show_results)
  except sqlite3.Error as e:
    print("Invalid update entered, error: ", e)  

#Method to provide some basic statistics for the airline database.
def statistics():

  #Statistical Analysis:
  print("Statistics for the airline:\n")
  
  #Track changes per session
  print("total changes to the database this session:\n", connection.total_changes, "\n")

  #Total number of flights scheduled
  total_flights = cursor.execute("SELECT COUNT(flight_no) FROM Flights").fetchall()
  print("Total flights:\n")
  present_query_result(total_flights)
  #print("\n")

  #Total flights out of each destination
  total_out = cursor.execute("SELECT source, COUNT(flight_no) FROM Flights GROUP BY source").fetchall()
  print("Total flights out of each airport:\n")
  present_query_result(total_out)

  #Total flights to each destination
  total_in = cursor.execute("""SELECT destination, COUNT(*) as count
FROM Flight_plans
JOIN Flights ON Flight_plans.flight_no = Flights.flight_no
GROUP BY destination;
""").fetchall()
  print("Total flights into each airport:\n")
  present_query_result(total_in)

  #Total number of aircraft
  total_craft = cursor.execute("""SELECT COUNT(*) FROM Aircraft""").fetchall()
  print("Total number of aircraft in service:\n")
  present_query_result(total_craft)

  #Flights per pilot
  per_pilot = cursor.execute("""SELECT Pilot.firstname, Pilot.lastname, COUNT(Pilot_schedules.flight_no) as flight_count
FROM Pilot
JOIN Pilot_schedules ON Pilot.employee_id = Pilot_schedules.employee_id
GROUP BY Pilot.employee_id;""").fetchall()
  print("flights per pilot:\n")
  present_query_result(per_pilot)

  
#Method to dump all database data to terminal for inspection
def db_dump():
  print("Table: Flight_plans")
  print(headers_output("Flight_plans"))
  present_query_result(cursor.execute("SELECT * FROM Flight_plans").fetchall())

  print("Table: Airports")
  print(headers_output("Airports"))
  present_query_result(cursor.execute("SELECT * FROM Airports").fetchall())

  print("Table: Pilot")
  print(headers_output("Pilot"))
  present_query_result(cursor.execute("SELECT * FROM Pilot").fetchall())

  print("Table: Pilot_schedules")
  print(headers_output("Pilot_schedules"))
  present_query_result(cursor.execute("SELECT * FROM Pilot_schedules").fetchall())

  print("Table: Aircraft")
  print(headers_output("Aircraft"))
  present_query_result(cursor.execute("SELECT * FROM Aircraft").fetchall())

  print("Table: Flights")
  print(headers_output("Flights"))
  present_query_result(cursor.execute("SELECT * FROM Flights").fetchall())


#Method for writing bespoke queries for power users.
def custom_query():
  custom = input("Please enter full SQL query directly, for advanced users only.\n")
  try:
    result = cursor.execute(custom).fetchall()
    present_query_result(result)
  except sqlite3.Error as e:
    print("Invalid query entered, error: ", e)  

  
#Method to exit the program, close the database connection safely and exit python.
def exit_program():
  cursor.close()
  connection.close()
  print("Exiting Skyline Database...")
  quit()
  
#____________________________________________________________________________________________________
#Loop to continually present menu and handle requests until exit command specified.
  
while True:
  
  #Intro message runs for each iteration of interacting with the menu in the loop
  print("\nWelcome to the Skyline Airline Database!\n")
  #Displays the menu items to the console for user selection
  serve_menu()
  #take user input
  command = input("Please enter menu option number...\n ")
  #process user input
  process_command(command)

