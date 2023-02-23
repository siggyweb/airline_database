# airline_database
A simple database application for an airline created as part of study on the topic of databases and cloud, using SQLite and python. The CSV files provide initial data for the application and can be replaced or altered as users see fit as long as the same internal structure is maintained.

• The database can be hard reset by setting the flag "reset" to "True" on line 14, which will then drop the existing tables and rebuild them based on the data contained in the CSV files.

• Altering the CSV files will change the baseline of what the database will be reset to, and it is recommended to make new copies when making changes to these files or use source control.

Installation instructions:
Clone the repo to your local machine and use "pipenv install" to install the dependencies to run the program.


Key Operations
The database application for the airline database has the following core features:
• View the schema of the database to inform the user about the items they will need
for transactions.

• Search for a specific tables data

• Add a record to a table (It is the responsibility of the user to ensure appropriate
records are added to maintain consistency. For example, if adding a record to Flights, then a Flight_plan record needs to be added as well as two records to Pilot_Schedules for pilot and co-pilot.)

• Updates for values can be performed for a single cell in a table in the database per transaction. For bulk uploads the data can be added to the relevant tables csv and will be inserted by the script at the beginning of the program which pulls from the csv data every time the code is executed.

• Deletion of a value

• Statistical output

• Database dump to view the entire dataset for manual inspection.

• Custom query entry for DBA/power users to directly interact with the database.
