# created a database using pgAdmin4 to insert the necessary data

# Load DB config from separate JSON file

## read data from the json file and removed duplicate data to insert into the table

###  Connected to PostgreSQL using a seperate json file to store the db details

#### Create 'voters' table with 'voter_id' as the PRIMARY KEY

##### Insert each unique record — duplicates (based on voter_id) are skipped

