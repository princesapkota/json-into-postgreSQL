import json
import psycopg2

#
with open("db_config.json", "r") as cfg_file:
    db_config = json.load(cfg_file)

##
with open("voters_data.json", "r", encoding="utf-8") as data_file:
    data = json.load(data_file)
    unique_data = {entry["voter_id"]: entry for entry in data}.values()

###
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

####
cur.execute("""
    CREATE TABLE IF NOT EXISTS voters (
        voter_id TEXT PRIMARY KEY,
        full_name_nepali TEXT,
        full_name_english TEXT,
        DOB_english TEXT,
        gender TEXT,
        citizenship_no TEXT,
        father_name TEXT,
        mother_name TEXT,
        spouse_name TEXT,
        province TEXT,
        district TEXT,
        house_of_representatives_constituency_no TEXT,
        provincial_assembly_constituency_no TEXT,
        municipality TEXT,
        ward_no TEXT,
        polling_station TEXT,
        serial_no TEXT
    )
""")
conn.commit()
#####
for voter in unique_data:
    try:
        cur.execute("""
            INSERT INTO voters (
                voter_id, full_name_nepali, full_name_english, DOB_english, gender,
                citizenship_no, father_name, mother_name, spouse_name, province,
                district, house_of_representatives_constituency_no,
                provincial_assembly_constituency_no, municipality, ward_no,
                polling_station, serial_no
            ) VALUES (
                %(voter_id)s, %(full_name_nepali)s, %(full_name_english)s, %(DOB_english)s, %(gender)s,
                %(citizenship_no)s, %(father_name)s, %(mother_name)s, %(spouse_name)s, %(province)s,
                %(district)s, %(house_of_representatives_constituency_no)s,
                %(provincial_assembly_constituency_no)s, %(municipality)s, %(ward_no)s,
                %(polling_station)s, %(serial_no)s
            )
            ON CONFLICT (voter_id) DO NOTHING
        """, voter)
    except Exception as e:
        print(f" Failed to insert voter_id {voter['voter_id']}: {e}")

conn.commit()
cur.close()
conn.close()

print(" Unique voter records inserted into the 'voters' table.")
