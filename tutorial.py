import psycopg2
from fake_data import generate_fake_data
records = tuple(generate_fake_data(100))

try: 
    print('start here')
    with psycopg2.connect(dbname='aerielellis', user='aerielellis', password='password', host='localhost', port=5432) as db_connection:
        print("successfully connected to the database")
        # don't have to do an explicit commit anywhere else
    db_connection.autocommit = True
    db_cursor = db_connection.cursor()
    
    
    create_table= '''CREATE TABLE IF NOT EXISTS people(
        id SERIAL PRIMARY KEY,
        name varchar(50) NOT NULL,
        city varchar(40),
        profession varchar(60)
        );'''
    db_cursor.execute(create_table)
    
    
    # inserting some records from the faker data
    insert_record = "INSERT INTO people(name, city, profession) \
        VALUES(%s,%s,%s);"
    for record in records:
        db_cursor.execute(insert_record,record)
    print("successfully added to the database")
    
    # going to see how many cities appear more than once
    get_count ='''SELECT city, COUNT(*)
              FROM people
              GROUP BY city HAVING COUNT(*)>1;'''
    db_cursor.execute(get_count)
    print(db_cursor.fetchall())

    # Update records
        
    update_query = 'UPDATE people SET city=%s WHERE city=%s;'
    values = ('Mathville','Johnsonmouth')
    db_cursor.execute(update_query,values)
            
    # Delete records
        
    delete_record = 'DELETE FROM people WHERE city=%s;'
    record = ('Mathville',)
    db_cursor.execute(delete_record,record)    

except psycopg2.OperationalError:
    print("Error connecting to the database!!! ")
    
finally:
    # close the connection to the database
    if db_connection:
        db_connection.close()
        print("Closed connection.")