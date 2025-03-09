import getpass
import mysql.connector
import pandas as pd

def get_db_connection():
    #connect to labthreesixfive db
    user = input("User: ")
    db_password = getpass.getpass()

    try:
        conn = mysql.connector.connect(
            user=user,
            password=db_password,
            host='mysql.labthreesixfive.com',
            database='jthammet'
        )
        print("Successfully Connected to LabThreeSixFive")
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_rooms_and_rates(conn):
    #Fetches and displays room details sorted by popularity using Pandas
    
    query = """
        SELECT 
            r.RoomCode,
            r.RoomName,
            r.Beds,
            r.bedType,
            r.maxOcc,
            r.basePrice,
            r.decor,

            ROUND(
                (SELECT COUNT(*) 
                 FROM jthammet.lab7_reservations res 
                 WHERE res.Room = r.RoomCode 
                   AND res.CheckIn >= (SELECT CURDATE() - 180)
                ) / 180, 2
            ) AS popularity_score,

            (SELECT MIN(CheckIn) 
             FROM jthammet.lab7_reservations res 
             WHERE res.Room = r.RoomCode 
               AND res.CheckIn >= CURDATE()
            ) AS next_available_checkin,

            (SELECT DATEDIFF(MAX(Checkout), MIN(CheckIn)) 
             FROM jthammet.lab7_reservations res 
             WHERE res.Room = r.RoomCode
             GROUP BY res.Room
            ) AS last_stay_length,

            (SELECT MAX(Checkout) 
             FROM jthammet.lab7_reservations res 
             WHERE res.Room = r.RoomCode
             GROUP BY res.Room
            ) AS last_checkout_date

        FROM jthammet.lab7_rooms r
        ORDER BY popularity_score DESC;
    """

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=columns)

        # Handle NULL values gracefully
        df = df.fillna({
            'popularity_score': 0.00,
            'next_available_checkin': 'No Bookings',
            'last_stay_length': 0,
            'last_checkout_date': 'No Bookings'
        })

        if df.empty:
            print("No rooms found.")
        else:
            print("\n**Room List Sorted by Popularity:**")
            print(df.to_string(index=False))  # Display without Pandas index

    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        if cursor:
            cursor.close()  # Close cursor after execution

if __name__ == "__main__":
    conn = get_db_connection()  # Open connection once at the start

    if conn is None:
        print("Database connection failed. Exiting...")
        exit()

    try:
        while True:
            print("\nOptions:\n1: Rooms and Rates\n0: Exit\n")

            try:
                selection = int(input("Selection: "))
            except ValueError:
                print("Invalid input. Please enter a number.")
                continue  # Restart loop

            if selection == 1:
                get_rooms_and_rates(conn)  # Pass connection (not reopening it)
            elif selection == 0:
                print("Exiting program.")
                break  #Exit loop properly
            else:
                print("Invalid selection. Please choose a valid option.")
    finally:
        conn.close()  #Close connection only when user exits
        print("Database connection closed.")
