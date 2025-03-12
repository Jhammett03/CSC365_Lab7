import getpass
import mysql.connector
import pandas as pd
import datetime

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


def make_reservation(conn):
    # Handles user input, finds available rooms, and books a reservation

    print("\n **New Reservation**")

    # 1. Get user input
    first_name = input("First Name: ").strip()
    last_name = input("Last Name: ").strip()
    room_preference = input("Room Code (or 'Any'): ").strip().upper()
    bed_type = input("Bed Type (or 'Any'): ").strip().capitalize()
    start_date = input("Begin Date (YYYY-MM-DD): ").strip()
    end_date = input("End Date (YYYY-MM-DD): ").strip()

    try:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        if start_date >= end_date:
            print("Invalid date range. Start date must be before end date.")
            return
    except ValueError:
        print("Invalid date format.")
        return

    adults = int(input("Number of Adults: "))
    kids = int(input("Number of Children: "))
    total_guests = adults + kids

    print("\nSearching for available rooms...\n")

    # 2. Ensure connection is still open
    if not conn.is_connected():
        print("Database connection lost. Reconnecting...")
        conn.reconnect()

    cursor = conn.cursor(buffered=True)  # Ensure query results are properly fetched

    try:
        # Build the query based on user input with parameterized statements
        query = """
            SELECT r.RoomCode, r.RoomName, r.Beds, r.bedType, r.maxOcc, r.basePrice, r.decor
            FROM jthammet.lab7_rooms r
            WHERE r.maxOcc >= %s
        """
        params = [total_guests]

        # Extend query if room or bed preference specified
        if room_preference != "ANY":
            query += " AND r.RoomCode = %s"
            params.append(room_preference)

        if bed_type != "Any":
            query += " AND r.bedType = %s"
            params.append(bed_type)

        cursor.execute(query, params)  # Execute the query
        rooms = cursor.fetchall()  # Fetch all results

        # 3. Filter rooms based on availability
        available_rooms = []
        for room in rooms:
            room_code = room[0]

            # Check if the room is already booked for the given dates
            cursor.execute("""
                SELECT 1 FROM jthammet.lab7_reservations
                WHERE Room = %s AND (
                    (CheckIn <= %s AND Checkout > %s) OR 
                    (CheckIn < %s AND Checkout >= %s) OR
                    (CheckIn >= %s AND Checkout <= %s)
                )
            """, (room_code, start_date, start_date, end_date, end_date, start_date, end_date))

            cursor.fetchall()  # Ensure results are consumed

            if not cursor.rowcount:
                available_rooms.append(room)

        # 4. If no exact match, suggest alternatives
        if not available_rooms:
            print("No exact matches found. Suggesting similar options...\n")
            cursor.execute("""
                SELECT r.RoomCode, r.RoomName, r.Beds, r.bedType, r.maxOcc, r.basePrice, r.decor
                FROM jthammet.lab7_rooms r
                WHERE r.maxOcc >= %s
                ORDER BY ABS(DATEDIFF(%s, CURDATE())) ASC
            """, (total_guests, start_date))

            # Fetch all alternative rooms
            alternative_rooms = cursor.fetchall()

            # Ensure we correctly slice to the top 5 rooms
            suggested_rooms = alternative_rooms[:5]

            # Debugging: Print what should be only 5 rooms
            print("Debug - Suggested Rooms (should be 5 max):", suggested_rooms)

        else:
            suggested_rooms = available_rooms  # If exact matches are found, use them

        if not suggested_rooms:
            print("No suitable rooms available. Try different dates or preferences.")
            return

        # Ensure we are only printing `suggested_rooms`, not `available_rooms`
        print("Available Rooms:\n")
        for i, room in enumerate(suggested_rooms, start=1):  # Use `suggested_rooms`
            print(f"{i}. {room[1]} ({room[0]}) - {room[2]} beds ({room[3]}) - Max {room[4]} guests - ${room[5]}/night - {room[6]} decor")

        print("\nChoose a room by entering the number, or enter 0 to cancel.")
        try:
            choice = int(input("Selection: "))
            if choice == 0:
                print("Reservation canceled.")
                return
            selected_room = suggested_rooms[choice - 1]  # Ensure we use `suggested_rooms`
        except (ValueError, IndexError):
            print("Invalid choice.")
            return

        # 6. Calculate total cost
        base_rate = float(selected_room[5])
        num_days = (end_date - start_date).days
        num_weekdays = sum(1 for i in range(num_days) if (start_date + datetime.timedelta(days=i)).weekday() < 5)
        num_weekends = num_days - num_weekdays

        total_cost = (num_weekdays * base_rate) + (num_weekends * base_rate * 1.1)
        total_cost = round(total_cost, 2)

        # 7. Confirm & insert reservation
        print("\nReservation Summary")
        print(f"Name: {first_name} {last_name}")
        print(f"Room: {selected_room[1]} ({room_code}) - {selected_room[3]} bed")
        print(f"Dates: {start_date} to {end_date} ({num_days} nights)")
        print(f"Guests: {adults} adults, {kids} children")
        print(f"Total Cost: ${total_cost}")

        confirm = input("\nConfirm booking? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Reservation canceled.")
            return

        cursor.execute("SELECT MAX(CODE) FROM jthammet.lab7_reservations")
        max_code = cursor.fetchone()[0]

        # Ensure a valid starting point
        if max_code is None:
            new_code = 1
        else:
            new_code = max_code + 1  # Increment for uniqueness

        cursor.execute("""
            INSERT INTO jthammet.lab7_reservations (Room, CheckIn, Checkout, Rate, LastName, FirstName, Adults, Kids)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (selected_room[0], start_date, end_date, total_cost, last_name, first_name, adults, kids))

        conn.commit()
        print("Reservation successful!")

    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        cursor.close()  # Close cursor at the very end

def cancel_reservation(conn):
    # Get name of customer to search for reservations
    print("Reservation Cancellation Request:\n")
    firstname = input('Enter Firstname: ').capitalize().strip()
    lastname = input('Enter Lastname: ').capitalize().strip()
    for char in '%_[]^-{}':
        if char in firstname or char in lastname:
            print("Invalid characters used, returning to home.")
            return

    if not conn.is_connected():
        print("Database connection lost. Reconnecting...")
        conn.reconnect()

    cursor = None
    try:
        query = (f"""
            SELECT * 
            FROM jthammet.lab7_reservations AS r 
            WHERE r.firstname = \"{firstname}\" AND lastname = \"{lastname}\" 
            ORDER BY r.checkin""")
        cursor = conn.cursor(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=columns)
        currentreservations = df.to_string(index=False)
        if df.empty:
            print("No reservations found.")

        else:
            while True:
                print(f"\n**Reservations for {firstname} {lastname}**")

                print(currentreservations)
                #Get code of reservation they want to cancel
                selectedres = input("Select the code of the reservation you want to cancel or type EXIT: ").upper()
                for char in "%_[]^-{}":
                    if char in selectedres:
                        print("Invalid characters used, returning home.")
                        return
                #need to check if code is one of their reservations
                if selectedres == 'EXIT':
                    print("Returning to home\n")
                    return
                elif type(selectedres) == int:
                    query = (f"""
                        WITH t1 as (
                            SELECT * 
                            FROM jthammet.lab7_reservations AS r 
                            WHERE r.firstname = \"{firstname}\" AND lastname = \"{lastname}\" 
                            ORDER BY r.checkin
                        ) SELECT t1.code
                            FROM t1
                            WHERE t1.code = {selectedres}""")
                    cursor = conn.cursor(query)
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    if df.empty:
                        print(f"The reservation with code {selectedres} was not found under your name.")
                    else:
                        while True:
                            #Confirm input
                            conf = input("Confirm? (Yes/No):")
                            if conf == 'Yes':
                                print(f"The reservation with code: {selectedres} has been cancelled.")
                                query = (f"""DELETE 
                                            FROM jthammet.lab7_reservations as r
                                            WHERE r.CODE = {selectedres}""")
                                cursor = conn.cursor(query)
                                cursor.execute(query)
                                return
                            elif conf =='No':
                                return
                else:
                    print("Invalid Input, returning to home")
                    return

    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        if cursor:
            cursor.close()

def reservation_info(conn):
    print("\n***RESERVATION INFORMATION***\n")
    fqline = ''
    lqline = ''
    roomqline = ''
    dateqline = ''
    resqline = ''
    firstname = input('Enter Firstname, Leave Blank For Any: ').strip()
    if firstname != '':
        for char in '%_[]^-{}':
            if char in firstname:
                fqline = f"AND r.firstname LIKE \"{firstname}\""
                break
            else:
                fqline = f"AND r.firstname = \"{firstname}\""
    lastname = input('Enter Lastname or Any: ').strip()
    if lastname != '':
        for char in '%_[]^-{}':
            if char in lastname:
                lqline = f"AND r.lastname LIKE \"{lastname}\""
                break
            else:
                lqline = f"AND r.lastname = \"{lastname}\""
    startdate = input('Enter Starting Date or Leave Blank for Any: ').strip()
    for char in "%_[]^-{}":
        if char in startdate:
            print("Invalid characters used, returning home.")
            return
    enddate = input('Enter End Date or Leave Blank for Any: ').strip()
    for char in "%_[]^-{}":
        if char in enddate:
            print("Invalid characters used, returning home.")
            return
    roomcode = input('Enter Room Code or Any: ').strip()
    if roomcode != '':
        for char in '%_[]^-{}':
            if char in roomcode:
                roomqline = f"AND r.room LIKE \"{roomcode}\""
                break
            else:
                roomqline = f"AND r.room = \"{roomcode}\""
    reservationcode = input('Enter Reservation Code or Any: ')
    for char in "%_[]^-{}":
        if char in reservationcode:
            print("Invalid characters used, returning home.")
            return
    if startdate != '' and enddate != '':
        dateqline = f'AND ((r.checkin <= \'{startdate}\' AND r.checkout >= \'{startdate}\') OR (r.checkin <= \'{enddate}\' AND r.checkout >= \'{enddate}\'))'
    elif startdate != '' and enddate == '':
        dateqline = f'AND (r.checkin <= \'{startdate}\' AND r.checkout >= \'{startdate}\')'
    elif startdate == '' and enddate != '':
        dateqline = f'AND (r.checkin <= \'{enddate}\' AND r.checkout >= \'{enddate}\')'
    if reservationcode != '':
        resqline = f'AND r.CODE = \'{reservationcode}\''

    checkifallempty = fqline + lqline + dateqline + roomqline + resqline

    if checkifallempty == '':
        query = """SELECT * FROM jthammet.lab7_reservations"""
    else:
        query = f"""SELECT *
                    FROM jthammet.lab7_reservations as r
                    where 1 = 1 {fqline} {lqline} {dateqline} {roomqline} {resqline}"""

    if not conn.is_connected():
        print("Database connection lost. Reconnecting...")
        conn.reconnect()

    cursor = None
    try:

        cursor = conn.cursor(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=columns)
        if df.empty:
            print("No reservations found.")
        else:
            print('\n' + df.to_string(index=False))
    except mysql.connector.Error as err:
        print(f"Database query error: {err}")
    finally:
        if cursor:
            cursor.close()


if __name__ == "__main__":
    conn = get_db_connection()  # Open connection once at the start

    if conn is None:
        print("Database connection failed. Exiting...")
        exit()

    try:
        while True:
            print("\nOptions:\n1: Rooms and Rates\n2: Reservations\n3: Cancel Reservation\n4: Reservation Info\n0: Exit\n")

            try:
                selection = int(input("Selection: "))
            except ValueError:
                print("Invalid input. Please enter a number.")
                continue  # Restart loop

            if selection == 1:
                get_rooms_and_rates(conn)  # Pass connection (not reopening it)
            elif selection == 2:
                make_reservation(conn)
            elif selection == 3:
                cancel_reservation(conn)
            elif selection == 4:
                reservation_info(conn)
            elif selection == 0:
                print("Exiting program.")
                break 
            else:
                print("Invalid selection. Please choose a valid option.")
    finally:
        conn.close()  #Close connection only when user exits
        print("Database connection closed.")
