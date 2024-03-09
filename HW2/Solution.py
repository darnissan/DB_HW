from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------
Table_Names=["Owner","Apartment","Customer","Reservations","Apartment_Reviews","Owner_Apartments"]
Views=["Owner_reviews","Owner_city_apartments","Customer_apartments","Cust1_apartments"
       ,"Apartment_ratios","Apartment_approximations"]


def make_table(tableName,attributes):
    conn = None
    try:
        conn = Connector.DBConnector()
        query="CREATE TABLE IF NOT EXISTS "+tableName+"\n("
        for i in range(len(attributes)-1):
            query=query+attributes[i]+",\n"
        query = query + attributes[len(attributes)-1] +");"
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    pass
def make_view(viewName,attributes):
    conn = None
    try:
        conn = Connector.DBConnector()
        query = "CREATE VIEW " + viewName + " AS\n"
        # Directly concatenate the parts of the SQL query
        query += " ".join(attributes)
        query += ";"
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    pass

def create_tables():
    attributes = []
    attributes.append("Owner_ID INTEGER NOT NULL UNIQUE CHECK (Owner_ID>0)")
    attributes.append("Owner_Name TEXT NOT NULL")
    make_table("Owner", attributes)

    # Apt
    attributes = []
    attributes.append("Apartment_ID INTEGER NOT NULL UNIQUE CHECK (Apartment_ID>0)")
    attributes.append("Address TEXT NOT NULL")
    attributes.append("City TEXT NOT NULL")
    attributes.append("Country TEXT NOT NULL")
    attributes.append("Size INTEGER NOT NULL CHECK (Size>0)")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (Address,City,Country)")
    make_table("Apartment", attributes)

    attributes = []
    attributes.append("Customer_ID INTEGER NOT NULL UNIQUE CHECK (Customer_ID>0)")
    attributes.append("Customer_name TEXT NOT NULL")
    make_table("Customer", attributes)

    # reservations
    attributes = []
    attributes.append("customer_id INTEGER NOT NULL REFERENCES Customer(customer_id) ON DELETE SET NULL ")
    attributes.append("apartment_id INTEGER NOT NULL REFERENCES Apartment(apartment_id) ON DELETE SET NULL ")
    attributes.append("start_date DATE NOT NULL")
    attributes.append("end_date DATE NOT NULL CHECK (end_date > start_date)")
    attributes.append("total_price INTEGER NOT NULL CHECK (total_price > 0)")
    # Remove the UNIQUE constraint on apartment_id as it would prevent more than one reservation per apartment
    # If needed, adjust the unique constraint below to suit your actual requirements
    # attributes.append("CONSTRAINT UC_Reservation UNIQUE (customer_id, apartment_id, start_date, end_date)")
    make_table("Reservations", attributes)

    # reviews
    attributes = []
    attributes.append("customer_id INTEGER REFERENCES Customer(customer_id) ON DELETE SET NULL CHECK (customer_id IS NULL OR customer_id > 0)")
    attributes.append("apartment_id INTEGER REFERENCES Apartment(apartment_id) ON DELETE SET NULL CHECK (apartment_id > 0 OR apartment_id IS NULL)")
    attributes.append("review_date DATE NOT NULL")
    attributes.append("rating INTEGER NOT NULL CHECK (rating > 0 AND rating <= 10)")
    attributes.append("review_text TEXT NOT NULL")
    # Adjust the UNIQUE constraint according to your business rules
    # If allowing multiple reviews by the same customer for the same apartment, consider removing this or adjusting.
    attributes.append("CONSTRAINT UC_Reviews UNIQUE (customer_id, apartment_id)")
    make_table("Apartment_Reviews", attributes)

    # owner and his apartments
    attributes = []
    attributes.append(
        "owner_id INTEGER REFERENCES owner(owner_id) ON DELETE SET NULL CHECK (owner_id IS NULL OR owner_id > 0)"
    )
    attributes.append("apartment_id INTEGER REFERENCES Apartment(apartment_id) ON DELETE SET NULL CHECK (apartment_id > 0 OR apartment_id IS NULL)")
    attributes.append("CONSTRAINT UC_Owner_Apts UNIQUE (owner_id,apartment_id)")
    make_table("Owner_Apartments", attributes)

    pass


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        for tab in Table_Names:
            conn.execute("DELETE FROM "+tab+";")
        for view in Views:
            if (conn.execute("SELECT * FROM pg_views WHERE viewname = '"+view+"';")[0]):
                conn.execute("DELETE FROM "+view+";")
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass


def drop_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        for tab in Table_Names:
            conn.execute("DROP TABLE " + tab + " CASCADE ;")
        for view in Views:
            conn.execute("DROP VIEW IF EXISTS " + view + ";")
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass

# temp func to make review_owner tab
def make_owner_reviews():
    attributes = []
    attributes.append("SELECT Owner_Apartments.Apartment_ID, Owner_ID, rating ")
    attributes.append("FROM Owner_Apartments LEFT OUTER JOIN Apartment_Reviews ")
    attributes.append("ON Owner_Apartments.Apartment_ID = Apartment_Reviews.apartment_id")
    make_view("Owner_reviews", attributes)

def make_owner_city_apartments():
    attributes = []
    attributes.append("SELECT Owner_ID,Name,City ")
    attributes.append("FROM Owner_Apartments INNER JOIN Apartment_Reviews ")
    attributes.append("ON Owner_Apartments.Owner_ID=Apartment_Reviews.Owner_ID ")
    attributes.append("AND Owner_Apartments.Apartment_ID=Apartment_Reviews.Apartment_ID ")
    attributes.append("INNER JOIN Owner ON Owner_Apartments.Owner_ID= Owner.Owner_ID")
    make_view("Owner_city_apartments", attributes)

def make_customer_apartments():
    attributes = []
    attributes.append("SELECT customer_id,COUNT(apartment_id) AS customer_count")
    attributes.append("FROM Reservations GROUP BY customer_id")
    make_view("Customer_apartments", attributes)


def make_customer_ratio(customer_ID):
    attributes = []
    #get apartments for our
    attributes.append("SELECT apartment_id,rating")
    attributes.append("FROM Apartment_Reviews")
    attributes.append("WHERE Apartment_Reviews.customer_id=customer_ID")
    make_view("Cust1_apartments", attributes)



    #get ratios
    attributes = []
    attributes.append("SELECT customer_id,AVG(Cust1_apartments.rating/Apartment_Reviews.rating) AS ratio ")
    attributes.append("FROM Apartment_Reviews GROUP BY customer_id")
    attributes.append("INNER JOIN Cust1_apartments ")
    attributes.append("ON Cust1_apartments.apartment_id=Apartment_Reviews.apartment_id_ID ")
    attributes.append("AND Apartment_Reviews.customer_id !=customer_ID")
    make_view("Apartment_ratios", attributes)

    # get approximations
    attributes = []
    attributes.append("SELECT apartment_id,GREATEST(1,LEAST(10,AVG(ratio * Apartment_Reviews.rating))) ")
    attributes.append("FROM Apartment_Reviews GROUP BY apartment_id")
    attributes.append("INNER JOIN Apartment_ratios ")
    attributes.append("ON Apartment_Reviews.customer_id=Apartment_ratios.customer_id")
    make_view("Apartment_approximations", attributes)

def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owner(Owner_ID,Owner_Name) VALUES({id},{Ownername})").format(
            id=sql.Literal(owner.get_owner_id()),
            Ownername=sql.Literal(owner.get_owner_name()),
        )
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result= ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result=ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result= ReturnValue.ALREADY_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def get_owner(owner_id: int) -> Owner:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query=sql.SQL("SELECT * FROM Owner WHERE owner_id={0}").format(sql.Literal(owner_id))
        rows,owners = conn.execute(query)
        if owners is not None and rows >0 :
            result=Owner(owners[0]['Owner_ID'],owners[0]['Owner_Name'])
        else :
            result = Owner.bad_owner()
    except Exception as e :
        print (e)
        return Owner.bad_owner()
    finally:
        conn.close()
        return result


def delete_owner(owner_id: int) -> ReturnValue:
    # Initial check for a valid owner_id
    if (owner_id <= 0):
        return ReturnValue.BAD_PARAMS

    conn = None
    rows = 0  # Initialize rows affected
    result = ReturnValue.OK  # Assume OK unless an exception occurs

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owner WHERE owner_id={0}").format(
            sql.Literal(owner_id)
        )
        rows, _ = conn.execute(query)  # Execute the delete query
        
        # Check if rows affected is 0, implying owner did not exist
        if rows == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except Exception as e:
        print(e)
        result = ReturnValue.ERROR
    finally:
        if conn is not None:
            conn.close()

    return result


def add_apartment(apartment: Apartment) -> ReturnValue:
    # TODO: implement
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Apartment(Apartment_ID,Address, City,Country,Size) VALUES({id},{address},{city},{country},{size})"
        ).format(
            id=sql.Literal(apartment.get_id()),
            address=sql.Literal(apartment.get_address()),
            city=sql.Literal(apartment.get_city()),
            country=sql.Literal(apartment.get_country()),
            size=sql.Literal(apartment.get_size()),
        )
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Apartment WHERE apartment_id = {0}").format(sql.Literal(apartment_id))
        rows_effected, apts = conn.execute(query)
        if apts is not None and rows_effected > 0:
            result = Apartment(apts[0]['apartment_id'], apts[0]['address'], apts[0]['city'], apts[0]['country'],apts[0]['size'])
        else:
            result = Apartment.bad_apartment()
    except Exception as e:
        print(e)
        result = Apartment.bad_apartment()
    finally:
        conn.close()
    return result


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE apartment_id={0}").format(
            sql.Literal(apartment_id)
        )
        rows_effected,_ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        result =ReturnValue.BAD_PARAMS
    except DatabaseException:
        result = ReturnValue.ERROR
    finally:
        if result == ReturnValue.OK:
            if rows_effected == 0:
                result=ReturnValue.NOT_EXISTS
        conn.close()
        return result


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    result= ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Customer(Customer_ID,Customer_Name) VALUES({id},{customername})"
        ).format(
            id=sql.Literal(customer.get_customer_id()),
            customername=sql.Literal(customer.get_customer_name()),
        )
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result=ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result= ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result=  ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result= ReturnValue.ALREADY_EXISTS
    except Exception as e:
        result= ReturnValue.ERROR
    finally:
        conn.close()
        return result


def get_customer(customer_id: int) -> Customer:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query=sql.SQL("SELECT * FROM Customer WHERE customer_id={0}").format(sql.Literal(customer_id))
        rows_effected,customers = conn.execute(query)
        if customers is not None and rows_effected > 0:
            result = Customer(customers[0]['Customer_ID'],customers[0]['Customer_Name'])
        else :
            result=Customer.bad_customer()
    except Exception as e:
        print(e)
        return Customer.bad_customer()
    finally:
        conn.close()
        return result


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    if (customer_id <= 0):
        return ReturnValue.BAD_PARAMS
    rows=0
    result= ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE customer_id={0}").format(
            sql.Literal(customer_id)
        )
        rows,_ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION as e:
        result= ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        result= ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result= ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result= ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        result= ReturnValue.ERROR
    except Exception as e:
        print(e)
        result= ReturnValue.ERROR
    finally:
        conn.close()
        if (result == ReturnValue.OK):
            if rows==0 :
                result= ReturnValue.NOT_EXISTS
        return result

"""ReturnValue customer_made_reservation(customer_id: int, apartment_id: int, start_date:
date, end_date: date, total_price: float)
Customer made a reservation of apartment from start_date to end_date an paid total_price
Input:
● customer_id: the id of the customer that made the reservation
● apartment_id: the id of the apartment the customer made a reservation at
● start_date: the date the reservation starts at
● end_date: the date the reservation ends at, has to be after start_date
● total_price: the total price of the reservation, has to be positive
Output:
● OK in case of success
● BAD_PARAMS if any of the params are illegal or the apartment isn’t available at the
specified date (there is already a reservation for it)
● NOT_EXISTS if the customer or the apartment don’t exist
● ERROR in case of database error

"""


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:
    if total_price <= 0 or start_date >= end_date:
        return ReturnValue.BAD_PARAMS
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        insert_query = sql.SQL("""
            INSERT INTO Reservations(customer_id, apartment_id, start_date, end_date, total_price)
            SELECT {customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price}
            WHERE NOT EXISTS (
                SELECT 1 FROM Reservations
                WHERE Apartment_ID = {apartment_id} AND (
                    (start_date < {end_date} AND end_date > {start_date})
                )
            );
        """).format(
            customer_id=sql.Literal(customer_id),
            apartment_id=sql.Literal(apartment_id),
            start_date=sql.Literal(start_date),
            end_date=sql.Literal(end_date),
            total_price=sql.Literal(total_price),
        )

        rows_affected = conn.execute(insert_query)
        if rows_affected[0] == 0:
            # Assuming zero rows affected indicates a failed condition check
            result = ReturnValue.BAD_PARAMS
        

    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    
    finally:
        
        if conn is not None:
            conn.close()
        return result


def customer_cancelled_reservation(
    customer_id: int, apartment_id: int, start_date: date
) -> ReturnValue:
    conn = None
    rows_affected = 0
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Reservations WHERE customer_id={0} AND apartment_id={1} AND start_date={2}"
        ).format(
            sql.Literal(customer_id), sql.Literal(apartment_id), sql.Literal(start_date)
        )
        rows_affected,_ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result =  ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result =  ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        if rows_affected == 0:
            if result == ReturnValue.OK:
                result =  ReturnValue.NOT_EXISTS
        conn.close()
        return  result
    pass


def customer_reviewed_apartment(
    customer_id: int,
    apartment_id: int,
    review_date: date,
    rating: int,
    review_text: str,
) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    if rating < 0 or rating > 10:
        result = ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            INSERT INTO Apartment_Reviews(customer_id, apartment_id, review_date, rating, review_text)
            SELECT {customer_id}, {apartment_id}, {review_date}, {rating}, {review_text}
            WHERE EXISTS (
                SELECT 1 FROM Reservations
                WHERE customer_id = {customer_id} AND apartment_id = {apartment_id}
                AND end_date < {review_date}
            ); 
           """).format(
            customer_id=sql.Literal(customer_id),
            apartment_id=sql.Literal(apartment_id),
            review_date=sql.Literal(review_date),
            rating=sql.Literal(rating),
            review_text=sql.Literal(review_text),
        )
        rows_affected,_ = conn.execute(query)
        if rows_affected == 0:
            result = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:

        conn.close()
        return result
    pass


def customer_updated_review(
    customer_id: int,
    apartment_id: int,
    update_date: date,
    new_rating: int,
    new_text: str,
) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    if new_rating < 0 or new_rating > 10:
        result = ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "UPDATE Apartment_Reviews SET rating={0},review_text={1},review_date={4} WHERE customer_id={2} AND apartment_id={3} AND review_date<={4}"
        ).format(
            sql.Literal(new_rating),
            sql.Literal(new_text),
            sql.Literal(customer_id),
            sql.Literal(apartment_id),
            sql.Literal(update_date),
        )
        rows_affected,_ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        if result == ReturnValue.OK :
            if rows_affected == 0:
                result = ReturnValue.NOT_EXISTS
        conn.close()
        return result
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Owner_Apartments(owner_id,apartment_id) VALUES({0},{1})"
        ).format(sql.Literal(owner_id), sql.Literal(apartment_id))
        rows,_ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result =ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result =ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result =ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result =ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    result = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Owner_Apartments WHERE owner_id={0} AND apartment_id={1}"
        ).format(sql.Literal(owner_id), sql.Literal(apartment_id))
        rows_affected,_ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result =  ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result =  ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result =  ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result =  ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        if rows_affected == 0:
            result =  ReturnValue.NOT_EXISTS
        conn.close()
        return result
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        query=sql.SQL("SELECT * FROM Owner WHERE owner_id IN (SELECT owner_id FROM Owner_Apartments WHERE apartment_id={0})".format(
                apartment_id
            )
        )
        rows_effected,owners = conn.execute(query)
        if owners is not None and rows_effected >0 :
            result = Owner(owners[0]['Owner_ID'],owners[0]['Owner_Name'])
        else :
            result = Owner.bad_owner()
    except Exception as e:
        print (e)
        return Owner.bad_owner()
    finally:
        conn.close()
        return result
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        query=sql.SQL("SELECT * FROM Apartment WHERE apartment_id IN (SELECT apartment_id FROM Owner_Apartments WHERE owner_id={0})".format( owner_id))
        rows,apartments  = conn.execute(query)
        if (apartments is not None) and rows >0 :
            for index in range(apartments.size()) :
                current_row = apartments[index]
                result.append(Apartment(current_row['Apartment_ID'],current_row['Address'],current_row['City'],current_row['Country'],current_row['Size']))
    except Exception as e:
        print (e)
        return []
    finally:
        conn.close()
        return result
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = None
    avg_rating =0
    res=None
    try:
        make_owner_reviews()
        make_owner_reviews()
        conn = Connector.DBConnector()
        query = "(SELECT AVG(Rating) FROM Owner_Reviews WHERE Apartment_ID={0})".format(apartment_id)
        _, res = conn.execute(query)
        # Access the first item in the result set which is the tuple containing the Decimal
        avg_rating_decimal = res.rows[0][0]  # This will be a Decimal object

        # Convert Decimal to a float or int (depending on what you need)
        avg_rating = int(avg_rating_decimal)  # For a floating-point number
        # avg_rating = int(avg_rating_decimal)  # If you prefer an integer and the precision allows for it

        # Now avg_rating contains the average rating as a float or an integer

    except DatabaseException.ConnectionInvalid as e:
        res= ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
       res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        res= ReturnValue.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        return avg_rating
    pass

def get_owner_rating(owner_id: int) -> float:
    conn = None
    res = 0
    try:
        make_owner_reviews()
        conn = Connector.DBConnector()
        sub_query = "(SELECT AVG(COALESCE(Rating,0)) AS avg_rating FROM Owner_Reviews WHERE Owner_ID="+str(owner_id)+" GROUP BY Apartment_ID) AS subquery;"
        query = "SELECT AVG(subquery.avg_rating) FROM" + sub_query
        res = float(conn.execute(query)[1][0]['avg'])
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
        return res
    pass


def get_top_customer() -> Customer:
    conn = None
    try:
        make_owner_reviews()
        conn = Connector.DBConnector()
        make_customer_apartments()
        sub_query="(SELECT customer_id,MAX(customer_count) FROM Customer_apartments)"
        sub_query2 = "(SELECT customer_id FROM" + sub_query + " LIMIT 1)"
        res = conn.execute("SELECT * FROM Customer WHERE "+sub_query2+"=customer_id")
        return res
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        return conn.execute("SELECT Name,COUNT(apartment_id) FROM Owner_Reservations GROUP BY apartment_id)")
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # join to get cities for apartments
    conn = None
    try:
        conn = Connector.DBConnector()
        make_owner_city_apartments()
        query=("SELECT Owner_ID,Name FROM Owner_city_apartments GROUP BY name "
              "HAVING COUNT(DISTINCT City) = (SELECT COUNT(DISTINCT City) FROM Owner_city_apartments);")
        return conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass


def best_value_for_money() -> Apartment:
    conn = None
    try:
        query = """
                SELECT a.apartment_id, a.name, MAX(reviews_avg_rating / avg_nightly_price) AS value_for_money
    FROM apartments a
    JOIN (
      SELECT r.apartment_id,
         AVG(r.total_price / NULLIF(julianday(r.end_date) - julianday(r.start_date), 0)) AS avg_nightly_price
      FROM reservations r
      GROUP BY r.apartment_id
    ) AS reservation_prices ON a.apartment_id = reservation_prices.apartment_id
    JOIN (
      SELECT rev.apartment_id,
         AVG(rev.rating) AS reviews_avg_rating
      FROM reviews rev
      GROUP BY rev.apartment_id
    ) AS review_ratings ON a.apartment_id = review_ratings.apartment_id
    GROUP BY a.apartment_id, a.name
    ORDER BY value_for_money DESC
    LIMIT 1;
    """
        conn = Connector.DBConnector()
        result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:

        # will happen any way after code try termination or exception handling
        conn.close()
        return result


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        query = """
                SELECT strftime('%m', end_date) AS month, SUM(total_price * 0.15) AS profit
                FROM reservations
                WHERE strftime('%Y', end_date) = ?
                GROUP BY month
                ORDER BY month;
                """
        conn = Connector.DBConnector()
        result = conn.execute(query, (year,))
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        #make ratio
        make_customer_ratio(customer_id)
        #get approximation
        query =("SELECT * "
                "FROM Apartment INNER JOIN Apartment_approximations"
                "ON Apartment.apartment_id=Apartment_approximations.apartment_id")
        return conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    pass
