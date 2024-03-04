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
Table_Names=["Owner","Apartment","Customer"]


def make_table(tableName,attributes):
    conn = None
    try:
        conn = Connector.DBConnector()
        query="CREATE TABLE "+tableName+"\n("
        for i in range(attributes.len-1):
            query=query+attributes[i]+",\n"
        query = query + attributes[attributes.len-1] +");"
        conn = Connector.DBConnector()
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
        query="CREATE VIEW "+viewName+" as\n"
        for att in attributes:
            query=query+att+",\n"
        query +=";"
        conn = Connector.DBConnector()
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
    attributes=[]
    attributes.append("Owner ID INTEGER UNIQUE CHECK (Owner ID>0)")
    attributes.append("Name STRING NOT NULL")
    make_table("Owner",attributes)

    attributes = []
    attributes.append("Apartment ID INTEGER UNIQUE CHECK (Apartment ID>0)")
    attributes.append("Address STRING NOT NULL")
    attributes.append("City STRING NOT NULL")
    attributes.append("Country STRING NOT NULL")
    attributes.append("Size INTEGER CHECK (Size>0)")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (Address,City,Country)")
    make_table("Apartment", attributes)

    attributes = []
    attributes.append("Customer ID INTEGER UNIQUE CHECK (Customer ID>0)")
    attributes.append("Customer name STRING NOT NULL")
    make_table("Customer", attributes)

    #reservations
    attributes.append("customer_id INTEGER CHECK (customer_id>0)")
    attributes.append("apartment_id INTEGER UNIQUE CHECK (apartment_id>0)")
    attributes.append("start_date DATE")
    attributes.append("end_date DATE")
    attributes.append("total_price INTEGER CHECK (total_price>0)")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (customer_id,apartment_id)")
    make_table("Reservations", attributes)


    #some view related to owner_reservation

    pass


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        for tab in Table_Names:
            conn.execute("DELETE FROM "+tab+";")
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
            conn.execute("DROP TABLE " + tab + ";")
        conn.execute("DROP VIEW Owner_reviews;")
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

#temp func to make review_owner tab
def make_owner_reviews():
    attributes = []
    attributes.append("SELECT Apartment_ID,Owner_ID,rating ")
    attributes.append("FROM Owner_Apartments LEFT OUTER JOIN Apartment_Reviews ")
    attributes.append("ON Owner_Apartments.Apartment_ID=Apartment_Reviews.Apartment_ID")
    make_table("Owner_reviews", attributes)

def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owners(id,name) VALUES({id},{owenrname})").format(
            id=sql.Literal(owner.get_owner_id()), owenrname=sql.Literal(owner.get_owner_name()))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Owners WHERE id=?", (owner_id))
    except DatabaseException:
        return Owner.bad_owner()
    finally:
        conn.close()
        return result


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owners WHERE id={0}").format(sql.Literal(owner_id))
        conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK

    pass


def add_apartment(apartment: Apartment) -> ReturnValue:
    # TODO: implement
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Apartments(id,owner_id,adress,city,country,size) VALUES({id},{owner_id},{adress},{city},{country},{size})").format(
            id=sql.Literal(apartment.get_id()), owner_id=sql.Literal(apartment.get_owner_id()),
            adress=sql.Literal(apartment.get_adress()), city=sql.Literal(apartment.get_city()),
            country=sql.Literal(apartment.get_country()), size=sql.Literal(apartment.get_size()))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK


def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Apartments WHERE id=?", (apartment_id))
    except DatabaseException:
        return Apartment.bad_apartment()
    finally:
        conn.close()
        return result


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartments WHERE id={0}").format(sql.Literal(apartment_id))
        conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Customers(id,name) VALUES({id},{customername})").format(
            id=sql.Literal(customer.get_customer_id()), customername=sql.Literal(customer.get_customer_name()))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Customers WHERE id=?", (customer_id))
    except DatabaseException:
        return Customer.bad_customer()
    finally:
        conn.close()
        return result


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customers WHERE id={0}").format(sql.Literal(customer_id))
        conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK

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


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    conn = None
    if total_price < 0 or start_date > end_date:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Reservations(customer_id,apartment_id,start_date,end_date,total_price) VALUES({customer_id},{apartment_id},{start_date},{end_date},{total_price})").format(
            customer_id=sql.Literal(customer_id), apartment_id=sql.Literal(apartment_id),
            start_date=sql.Literal(start_date), end_date=sql.Literal(end_date), total_price=sql.Literal(total_price))
        rows_affected = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        # check if the apartment is available
        if rows_affected == 0:
            return ReturnValue.BAD_PARAMS
        conn.close()
        return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = None
    try:
        make_owner_reviews()
        conn = Connector.DBConnector()
        query="(SELECT AVG(Rating) from Owner_Reviews WHERE Apartment_ID=apartment_id)"
        res=conn.execute(query)
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


def get_owner_rating(owner_id: int) -> float:
    conn = None
    try:
        make_owner_reviews()
        conn = Connector.DBConnector()
        sub_query="(SELECT AVG(COALESCE(Rating,0)) from Owner_Reviews GROUP BY Apartment_ID)"
        query = "(SELECT AVG"+ sub_query
        res=conn.execute(query)
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


def get_top_customer() -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        sub_query="(SELECT customer_id,COUNT(apartment_id) AS customer_count FROM Reservations GROUP BY customer_id)"
        sub_query2="(SELECT customer_id,MAX(customer_count) FROM "+sub_query+")"
        res = conn.execute("SELECT * from Customer WHERE ")
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
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass