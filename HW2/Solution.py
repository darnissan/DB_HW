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
        query="CREATE TABLE "+tableName+" \n ( "
        query += ",\n".join(attributes)
        query +=");"
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
    attributes.append("Owner_ID INTEGER UNIQUE CHECK (Owner_ID>0)")
    attributes.append("Name TEXT NOT NULL")
    make_table("Owner",attributes)

    attributes = []
    attributes.append("Apartment_ID INTEGER UNIQUE CHECK (Apartment_ID>0)")
    attributes.append("Address TEXT NOT NULL")
    attributes.append("City TEXT NOT NULL")
    attributes.append("Country TEXT NOT NULL")
    attributes.append("Size INTEGER CHECK (Size>0)")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (Address,City,Country)")
    make_table("Apartment", attributes)

    attributes = []
    attributes.append("Customer_ID INTEGER UNIQUE CHECK (Customer_ID>0)")
    attributes.append("Customer_name TEXT NOT NULL")
    make_table("Customer", attributes)

    #reservations
    attributes = []
    attributes.append("customer_id INTEGER CHECK (customer_id>0)")
    attributes.append("apartment_id INTEGER UNIQUE CHECK (apartment_id>0)")
    attributes.append("start_date DATE")
    attributes.append("end_date DATE")
    attributes.append("total_price INTEGER CHECK (total_price>0)")
    attributes.append("CONSTRAINT Customer UNIQUE (customer_id,apartment_id)")
    make_table("Reservations", attributes)

    attributes = []
    attributes.append("customer_id INTEGER CHECK (customer_id>0)")
    attributes.append("apartment_id INTEGER UNIQUE CHECK (apartment_id>0)")
    attributes.append("review_date DATE")
    attributes.append("rating INTEGER CHECK (rating>0 AND rating<11)")
    attributes.append("review_text TEXT")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (customer_id,apartment_id)")
    make_table("Apartment_Reviews", attributes)
    
    #make the table of owner and his apartments
    attributes = []
    attributes.append("owner_id INTEGER CHECK (owner_id>0)")
    attributes.append("apartment_id INTEGER UNIQUE CHECK (apartment_id>0)")
    attributes.append("CONSTRAINT UC_Apartment UNIQUE (owner_id,apartment_id)")
    make_table("Owner_Apartments", attributes)
    clear_tables()


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


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owner(id,name) VALUES({id},{owenrname})").format(
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
            "INSERT INTO Apartment(id,owner_id,adress,city,country,size) VALUES({id},{owner_id},{adress},{city},{country},{size})").format(
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
        result = conn.execute("SELECT * FROM Apartment WHERE id=?", (apartment_id))
    except DatabaseException:
        return Apartment.bad_apartment()
    finally:
        conn.close()
        return result


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE id={0}").format(sql.Literal(apartment_id))
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
    result =ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Customer(Customer_ID,Customer_name) VALUES({id},{customername})").format(
            id=sql.Literal(customer.get_customer_id()), customername=sql.Literal(customer.get_customer_name()))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result


def get_customer(customer_id: int) -> Customer:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Customer WHERE id=?", (customer_id))
    except DatabaseException:
        return Customer.bad_customer()
    finally:
        conn.close()
        return result


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE id={0}").format(sql.Literal(customer_id))
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Reservations WHERE customer_id={0} AND apartment_id={1} AND start_date={2}").format(
            sql.Literal(customer_id), sql.Literal(apartment_id), sql.Literal(start_date))
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
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    conn = None
    if rating < 0 or rating > 10:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Apartment_Reviews(customer_id,apartment_id,review_date,rating,review_text) VALUES({customer_id},{apartment_id},{review_date},{rating},{review_text})").format(
            customer_id=sql.Literal(customer_id), apartment_id=sql.Literal(apartment_id),
            review_date=sql.Literal(review_date), rating=sql.Literal(rating), review_text=sql.Literal(review_text))
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
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    conn = None
    if new_rating < 0 or new_rating > 10:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "UPDATE Apartment_Reviews SET rating={0},review_text={1} WHERE customer_id={2} AND apartment_id={3} AND review_date={4}").format(
            sql.Literal(new_rating), sql.Literal(new_text), sql.Literal(customer_id), sql.Literal(apartmetn_id),
            sql.Literal(update_date))
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
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owner_Apartment(owner_id,apartment_id) VALUES({0},{1})").format(
            sql.Literal(owner_id), sql.Literal(apartment_id))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
        return ReturnValue.OK
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owner_Apartment WHERE owner_id={0} AND apartment_id={1}").format(
            sql.Literal(owner_id), sql.Literal(apartment_id))
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
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
        conn.close()
        return ReturnValue.OK
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Owners WHERE id IN (SELECT owner_id FROM Owner_Apartment WHERE apartment_id={0})".format(apartment_id))
    except DatabaseException:
        return Owner.bad_owner()
    finally:
        conn.close()
        return result
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    result = 0
    try:
        conn = Connector.DBConnector()
        result = conn.execute("SELECT * FROM Apartment WHERE id IN (SELECT apartment_id FROM Owner_Apartment WHERE owner_id={0})".format(owner_id))
    except DatabaseException:
        return []
    finally:
        conn.close()
        return result
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        sub_query="(SELECT get_owner_rating(Owner_ID) from Owner_Reservations WHERE Apartment_ID=apartment_id)"
        res=conn.execute("SELECT AVG(RATING) FROM ")
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
        conn = Connector.DBConnector()
        sub_query="(SELECT get_apartment_rating(Apartment_ID) from Owner_Reservations WHERE Owner_ID=owner_id)"
        res=conn.execute("SELECT AVG(RATING) FROM  "+sub_query)
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
        sub_query="(SELECT COUNT(customer_id) AS customer_count FROM Reservations GROUP BY customer_id)"
        res = conn.execute("SELECT MAX(customer_count) FROM "+sub_query)
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
