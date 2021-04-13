from sqlalchemy import create_engine, Column, Text, Integer, ForeignKey, DATETIME, NUMERIC, Float, Date, String, \
    VARCHAR, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, column_property
from sqlalchemy.sql import case

# creates a local .db file if doesn't exist
engine = create_engine('sqlite:///database.db')
# connect to the engine
engine.connect()

# declare a base
Base = declarative_base()


# create the necessary tables
'''
Data normalization
1. First normal form:
    Each column containing atomic values
    Each table has a primary key
2. Second normal form:
    All the column depends on the table's primary key
3. Third normal form:
    All of its columns are not transitively dependent on the primary key
    eg. There are separate tables to store the customer, agent, office information
'''
# Table customer: customer (either a seller or buyer) information
class customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    firstname = Column(VARCHAR(20))
    surname = Column(VARCHAR(20))
    email = Column(VARCHAR(20))
    phone = Column(VARCHAR(20))

    def __repr__(self):
        return "<customer(id={0}, firstname={1}, surname={2}, email={3}, phone={4})>".format(self.id,
                                                                                             self.firstname,
                                                                                             self.surname,
                                                                                             self.email,
                                                                                             self.phone)


# Table estate_agent: estate_agent information
class estate_agent(Base):
    __tablename__ = 'estate_agent'
    agent_id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    firstname = Column(VARCHAR(20))
    surname = Column(VARCHAR(20))
    email = Column(VARCHAR(20))
    phone = Column(VARCHAR(20))

    def __repr__(self):
        return "<estate_agent(agent_id={0}, firstname={1}, surname={2}, email={3}, phone={4})>".format(self.agent_id,
                                                                                                       self.firstname,
                                                                                                       self.surname,
                                                                                                       self.email,
                                                                                                       self.phone)


# Table office: office information
class office(Base):
    __tablename__ = 'office'
    office_id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    office_name = Column(Text)  # office name

    def __repr__(self):
        return "<office(office_id={0}, office_name={1})>".format(self.office_id, self.office_name)


# Table agent_office_relation: the many to many relation mapping each estate agent to their belonged offices
class agent_office_relation(Base):
    __tablename__ = 'agent_office_relation'
    id = Column(Integer, primary_key=True)
    agent_id = Column(Integer, ForeignKey(estate_agent.agent_id))
    office_id = Column(Integer, ForeignKey(office.office_id))

    def __repr__(self):
        return "<agent_office_relation(id={0}, agent_id={1}, office_id={2})>".format(self.id, self.agent_id,
                                                                                     self.office_id)


# Table house_info: house information
class house_info(Base):
    __tablename__ = 'house_info'
    house_id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    bedrooms_num = Column(Integer)  # number of bedrooms
    bathrooms_num = Column(Integer)  # number of bathrooms
    zip_code = Column(Integer)  # zip code of the house
    office = Column(Integer, ForeignKey(office.office_id))  # office which it belongs to

    def __repr__(self):
        return "<house_info(house_id={0}, bedrooms_num={1}, bathrooms_num={2}, zip_code={3}, office={4})>".format(
            self.house_id,
            self.bedrooms_num,
            self.bathrooms_num,
            self.zip_code,
            self.office)


# Table house_listing:
class house_listing(Base):
    __tablename__ = 'house_listing'
    list_id = Column(Integer, primary_key=True, autoincrement=True)  # unique listing_id
    house_id = Column(Integer, ForeignKey(house_info.house_id))  # house id
    seller_id = Column(Integer, ForeignKey(customer.id))  # seller's customer id
    listing_price = Column(Integer)  # the house price on listing
    listing_date = Column(Date)  # the date of listing
    estate_agent_id = Column(Integer)  # id of the agent associated with the entry
    sold = Column(Boolean)  # boolean value of whether it's sold or not. Sold - True

    def __repr__(self):
        return "<house_listing(list_id={0}, house_id={1}, seller_id={2}, listing_price={3}, listing_date={4}, estate_agent_id={5}, sold={6})>".format(
            self.list_id,
            self.house_id,
            self.seller_id,
            self.listing_price,
            self.listing_date,
            self.estate_agent_id,
            self.sold)


# Table sales:
class sales(Base):
    __tablename__ = 'sales'
    sales_id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    buyer_id = Column(Integer, ForeignKey(customer.id))  # buyer's customer id
    sale_price = Column(Integer)  # the sold price
    date = Column(Date)  # the sold date
    estate_agent_id = Column(Integer, ForeignKey(estate_agent.agent_id))  # the id of the selling estate agent
    # calculate the commission according to the rule given
    estate_agent_commission = column_property(sale_price * case(
        [
            (sale_price < 100000, 0.1),
            (sale_price < 200000, 0.075),
            (sale_price < 500000, 0.06),
            (sale_price < 1000000, 0.05),
        ], else_=0.04))
    house_id = Column(Integer, ForeignKey(house_info.house_id))

    def __repr__(self):
        return "<sales(sales_id={0}, buyer_id={1}, sale_price={2}, date={3}, estate_agent_id={4}, estate_agent_commission={5}, house_id={6})>".format(
            self.sales_id,
            self.buyer_id,
            self.sale_price,
            self.date,
            self.estate_agent_id,
            self.estate_agent_commission,
            self.house_id)


# Table summary_commission: a table to store commission that each estate agent must receive
class summary_commission(Base):
    __tablename__ = 'summary_commission'
    id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    estate_agent_id = Column(Integer, ForeignKey(estate_agent.agent_id))  # id of the estate agent
    monthly_commission = Column(Integer)  # commission that the estate agent must receive

    def __repr__(self):
        return "<summary_commission(id={0}, estate_agent_id={1}, monthly_commission={2}>".format(
            self.id,
            self.estate_agent_id,
            self.monthly_commission
        )


# Table summary_sale_price: a summary table containing the sum of all sale prices
# updated whenever a house is sold
class summary_sale_price(Base):
    __tablename__ = 'summary_sale_price'
    id = Column(Integer, primary_key=True, autoincrement=True)  # unique id
    total_sale = Column(Integer)  # the sum of all sale prices

    def __repr__(self):
        return "<summary_sale_price(id={0}, total_sale={1}>".format(
            self.id,
            self.total_sale
        )


Base.metadata.create_all(bind=engine)

# create and start a session
Session = sessionmaker(bind=engine)
session = Session()

# print the schemas for all tables created
print(repr(customer))
print(repr(estate_agent))
print(repr(office))
print(repr(agent_office_relation))
print(repr(house_info))
print(repr(house_listing))
print(repr(sales))
print(repr(summary_commission))
print(repr(summary_sale_price))

#Base.metadata.drop_all(engine)
