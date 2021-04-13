from create import customer, estate_agent, office, agent_office_relation, house_info, house_listing, sales, \
    summary_sale_price, Base, engine
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert

# create and start a session
Session = sessionmaker(bind=engine)
session = Session()

# add fictious data
session.add(customer(firstname='Harry', surname='Potter', email='harry.potter@hogwarts.com', phone='(384) 234-9442'))
session.add(customer(firstname='Hermione', surname='Granger', email='hermione@hogwarts.edu', phone='(251) 546-3984'))
session.add(customer(firstname='Ginny', surname='Wesley', email='ginny@hogwarts.edu', phone='(847) 384-3984'))
session.add(customer(firstname='Minerva', surname='McGonagall', email='minerva@hogwarts.edu', phone='(384) 546-2837'))

session.add(estate_agent(firstname="Robert", surname='Warren', email='RobertDWarren@teleworm.us',
                         phone='(251) 546-9442'))
session.add(estate_agent(firstname="'Vincent'", surname='Brown', email='VincentHBrown@rhyta.com',
                         phone='(125) 546-4478'))
session.add(estate_agent(firstname='Janet', surname='Prettyman', email='JanetTPrettyman@teleworm.us',
                         phone='(949) 569-4371'))
session.add(estate_agent(firstname='Martina', surname='Kershner', email='MartinaMKershner@rhyta.com',
                         phone='(630) 446-8851'))
session.add(estate_agent(firstname='Tony', surname='Schroeder', email='TonySSchroeder@teleworm.us',
                         phone='(226) 906-2721'))
session.add(estate_agent(firstname='Harold', surname='Grimes', email='HaroldVGrimes@dayrep.com',
                         phone='(671) 925-1352'))

session.add(office(office_name='north carolina north'))
session.add(office(office_name='south carolina north'))
session.add(office(office_name='north carolina west'))
session.add(office(office_name='north carolina east'))
session.add(office(office_name='north carolina south'))
session.add(office(office_name='south carolina west'))

session.add(agent_office_relation(office_id=1, agent_id=1))
session.add(agent_office_relation(office_id=1, agent_id=3))
session.add(agent_office_relation(office_id=1, agent_id=2))
session.add(agent_office_relation(office_id=2, agent_id=1))
session.add(agent_office_relation(office_id=2, agent_id=6))
session.add(agent_office_relation(office_id=3, agent_id=3))
session.add(agent_office_relation(office_id=4, agent_id=4))
session.add(agent_office_relation(office_id=5, agent_id=5))
session.add(agent_office_relation(office_id=6, agent_id=6))

session.add(house_info(bedrooms_num=3, bathrooms_num=1, zip_code=94102, office=1))
session.add(house_info(bedrooms_num=1, bathrooms_num=1, zip_code=94102, office=2))
session.add(house_info(bedrooms_num=4, bathrooms_num=2, zip_code=94145, office=3))
session.add(house_info(bedrooms_num=2, bathrooms_num=1, zip_code=94145, office=4))
session.add(house_info(bedrooms_num=1, bathrooms_num=1, zip_code=94135, office=5))
session.add(house_info(bedrooms_num=2, bathrooms_num=1, zip_code=94146, office=6))
session.add(house_info(bedrooms_num=2, bathrooms_num=1, zip_code=94136, office=1))
session.add(house_info(bedrooms_num=3, bathrooms_num=1, zip_code=94135, office=2))

session.add(
    house_listing(house_id=1, seller_id=1, listing_price=523766, listing_date=datetime(2017, 11, 1), estate_agent_id=1,
                  sold=False))
session.add(
    house_listing(house_id=2, seller_id=2, listing_price=837485, listing_date=datetime(2017, 11, 2), estate_agent_id=2,
                  sold=False))
session.add(
    house_listing(house_id=3, seller_id=3, listing_price=93845, listing_date=datetime(2017, 11, 3), estate_agent_id=3,
                  sold=False))
session.add(
    house_listing(house_id=4, seller_id=4, listing_price=1002030, listing_date=datetime(2017, 11, 4), estate_agent_id=4,
                  sold=False))
session.add(
    house_listing(house_id=5, seller_id=1, listing_price=837456, listing_date=datetime(2017, 11, 5), estate_agent_id=5,
                  sold=False))
session.add(
    house_listing(house_id=6, seller_id=2, listing_price=346467, listing_date=datetime(2017, 11, 6), estate_agent_id=6,
                  sold=False))
session.add(
    house_listing(house_id=7, seller_id=3, listing_price=948576, listing_date=datetime(2017, 11, 7), estate_agent_id=1,
                  sold=False))
session.add(
    house_listing(house_id=8, seller_id=2, listing_price=523766, listing_date=datetime(2017, 11, 8), estate_agent_id=2,
                  sold=False))

session.add(summary_sale_price(total_sale=0))

session.commit()


# function for a transaction happen
# to ensure the integrity in case the service break down
def sale_transaction(sale):
    try:
        # update the sales table with the new sales entry
        session.execute(insert(sales), [sale])
        # update the sold status of the listing table
        session.query(house_listing).filter(house_listing.house_id == sale['house_id']).update(
            {house_listing.sold: True}, synchronize_session=False)
        # update the total sale price in the summary table
        session.query(summary_sale_price).filter(summary_sale_price.id == 1).update(
            {summary_sale_price.total_sale: summary_sale_price.total_sale + sale['sale_price']},
            synchronize_session=False)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


sale = [{'buyer_id': 1, 'sale_price': 613827, 'date': datetime(2018, 1, 1), 'estate_agent_id': 4, 'house_id': 1},
        {'buyer_id': 4, 'sale_price': 1100000, 'date': datetime(2018, 1, 2), 'estate_agent_id': 1, 'house_id': 2},
        {'buyer_id': 2, 'sale_price': 93846, 'date': datetime(2018, 1, 3), 'estate_agent_id': 2, 'house_id': 3},
        {'buyer_id': 3, 'sale_price': 1100000, 'date': datetime(2018, 1, 4), 'estate_agent_id': 3, 'house_id': 4},
        {'buyer_id': 1, 'sale_price': 837459, 'date': datetime(2018, 1, 5), 'estate_agent_id': 5, 'house_id': 5},
        {'buyer_id': 2, 'sale_price': 600000, 'date': datetime(2018, 1, 6), 'estate_agent_id': 6, 'house_id': 6},
        {'buyer_id': 2, 'sale_price': 700000, 'date': datetime(2018, 1, 7), 'estate_agent_id': 6, 'house_id': 7}]

for i in sale:
    sale_transaction(i)

session.commit()

# check whether all data are inserted
print(session.query(customer).all())
print(session.query(estate_agent).all())
print(session.query(office).all())
print(session.query(agent_office_relation).all())
print(session.query(house_info).all())
print(session.query(house_listing).all())
print(session.query(sales).all())
print(session.query(summary_sale_price).all())
