from create import customer, estate_agent, office, agent_office_relation, house_info, house_listing, sales, Base, \
    engine, summary_commission
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Index
from sqlalchemy import func, distinct, join, extract, insert

# create and start a session
Session = sessionmaker(bind=engine)
session = Session()

year = 2018
month = 1
print('For year', year, 'month', month)
# 1. Find the top 5 offices with the most sales for that month.
# create index
Index("idx1",house_info.office, house_info.house_id)
Index("idx2",sales.date, sales.sale_price)
result1 = session.query(
    office.office_name, func.sum(sales.sale_price).label('office_sale')).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).join(
    house_info,house_info.house_id == sales.house_id).join(
    office,office.office_id == house_info.office).group_by(
    house_info.office).order_by(func.sum(sales.sale_price).desc()).limit(5).all()
assert result1[0] == ('north carolina north', 1313827)
assert result1[4] == ('south carolina west', 600000)
print('The top five offices with the most sales are:', result1)

# 2. Find the top 5 estate agents who have sold the most (include their contact details and their sales details so that it is easy contact them and congratulate them).
Index("idx3", sales.estate_agent_id)
result2 = session.query(estate_agent.firstname, estate_agent.surname, estate_agent.email,
                    estate_agent.phone, func.sum(sales.sale_price).label("Amount_sold")).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).join(
    estate_agent,estate_agent.agent_id == sales.estate_agent_id).group_by(
    sales.estate_agent_id).order_by(func.sum(sales.sale_price).desc()).limit(5).all()
print('The top 5 estate agents who have sold the most are:', result2)

# 3. Calculate the commission that each estate agent must receive and store the results in a separate table.
Index("idx3", sales.estate_agent_commission)
sel = session.query(sales.estate_agent_id, func.sum(sales.estate_agent_commission).label("Total_commission")).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).group_by(
    sales.estate_agent_id)
i = insert(summary_commission).from_select(names=['estate_agent_id', 'monthly_commission'],
                                           # array of column names that your query returns
                                           select=sel)
session.execute(i)
result3 = session.query(summary_commission).all()
assert result3[0].__dict__['monthly_commission'] == 44000
print('The commission for each agent is: ', result3)

# 4. For all houses that were sold that month, calculate the average number of days that the house was on the market.
'''sql
SELECT AVG(sales.date - house_listing.date) FROM sales
WHERE date = datetime(2018,1)
JOIN house_listing 
ON house_listing.house_id = sales.house_id
'''
result4 = session.query(func.avg(func.julianday(sales.date) - func.julianday(house_listing.listing_date))).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).join(
    house_listing, house_listing.house_id == sales.house_id).first()
assert result4[0] == 61
print('The average number of days that the house was on the market is: ', result4)

# 5. For all houses that were sold that month, calculate the average selling price
'''sql
SELECT avg(sales.sale_price) FROM sales
WHERE date = datetime(2018,1)
'''
result5 = session.query(func.avg(sales.sale_price)).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).first()
assert result5[0] == 720733.1428571428
print('The average selling price for all houses is:', result5)

# 6. Find the zip codes with the top 5 average sales prices
'''sql
SELECT house_info.zip_code, avg(sales.sale_price) from sales
JOIN house_info
ON house_info.house_id = sales.house_id
WHERE date = datetime(2018,1)
GROUP BY house_info.zip_code
'''
result6 = session.query(house_info.zip_code, func.avg(sales.sale_price)).join(
    house_info, house_info.house_id == sales.house_id).filter(
    extract('year', sales.date) == year, extract('month', sales.date) == month).group_by(
    house_info.zip_code).order_by(func.avg(sales.sale_price).desc()).all()
assert result6[0] == (94102,856913.5)
print('The zip codes with the top 5 average sales prices are:', result6)


