from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

# Replace 'username' and 'password' with your MySQL username and password
engine = create_engine('mysql://root:@localhost/sakila')

query = f"""
    select film.title, SUM(payment.amount) as revenue 
    from film
    join inventory on inventory.film_id = film.film_id
    join rental on rental.inventory_id = inventory.inventory_id
    join payment on payment.rental_id = rental.rental_id
    group by film.film_id, film.title
    order by revenue DESC
    limit 5
    """

# Execute the SQL query and load the results into a pandas DataFrame
revenue_data = pd.read_sql(query, engine)


# Set the figure size
plt.figure(figsize=(10, 6))

# Create a bar chart
plt.bar(revenue_data['title'], revenue_data['revenue'])

# Customize the chart
plt.title('Top 5 Revenue Movies')
plt.xlabel('Title')
plt.ylabel('Revenue')
plt.xticks(rotation=45)  # Rotate category labels for readability

# Display the chart
plt.tight_layout()
plt.show()