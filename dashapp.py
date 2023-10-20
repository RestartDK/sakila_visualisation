import dash
from dash import html,dcc
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def updateRevenueChart():
    # SQL query to retrieve data for the selected category over time
    query = """
    select film.title, SUM(payment.amount) as revenue 
    from film
    join inventory on inventory.film_id = film.film_id
    join rental on rental.inventory_id = inventory.inventory_id
    join payment on payment.rental_id = rental.rental_id
    group by film.film_id, film.title
    order by revenue DESC
    limit 5
    """
    
    data = pd.read_sql(query, engine)

    # Create the line chart
    fig = px.bar(data, x="title", y="revenue")

    return fig

def updateMovieAppearancesChart():
    # SQL query to retrieve data for the selected category over time
    query = """
    select actor.first_name, actor.last_name, count(film_actor.film_id) as movie_appearances
    from actor
    join film_actor on actor.actor_id = film_actor.actor_id
    join film on film_actor.film_id = film.film_id 
    group by actor.actor_id, actor.first_name, actor.last_name
    having count(film_actor.film_id) > 15
    order by movie_appearances DESC
    """
    
    data = pd.read_sql(query, engine)

    # Create the line chart
    fig = px.bar(data, x="first_name", y="movie_appearances")

    return fig

def updateTotalPayments():
    # SQL query to retrieve data for the selected category over time
    query = """
    select c.customer_id, c.first_name, p.amount
    from customer as c
    join payment as p on c.customer_id = p.customer_id
    order by p.amount desc;
    """
    
    data = pd.read_sql(query, engine)

    # Create the line chart
    fig = px.bar(data, x="first_name", y="amount")

    return fig

def updateActorHorror():
    # SQL query to retrieve data for the selected category over time
    query = """
    SELECT actor.first_name, actor.last_name, COUNT(*) as film_count
    FROM actor
    JOIN film_actor ON actor.actor_id = film_actor.actor_id
    JOIN film ON film_actor.film_id = film.film_id
    JOIN film_category ON film.film_id = film_category.film_id
    JOIN category ON film_category.category_id = category.category_id
    WHERE category.name = 'Horror'
    GROUP BY actor.actor_id
    ORDER BY film_count DESC
    LIMIT 3;
    """
    
    data = pd.read_sql(query, engine)

    # Create the line chart
    fig = px.bar(data, x="first_name", y="film_count")

    return fig

def updateRentalCategories():
    # SQL query to retrieve data for the selected category over time
    query = """
    SELECT DATE(rental_date) AS rental_day, COUNT(rental_id) AS rental_count
    FROM rental, inventory, film, film_category
    WHERE rental.inventory_id = inventory.inventory_id AND
    inventory.film_id = film.film_id AND
    film.film_id = film_category.film_id AND
    category_id = 1
    GROUP BY rental_day;
    """
    
    data = pd.read_sql(query, engine)

    # Create the line chart
    fig = px.bar(data, x="rental_day", y="rental_count")

    return fig

# Connect to the Sakila database
engine = create_engine('mysql://root:@localhost/sakila')

# Create the Dash app
app = dash.Dash(__name__)

# Create bar Chart
fig = updateRevenueChart()
fig2 = updateMovieAppearancesChart()
fig3 = updateTotalPayments()
fig4 = updateMovieAppearancesChart()
fig5 = updateRentalCategories()

# Define the layout of the app
app.layout = html.Div([
    html.H1("Top 5 Movie Revenues"),
    dcc.Graph(id='bar-chart', figure=fig),
    html.H1("All Movie Appearances"),
    dcc.Graph(id='bar-chart', figure=fig2),
    html.H1("Total Payments"),
    dcc.Graph(id='bar-chart', figure=fig3),
    html.H1("Top 3 Movie Appearnaces"),
    dcc.Graph(id='bar-chart', figure=fig4),
    html.H1("Movie Categories over Average Length"),
    dcc.Graph(id='bar-chart', figure=fig5),
])



if __name__ == '__main__':
    app.run_server(debug=True)
