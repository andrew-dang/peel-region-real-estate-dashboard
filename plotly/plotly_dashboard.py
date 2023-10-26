#!/usr/bin/env python
# coding: utf-8

# Standard imports
import pandas as pd

# Plotly and Dash
import plotly.express as px
import plotly.graph_objects as go
import dash 
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

# Custom functions
from helper import * 

# Constants and config settings
from dashboard_config import *


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.SUPERHERO])
server = app.server


# App layout
app.layout = html.Div([
    # Top bar
    html.Div(
        className="my_row",
        children=[
            html.P(
                children=F"Find and compare listings in the GTA",
                style=p_style,
                className="card_container_one"
            )
        ]
    ),
    
    # First row
    html.Div(
            className="my_row",
            children=[
                html.Div(
                    className="dropdown_card",
                    children=[
                        # html.Label(["Type in an address:"], style={'font-weight': 'bold', "text-align": "center", 'color': 'white', 'font-size': 30}),
                        dcc.Dropdown(
                            id='address_selector', 
                            options=[{'label': x, 'value': x} for x in address_ls], 
                            value=None,
                            multi=False, 
                            disabled=False,
                            clearable=True, 
                            searchable=True,
                            placeholder='Search by City or Address',
                            persistence=True, 
                            persistence_type='memory'
                            ),
                    ])
            ]),
    
    # Fourth row - carousel
    html.Div(
        className="my_row",
        children=[
            html.Div(
                className="carousel_card",
                children=[
                    dbc.Carousel(
                        id="carousel",
                        items=carousel_items,
                        controls=True,
                        indicators=False,
                        class_name="carousel_container",
                        variant="dark"
                    )
            ])
    ]),

    # Second row
    html.Div(
        className="my_row",
        children=[
            html.Div(
                className="card_container_four",
                children=[
                html.H6(children="Number of Rooms", style=h6_style),
                html.P(children="", style=p_style, id='number_rooms')
                ] 
            ),
            
            html.Div(
                className="card_container_four",
                children=[
                html.H6(children="Property Type", style=h6_style),
                html.P(children="", style=p_style, id='property_type')
                ]
            ),

            html.Div(
                className="card_container_four",
                children=[
                html.H6(children="Listed Price", style=h6_style),
                html.P(children="", style=p_style, id='list_price')
                ]
            ),

            html.Div(
                className="card_container_four",
                children=[
                html.H6(children="Square Footage", style=h6_style),
                html.P(children="", style=p_style, id='sqft')
                ]
            )
    ]),

    # Third row 
    html.Div(
        className="my_row",
        children=[
            html.Div(
                className="card_container_three",
                children=[
                html.H6(children="Age of Home", style=h6_style),
                html.P(children="", style=p_style, id='home_age')
                ]
            ),

            html.Div(
                className="card_container_three",
                children=[
                html.H6(children="Days on Market", style=h6_style),
                html.P(children="", style=p_style, id='days_on_market')
                ]
            ),

            html.Div(
                className="card_container_three",
                children=[
                html.H6(children="Finished Basement", style=h6_style),
                html.P(children="", style=p_style, id='finished_basement')
                ]
            ),
    ]),

    html.Div(
        className="my_row",
        children=[
            html.P(
                children=F"Comparing selected home against the average listing prices of similar homes",
                style=p_style,
                className="card_container_one"
            )
        ]
    ),

    # html.Br(),
    

    # Fifth row - Graphs 
    html.Div(
        className="my_row",
        children=[
            html.Div(
                className="card_container_two",
                children=[
                dcc.Graph(id='sqft_comparison')
                ]
            ), 

            html.Div(
                className="card_container_two",
                children=[
                    dcc.Graph(id='age_comparison')
                ]
            ),
        ]
    )
], className="create_container")

# Filter what active decks are available in the dropdown based on format that is selected
@app.callback(
    Output('number_rooms', 'children'),
    Output('property_type', 'children'), 
    Output('list_price', 'children'),
    Output('sqft', 'children'),
    Output('home_age', 'children'),
    Output('days_on_market', 'children'),
    Output('finished_basement', 'children'),
    Output('sqft_comparison', 'figure'),
    Output('age_comparison', 'figure'),
    Input('address_selector', 'value'),
    prevent_initial_call=True
)
def get_address(address_selector):
    query = text(f"""
    SELECT 
        *,
        CURRENT_DATE::date - list_date::date as days_on_market
    FROM 
        {TABLE}
    WHERE 
        address = '{address_selector}'
    """)

    row = pd.read_sql(query, db_conn)
    
    # Extract relevant details from the row
    number_rooms = row['number_bedrooms'][0]
    property_type = row['property_type'][0]
    list_price = locale.currency(row['listing_price'][0], grouping=True)
    list_price_no_currency = row['listing_price'][0]
    sqft = row['sqft'][0]
    CITY = row['city'][0]
    PROPERTY_TYPE = row['property_type'][0]
    home_age = row['age'][0]
    days_on_market = row['days_on_market'][0]
    finished_basement = str(row['finished_basement'][0])

    # Create queries to do comparisons 
    sqft_comparison_query = text(f"""
    SELECT 
        sqft_range, 
        AVG(listing_price) AS avg_listing_price
    FROM 
        {TABLE}
    WHERE 
        city = '{CITY}' AND
        property_type = '{PROPERTY_TYPE}'
    GROUP BY sqft_range
    ORDER BY sqft_range
    """)

    sqft_df = pd.read_sql(sqft_comparison_query, db_conn)


    # Compare selected home price against average house prices by square footage
    fig = px.bar(
                sqft_df, 
                x='sqft_range', 
                y='avg_listing_price', 
                hover_data=["avg_listing_price"], 
                labels={"sqft_range": "Square Foot Range", "avg_listing_price": "Average Listing Price"},
                )
    
    # Update layout
    tickvals = [*range(0, 1_300_000, 100000)]
    ticktext = [f"${t // 1000:,}K" for t in tickvals]
    yaxis = {'showgrid': False, "tickvals": tickvals, "ticktext": ticktext, "range": [0, 1200000], "title_standoff": 0}

    fig.update_layout(
        showlegend=True,
        title_text=textwrapper(f"Average listing price of {PROPERTY_TYPE} homes in {CITY} by square foot range"),
        title_x=0.5,
        title={"xanchor": "center", "yanchor": "middle"},
        yaxis=yaxis,
        xaxis={'ticks': 'outside', 'ticklen': 10, 'showgrid': False},
        plot_bgcolor='rgba(0,0,0,0)'
    )
    fig.add_hline(
        y=list_price_no_currency, 
        annotation={
            "hoverlabel": {"font_size": 12}, 
            "hovertext": f'Selected home list price: {list_price}',
            'text': f'{list_price}' 
            }
        )

    # Query for age range
    age_comparison_query = text(f"""
    SELECT 
        age_range, 
        AVG(listing_price) AS avg_listing_price
    FROM 
        {TABLE}
    WHERE 
        city = '{CITY}' AND
        property_type = '{PROPERTY_TYPE}'
    GROUP BY age_range
    ORDER BY
        CASE
            WHEN age_range = '0-5' THEN 0
            WHEN age_range = '6-10' THEN 1
            WHEN age_range = '11-15' THEN 2
            WHEN age_range = '16-20' THEN 3
            WHEN age_range = '21-25' THEN 4
        END ASC
    """)

    age_df = pd.read_sql(age_comparison_query, db_conn)

    # Compare selected home price against average house prices by home age
    fig2 = px.bar(
                age_df, 
                x='age_range', 
                y='avg_listing_price', 
                hover_data=["avg_listing_price"], 
                labels={"age_range": "Age Range", "avg_listing_price": "Average Listing Price"}
                )
    
    # Update layout
    fig2.update_layout(
        showlegend=True, 
        title_text=textwrapper(f"Average listing price of {PROPERTY_TYPE} homes in {CITY} by age range"),
        title_x=0.5,
        title={"xanchor": "center", "yanchor": "middle"},
        yaxis=yaxis,
        xaxis={'ticks': 'outside', 'ticklen': 10, 'showgrid': False},
        plot_bgcolor='rgba(0,0,0,0)'
        )
    fig2.add_hline(
        y=list_price_no_currency,
        annotation={
            "hoverlabel": {"font_size": 12}, 
            "hovertext": f'Selected home list price: {list_price}',
            'text': f'{list_price}' 
            }
        )

    return number_rooms, property_type, list_price, sqft, home_age, days_on_market, finished_basement, fig, fig2

@app.callback(
    Output('carousel', 'items'),
    Output('carousel', 'active_index'),
    Input('address_selector', 'value')
)
def change_carousel_photoset(selected_address: str):
    carousel_items = get_carousel_items(selected_address)

    # reset active_index
    idx = 0

    return carousel_items, idx

# Only need lines below when developing locally
if __name__ == '__main__':
    app.run_server(debug=True)
    # app.run_server(debug=False, host="0.0.0.0", port=8080)