from sqlalchemy import create_engine
import pandas as pd
import streamlit as st
import pydeck as pdk

import pandas as pd



# Set Streamlit page to wide mode
st.set_page_config(layout="wide")
# Database connection details
db_user = st.secrets["db_user"]
db_password = st.secrets["db_password"]
db_host = st.secrets["db_host"]
db_name = st.secrets["db_name"]
db_port = st.secrets["db_port"]

db_connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


engine = create_engine(db_connection_string)


# Database fetching function
def fetch_data(query):
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result

st.title('Restaurant Financial Data Visualization')

# Fetch options with 'All' for single select dropdowns
def get_options_with_all(query, column_name):
    data = fetch_data(query)
    options = data[column_name].unique().tolist()
    options.insert(0, 'All')  # Add 'All' option at the beginning
    return options

# Fetch options without 'All' for multi-select
def get_options(query, column_name):
    data = fetch_data(query)
    return data[column_name].unique().tolist()


restaurant_names = get_options_with_all("SELECT DISTINCT name FROM financial.restaurants", 'name')
company_names = get_options_with_all("SELECT name FROM financial.company", 'name')
city_names = get_options_with_all("SELECT DISTINCT city FROM financial.restaurants", 'city')
post_codes = get_options_with_all("SELECT DISTINCT post FROM financial.restaurants", 'post')
years = get_options_with_all("SELECT DISTINCT year FROM financial.financial_data", 'year')
restaurant_types = get_options("SELECT DISTINCT type FROM financial.restaurants", 'type')  # For multi-select

selected_restaurant = st.selectbox('Select a Restaurant', restaurant_names)
selected_company = st.selectbox('Select a Company', company_names)
selected_city = st.selectbox('Select a City', city_names)
selected_post = st.selectbox('Select a Post Code', post_codes)
selected_year = st.selectbox('Select a Year', years,index=len(years)-2)  # Select year 2019 as default
selected_types = st.multiselect('Select Restaurant Types', restaurant_types, default=['ravintolat'])  # Multi-select for types
metric = st.radio('Select Metric', ['revenue', 'profit'])

# Adjust query based on selections
where_clauses = []
if selected_company != 'All':
    where_clauses.append(f"c.name = '{selected_company}'")
if selected_restaurant != 'All':
    where_clauses.append(f"r.name = '{selected_restaurant}'")
if selected_city != 'All':
    where_clauses.append(f"r.city = '{selected_city}'")
if selected_post != 'All':
    where_clauses.append(f"r.post = '{selected_post}'")
if selected_year != 'All':
    where_clauses.append(f"fd.year = {selected_year}")
if selected_types:  # Handling multi-select for types
    types_clause = "r.type IN (" + ",".join([f"'{t}'" for t in selected_types]) + ")"
    where_clauses.append(types_clause)

where_clause = " AND ".join(where_clauses)
if where_clause:
    where_clause = "WHERE " + where_clause

query = f"""
SELECT
r.name as restaurant_name,
c.name as company_name,
fd.{metric},
r.latitude,
r.longitude,
r.company_id,
r.type,
fd.year
FROM financial.restaurants r
JOIN financial.company c ON r.company_id = c.id
JOIN financial.financial_data fd ON fd.company_id = c.id
{where_clause}
"""

data = fetch_data(query)
def normalize_column(data, column_name):
    """Normalize the data column to a 0-1 range."""
    max_value = data[column_name].max()
    min_value = data[column_name].min()
    data[f'{column_name}'] = (data[column_name] - min_value) / (max_value - min_value)
    return data

# Check if data is not empty
if not data.empty:
    # Convert latitude and longitude to float
    data['latitude'] = data['latitude'].astype(float)
    data['longitude'] = data['longitude'].astype(float)
    # Calculate the number of unique restaurants per company per year
    unique_restaurants_per_year = data.groupby(['company_id', 'year']).restaurant_name.nunique().reset_index(name='unique_restaurant_count')
    # Merge this count back to the original data
    data = pd.merge(data, unique_restaurants_per_year, on=['company_id', 'year'])
    # Calculate per-restaurant metric by dividing the company-wide metric by the count of unique restaurants for that year
    data[f'per_restaurant_{metric}'] = data[metric] / data['unique_restaurant_count']

    with st.expander("See detailed data"):
        st.dataframe(data)
    data['color'] = data[metric].apply(lambda x: [52, 152, 219, 50] if x > 0 else [230, 126, 34, 50])
    scale_factor = 0.004 if metric =='revenue' else 0.4
    if metric !='revenue':
        data[f'per_restaurant_{metric}'] =  data[f'per_restaurant_{metric}'].abs()
    # Display the map with one point per restaurant, including the updated metric
    st.pydeck_chart(pdk.Deck(
        map_style='mapbox://styles/mapbox/light-v9',
        initial_view_state=pdk.ViewState(
            latitude=data['latitude'].mean(),
            longitude=data['longitude'].mean(),
            zoom=11,
            # pitch=50,
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=data,
                get_position='[longitude, latitude]',
                get_color='color',
                get_radius=f'per_restaurant_{metric} * {scale_factor}',  # Radius of each point
                pickable=True,
                extruded=True,  # This property enables 3D visualization
            ),
        ],
        tooltip={
            'html': f'{{restaurant_name}}<br>{{company_name}}<br>Number of restaurants: {{unique_restaurant_count}}<br><b>Per-Restaurant {metric.capitalize()}:</b> {{{metric}}}',
            'style': {
                # 'backgroundColor': 'steelblue',
                'color': 'white'
            }
        }
    ))
    # st.dataframe(data)

    # data['color'] = data[metric].apply(lambda x: [52, 152, 219, 60] if x > 0 else [230, 126, 34, 60])
    # if metric =='revenue':
    #     data[f'per_restaurant_{metric}'] = data[f'per_restaurant_{metric}']*0.01
    # else:
    #     data[f'per_restaurant_{metric}'] = data[f'per_restaurant_{metric}'].abs()*0.2


    # st.map(data,
    #     latitude='latitude',
    #     longitude='longitude',
    #     size=f'per_restaurant_{metric}',
    #     color='color',
    #     use_container_width=True
    #     )
    
    # st.title('EDA of Restaurant Financial Data')

    # st.header('Basic Statistics')
    # st.write(data.describe())

    # st.header('Distribution of Revenue and Profit')
    # fig, ax = plt.subplots()
    # sns.histplot(data[f'per_restaurant_{metric}'], kde=True, color="blue", label="Revenue", ax=ax)
    # sns.histplot(data['profit'], kde=True, color="green", label="Profit", ax=ax)
    # plt.legend()
    # st.pyplot(fig)


    # # Grouping data by restaurant type and calculating average revenue and profit
    # avg_financials_by_type = data.groupby('type')[f'per_restaurant_{metric}'].mean().reset_index()

    # # Visualization
    # fig1= plt.figure(figsize=(10, 6))
    # sns.barplot(x='type', y=f'per_restaurant_{metric}', data=avg_financials_by_type, palette='coolwarm')
    # plt.title(f'Average {metric} by Restaurant Type')
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # st.pyplot(fig1)

else:
    st.write("No data available for the selected filters.")