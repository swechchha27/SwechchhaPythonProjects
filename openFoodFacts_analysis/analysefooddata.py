import sqlite3
import pandas as pd
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt

DATABASE = 'openfoodfacts/foodrescue-content.sqlite3'
DEFAULT_COUNTRY = 'INDIA'

# Function to return database connection
def connect_db(dbname):
    return sqlite3.connect(dbname)

# Function to accept country input
def accept_country_input():
    # User inputs the country for which they want to analyse products
    country = input("Enter the country you are interested in: ").strip().upper()
    # Validate the user input
    input_counter = 1
    while not validate_country_input(conn, country):
        if input_counter == 5:
            print(f"Input Attempt limit reached. Taking default country {DEFAULT_COUNTRY}.")
            country = DEFAULT_COUNTRY
            break
        country = input("Enter the country you are interested in: ").strip().upper()
        input_counter += 1
    return country

# Function to validate country input
def validate_country_input(conn, country):
    if not country:
        print("Country name cannot be empty. Please try again.")
        return False
    else:
        # Validate if country has products present in database
        validate_query = '''
            SELECT COUNT(*)
            FROM product_countries pc 
            JOIN countries c 
            ON pc.country_id = c.id
            WHERE UPPER(c.name) = ?
        '''
        cur = conn.cursor()
        cur.execute(validate_query, (country, ))
        try:
            row = cur.fetchone()
            if not row[0]:
                country_exists = False
            else:
                country_exists = True
        except:
            print('Could not check country existence')
            return False
        finally:
            cur.close()
    return country_exists

# Function to convert sql query data into pandas dataframe
def fetch_df_from_query(conn, query, params_tuple=None):
    df = pd.read_sql_query(query, conn, params=params_tuple)
    return df

# Function to find the top-level parent category (or a desired hierarchy level)
def get_top_level_category(category_id, category_to_parent):
    while category_to_parent.get(category_id) is not None:
        category_id = category_to_parent[category_id]
    return category_id

# Function to convert text into camel case
def normalize_text(s):
    s = re.sub(r'[^\w\s]', '', s)
    parts = s.lower().split()
    return parts[0] + ''.join(part.capitalize() for part in parts[1:])

# Connect to the SQLite database
conn = connect_db(DATABASE)
# Accept and validate user input country
country = accept_country_input()
# Read data from product_countries table and extract products of input country
query = '''
    SELECT pc.product_id, pc.country_id, c.name AS country_name 
    FROM product_countries pc 
    JOIN countries c 
    ON pc.country_id = c.id
    WHERE UPPER(c.name) = ?
'''
products_df = fetch_df_from_query(conn, query, params_tuple=(country, ))
product_ids = list(products_df['product_id'])
print(product_ids[:5])

# Now lets check the category that these products fall into
query = '''
    SELECT ct.category_id, cn.name AS category_name, pc.product_id, p.code AS product_code
    FROM category_names cn
    JOIN product_categories ct
    ON ct.category_id = cn.category_id
    JOIN products p
    ON p.id = ct.product_id
    JOIN product_countries pc 
    ON ct.product_id = pc.product_id
    JOIN countries c 
    ON pc.country_id = c.id
    WHERE UPPER(c.name) = ?
    AND cn.lang = 'en'
'''
categories_df = fetch_df_from_query(conn, query, params_tuple=(country, ))
# print(categories_df)
category_names = list(categories_df['category_name'])
print(category_names[:5])
# Load the category_structure table
query = '''
    SELECT cs.category_id, cs.parent_id, cn.name AS parent_category_name 
    FROM category_structure cs
    JOIN category_names cn
    ON cn.category_id = cs.parent_id
    WHERE cn.lang = 'en'
'''
category_structure = fetch_df_from_query(conn, query)
# Create a mapping from category_id to its parent_id
category_to_parent = dict(zip(category_structure['category_id'], category_structure['parent_id']))
parent_name_mapping = dict(zip(category_structure['parent_id'], category_structure['parent_category_name']))
# Apply the mapping to the original DataFrame to simplify the category_id
categories_df['parent_category_id'] = categories_df['category_id'].apply(lambda x: get_top_level_category(x, category_to_parent))
# Now map the parent_category_id to parent_category_name
categories_df['parent_category_name'] = categories_df['parent_category_id'].map(parent_name_mapping)
# print(categories_df)
# Now, group by product_id and aggregate parent_category_id into a set
product_category_dict = categories_df.groupby('product_id')['parent_category_id'].apply(set).to_dict()
# print(product_category_dict)
# Group by product_id and aggregate category_id and category_name into lists
product_category_df = categories_df.groupby('product_id').agg({
    'product_code': 'first',                     # Just take the first (or any) value, since it's 1-to-1 with product_id
    'category_id': lambda x: set(x),                     # Aggregate category_id into a set
    'category_name': lambda x: set(x),                   # Aggregate category_name into a set
    'parent_category_id': lambda x: set(x),      # Aggregate unique top-level category IDs into a set
    'parent_category_name': lambda x: set(x)              # Aggregate unique parent category names into a set
}).reset_index()
print(product_category_df)

# Normalizing category names for wordcloud
normalized_categories = [normalize_text(category) for category in category_names]
# Combine into a single string for the word cloud
categories_text = ' '.join(normalized_categories)
# Generate the word cloud
wordcloud = WordCloud(width=800, height=400).generate(categories_text)
# Display the word cloud
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()


# Close the database connection
conn.close()

print("Data Analysed successfully.")
