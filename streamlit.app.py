

import streamlit as st
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled.")

# Get Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()
#session = get_active_session()

# Fetch the data from Snowflake
try:
    # Fetch the relevant columns from fruit_options for editing
    my_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_ID")).to_pandas()
except Exception as e:
    st.write(f"Error fetching data: {e}")
    my_dataframe = pd.DataFrame()  # Empty DataFrame to handle error gracefully

# Display the data if available
if not my_dataframe.empty:
    # Use st.data_editor instead of st.experimental_data_editor
    editable_df = st.data_editor(my_dataframe)

    submitted = st.button('Submit')
    if submitted:
        try:
            # Create dataframes for merge operation
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            # Perform merge operation
            # Ensure the merge is done on FRUIT_ID and updates ORDER_FILLED in the orders table
            merge_result = og_dataset.merge(
                edited_dataset,
                (og_dataset['FRUIT_ID'] == edited_dataset['FRUIT_ID']),  # Condition for matching rows
                when_matched_update={'ORDER_FILLED': edited_dataset['ORDER_FILLED']}  # Update the matched rows
            )
            
            st.success("Order(s) Updated!", icon="👍")
            
            # Fetch updated data to check for pending orders
            updated_dataframe = session.table("smoothies.public.fruit_options").select(col("FRUIT_ID")).to_pandas()
            if updated_dataframe.empty:
                st.success('There are no pending orders right now', icon="👍")

        except Exception as e:
            st.write(f'Something went wrong: {e}')
else:
    st.success('There are no pending orders right now', icon="👍")
