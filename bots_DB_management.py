import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

# Database connection
db_path = "final_bots_db_updated_with_new_docs_corrected.sqlite"

# Streamlit application

st.set_page_config(
   page_title="Bots Database Management",
   page_icon="GoChat247.png",
   layout="wide",
   initial_sidebar_state="auto",
)
st.image("logo.png", width = 250)
st.title("Bots Management :robot_face:")

# Load data from the SQLite database
@st.cache_data(ttl=60)
def load_data(table_name):
    conn = sqlite3.connect(db_path)
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Add a new record to the database

def add_record(data: pd.DataFrame, table_name: str) -> None:
    """
    Adds a new record to the specified table in the SQLite database.

    Args:
        data (pd.DataFrame): The data to be added as a new record.
        table_name (str): The name of the table where the record will be added.
    """
    try:
        conn = sqlite3.connect(db_path)
        data.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
    except sqlite3.IntegrityError as e:
        st.error("Integrity error:", e)
    except sqlite3.DatabaseError as e:
        st.error("Database error:", e)
    except Exception as e:
        st.error("An unexpected error occurred:", e)
    finally:
        conn.close()
        st.cache_data.clear()  # Clear the cache after adding a record


def delete_record(identifier: int, table_name: str, identifier_column: str) -> None:
    """
    Deletes a record from the specified table in the SQLite database. If the table is 'Bots',
    it also deletes related entries in the BotKnowledgeLink table.

    Args:
        identifier (int): The identifier of the record to delete.
        table_name (str): The name of the table where the record will be deleted.
        identifier_column (str): The name of the column to match the identifier against.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraint enforcement
        cursor = conn.cursor()
        cursor.execute("BEGIN")  # Start a transaction

        if table_name == "Bots":
            cursor.execute(f"DELETE FROM BotKnowledgeLink WHERE Bot_ID = ?", (identifier,))
        elif table_name == "KnowledgeBase":
            cursor.execute(f"DELETE FROM BotKnowledgeLink WHERE KnowledgeBase_ID = ?", (identifier,))

        cursor.execute(f"DELETE FROM {table_name} WHERE {identifier_column} = ?", (identifier,))
        conn.commit()  # Commit the transaction

    except sqlite3.IntegrityError as e:
        conn.rollback()  # Rollback the transaction on error
        st.error("Integrity error:", e)
    except sqlite3.DatabaseError as e:
        conn.rollback()  # Rollback the transaction on error
        st.error("Database error:", e)
    except Exception as e:
        conn.rollback()  # Rollback the transaction on error
        st.error("An unexpected error occurred:", e)
    finally:
        conn.close()  # Ensure the connection is closed


def update_record(record_id: int, data: dict, table_name: str, column_name: str) -> None:
    """
    Updates a record in the specified table in the SQLite database with the given data.

    Args:
        record_id (int): The ID of the record to update.
        data (dict): A dictionary of the data to update.
        table_name (str): The name of the table where the record is located.
        column_name (str): The name of the column that identifies the record.
    """
    try:
        conn = sqlite3.connect(db_path)
        processed_data = {k: v if pd.notna(v) and v != "None" else None for k, v in data.items()}
        columns = ', '.join([f"{k} = ?" for k in processed_data.keys()])
        values = list(processed_data.values())
        values.append(record_id)
        conn.execute(f"UPDATE {table_name} SET {columns} WHERE {column_name} = ?", values)
        conn.commit()
    except sqlite3.DatabaseError as e:
        st.error("Database error:", e)
    except Exception as e:
        st.error("An unexpected error occurred:", e)
    finally:
        conn.close()
        st.cache_data.clear()  # Clear the cache after updating a record


def get_default_value_for_column(column: str, table_name: str) -> str:
    """
    Gets the default value for a column in the specified table.

    Args:
        column (str): The name of the column.
        table_name (str): The name of the table.

    Returns:
        str: The default value for the column.
    """
    default_values = {
        'Bots': {
            "Total_Interactions": "0",
            "Positive_Feedback_Count": "0",
            "Negative_Feedback_Count": "0",
            "Level_of_Access": "Full",
            "Active_Status": "Active",
            "Version": "1.0",
            "Owner_Maintainer": "Bahrain E-GOV",
            "Foundation_Business": "Bahrain E-GOV",
            "Foundation_Name": "Bahrain E-GOV",
            "Last_Updated": datetime.now().strftime("%Y-%m-%d")
        },
        'KnowledgeBase': {
            "Content": "Sample Document",
            "Metadata": "Sample Metadata"
        }
    }
    return default_values.get(table_name, {}).get(column, None)

def get_knowledgebase_entries():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT ID, Content FROM KnowledgeBase", conn)
    conn.close()
    return df

# Function to add a new Bot record and return its ID
def add_bot_and_get_id(data):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' for _ in data)
        cursor.execute(f"INSERT INTO Bots ({columns}) VALUES ({placeholders})", list(data.values()))
        bot_id = cursor.lastrowid  # Get the ID of the newly added Bot
        conn.commit()  # Make sure to commit the transaction
        return bot_id
    except sqlite3.Error as e:
        st.error(f"An error occurred: {e}")
    finally:
        conn.close()
        st.cache_data.clear()  # Clear the cache after updating a record


# Function to link Bot to KnowledgeBase without creating duplicates
def link_bot_to_knowledgebase(bot_id, kb_ids):
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            cursor = conn.cursor()
            for kb_id in kb_ids:
                # Check if the link already exists
                cursor.execute("SELECT * FROM BotKnowledgeLink WHERE Bot_ID=? AND KnowledgeBase_ID=?", (bot_id, kb_id))
                if cursor.fetchone() is None:
                    # If not, create the link
                    cursor.execute("INSERT INTO BotKnowledgeLink (Bot_ID, KnowledgeBase_ID) VALUES (?, ?)", (bot_id, kb_id))
    except sqlite3.Error as e:
        st.error(f"An error occurred: {e}")
    finally:
        conn.close()


def get_linked_knowledgebase_entries(bot_id):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT KnowledgeBase_ID FROM BotKnowledgeLink WHERE Bot_ID = ?"
    try:
        print(f"Executing query: {query} with Bot_ID {bot_id}")
        cursor.execute(query, (bot_id,))
        rows = cursor.fetchall()
        if not rows:
            print(f"No KnowledgeBase entries linked to Bot_ID {bot_id}")
        return [row[0] for row in rows]
    except sqlite3.Error as e:
        print(f"An error occurred during query execution: {e}")
        return []
    finally:
        conn.close()


# Function to update the BotKnowledgeLink table
def update_bot_knowledge_links(bot_id, kb_ids_selected):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure bot_id is an integer
    bot_id = int(bot_id)

    # First, delete existing links
    cursor.execute("DELETE FROM BotKnowledgeLink WHERE Bot_ID=?", (bot_id,))

    # Then, insert the new links
    for kb_id in kb_ids_selected:
        # Ensure kb_id is an integer
        kb_id = int(kb_id)
        cursor.execute("INSERT INTO BotKnowledgeLink (Bot_ID, KnowledgeBase_ID) VALUES (?, ?)", (bot_id, kb_id))

    conn.commit()
    conn.close()


# Sidebar for user inputs
st.sidebar.header("Settings")
table_name = st.sidebar.selectbox("Select a table:", ["Bots", "KnowledgeBase", "BotKnowledgeLink"])

# Depending on the table, set the available actions
if table_name == "BotKnowledgeLink":
    action = "View Records"  # Only allow viewing records for the BotKnowledgeLink table
    # action = st.sidebar.radio("Action:", ("View Records", "Add Record", "Update Record", "Delete Record"))
else:
    # Allow all actions for other tables
    action = st.sidebar.radio("Action:", ("View Records", "Add Record", "Update Record", "Delete Record"))

df = load_data(table_name)

# Depending on the table, set the identifier column for selecting records
identifier_column = "Botperson_Name" if table_name == "Bots" else "ID"
required_columns = {
    'KnowledgeBase': 'ID',
    'Bots': 'Bot_ID',
    'BotKnowledgeLink': 'Bot_ID'
}

# View Records
if action == "View Records":
    # Sidebar for user inputs
    st.sidebar.header("View Settings")
    # Ensure the required column is always selected
    required_column = required_columns[table_name]
    # Set up the default columns with the required column always included
    default_columns = [required_column] + [col for col in df.columns.tolist() if col != required_column]
    # Use the multiselect widget with default columns
    columns_to_show = st.sidebar.multiselect("Select columns to show:", df.columns.tolist(), default=default_columns)
    # Check if the required column is in the selection after the user input
    if required_column not in columns_to_show:
        # If not, add it back to the selection and show a warning
        columns_to_show.insert(0, required_column)
        st.sidebar.warning(f"The '{required_column}' column cannot be removed.")
    sorted_column = st.sidebar.selectbox("Sort by:", df.columns.tolist())
    sort_order = st.sidebar.radio("Sort order:", ("Ascending", "Descending"))
    # Apply settings
    df_display = df[columns_to_show]
    if sort_order == "Ascending":
        df_display = df_display.sort_values(by=sorted_column)
    else:
        df_display = df_display.sort_values(by=sorted_column, ascending=False)
    st.dataframe(df_display, hide_index = True, use_container_width=True)


# Add Record
elif action == "Add Record":
    with st.form("Add New Bot"):
        new_data = {}
        required_fields = ["Botperson_Name", "Botperson_Role", "Role", "Usage", "Sector", "Prompt"]
        
        kb_ids_selected = []
        for col in df.columns:
            if col not in ['Bot_ID', 'ID']:  # Assuming 'ID' or 'Bot_Name' should not be manually entered
                default_value = get_default_value_for_column(col, table_name)
                new_data[col] = st.text_input(col, value=default_value)

        if table_name == "Bots":
            # Fetch KnowledgeBase entries for linking
            kb_entries = get_knowledgebase_entries()
            kb_options = list(zip(kb_entries['ID'], kb_entries['Content']))
            kb_ids_selected = st.multiselect("Select KnowledgeBase entries to link with the Bot:", options=kb_options, format_func=lambda x: x[1])

        submitted = st.form_submit_button("Add Record")
        if submitted:
            if table_name == "Bots":
                missing_fields = [field for field in required_fields if not new_data[field]]
                if missing_fields:
                    st.warning(f"Please fill out all required fields: {', '.join(missing_fields)}")
                else:
                    bot_id = add_bot_and_get_id(new_data)  # This should be only for Bots table
                    st.success("Bot added successfully!")
                    if kb_ids_selected:
                        kb_ids = [option[0] for option in kb_ids_selected]
                        link_bot_to_knowledgebase(bot_id, kb_ids)
                        st.success("Bot linked to KnowledgeBase successfully!")
            else:
                new_data = {k: v if v != "" else None for k, v in new_data.items()}
                new_data[identifier_column] = df[identifier_column].max() + 1 if not df.empty else 1  # Auto-increment ID
                new_df = pd.DataFrame([new_data])
                add_record(new_df, table_name)
                st.success("Record added successfully!")

# Delete Record
elif action == "Delete Record":
    if table_name == "BotKnowledgeLink":
        identifier_column = 'Bot_ID'
    record_identifier = st.selectbox(f"Select a {identifier_column} to delete:", [f"Select {identifier_column}"] + df[identifier_column].astype(str).tolist())
    if st.button("Delete Record") and record_identifier != f"Select {identifier_column}":
        delete_record(record_identifier, table_name, identifier_column)
        st.success("Record deleted successfully!")
        
# # Update Record
elif action == "Update Record":
    # Inside your update section
    record_identifier = st.selectbox(f"Select a {identifier_column} to update:", [f"Select {identifier_column}"] + df[identifier_column].astype(str).tolist())
    if record_identifier and record_identifier != f"Select {identifier_column}":
        selected_record = df[df[identifier_column].astype(str) == record_identifier].iloc[0]
        # print('1:', selected_record)
        bot_id = selected_record['Bot_ID'] if 'Bot_ID' in selected_record else None
        # print('2:', bot_id)
        # This should get the existing linked KnowledgeBase IDs for the selected bot
        linked_kb_ids = get_linked_knowledgebase_entries(bot_id) if bot_id else 'no'
        with st.form("Update Record"):
            updated_data = {}
            for col in df.columns:
                if col not in ['Bot_ID', 'ID']:
                    updated_data[col] = st.text_input(col, value=str(selected_record[col]) if pd.notna(selected_record[col]) else "")

            if table_name == "Bots":
                # Fetch all KnowledgeBase entries
                kb_entries = get_knowledgebase_entries()
                kb_options = list(zip(kb_entries['ID'], kb_entries['Content']))
                
                # Set the default values for the multiselect to be the already linked entries
                kb_ids_selected = st.multiselect("Select KnowledgeBase entries to link with the Bot:",
                                                options=kb_options,
                                                default=[kb_id for kb_id in linked_kb_ids],
                                                format_func=lambda x: x[1])

            submitted = st.form_submit_button("Update Record")
            if submitted:
                updated_data = {k: v if v != "" else None for k, v in updated_data.items()}
                update_record(record_identifier, updated_data, table_name, identifier_column)
                if bot_id:
                    update_bot_knowledge_links(bot_id, [kb_id for kb_id, _ in kb_ids_selected])
                st.success("Record updated successfully!")

st.divider()
# Footer
st.markdown("<p style='text-align: center; font-size: 22px;'>Developed By GoChat247</p>", unsafe_allow_html=True)
def load_data(table_name: str) -> pd.DataFrame:
    """
    Loads data from a specified table in the SQLite database.
    
    Args:
        table_name (str): The name of the table to load data from.
    
    Returns:
        pd.DataFrame: The data from the table as a pandas DataFrame.
    """
    try:
    except sqlite3.DatabaseError as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()
