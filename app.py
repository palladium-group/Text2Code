import pandas as pd
import psycopg2
import os
from llama_index.core.objects import (
    ObjectIndex,
    SQLTableNodeMapping,   
    SQLTableSchema,
)
from llama_index.llms.openai import OpenAI
from llama_index.legacy import SQLDatabase
from llama_index.core import VectorStoreIndex
from llama_index.core.retrievers import NLSQLRetriever
from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker

db = "text2code"  # provide the name of your db
host_db = "102.133.145.48"
db_port = '5432'  # or any other port specified by the DBA
db_user = "benedette"
db_password = "4d9r58br07YZM"

# Construct the connection string
SQL_DATABASE_URL = f'postgresql://{db_user}:{db_password}@{host_db}:{db_port}/{db}'
# Create an engine instance
engine = create_engine(
    SQL_DATABASE_URL, connect_args={}, echo=True
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Initialize MetaData
metadata = MetaData()

# Bind the engine to the metadata
metadata.bind = engine

metadata.reflect(engine)
# Base = declarative_base()
db = SessionLocal()

os.environ["OPENAI_API_KEY"] = ""
from llama_index.llms.openai import OpenAI
llm = OpenAI(temperature=0, model="gpt-4o")

# a wrapper around the SQLAlchemy engine to interact with a SQL database. 
tables= ["dreams"]
sql_database = SQLDatabase(engine, include_tables=tables)
sql_database.get_single_table_info

# Set the working directory
os.chdir(r"C:\Users\benedette.otieno\OneDrive - Palladium International, LLC\Documents\DataFI_Lesotho")
# Define the path to your Excel file
excel_file_path = r'DreamsDataset_datadictionary.xlsx' 

# Read the Excel file
dictionary = pd.read_excel(excel_file_path)
#dictionary

table_name_col = 'name*'
parent_col = 'parent'
description_col = 'description'

# Initialize a dictionary to hold table descriptions and columns
data_dictionary = {}
tables = []
columns_info_list = {}
table_obj = []

# Process rows to identify tables and columns
for index, row in dictionary.iterrows():
    parent = row.get(parent_col)
    name = row.get(table_name_col)
    description = row.get(description_col)
    

    # Print debug information
    print(f"Row {index}: parent={parent}, name={name}, description={description}")

    if pd.isna(parent):  # This is a table row
        table_name = name
        table_description = description
        tables.append(table_name)
        if table_name:  # Check if table_name is not None or empty
            columns_info_list[table_name] = {'columns': []}
            data_dictionary[table_name] = {
                'description': table_description if table_description else 'No description available',
                'columns': {}
            }
        else:
            print(f"Warning: Table name is missing for row {index}.")
    else:  # This is a column row
        print(parent.split('.')[1])
        table = parent.split('.')[1]
        if table in data_dictionary:
            column_name = name if name else 'Unnamed Column'
            column_description = description if description else 'No description available'
            data_dictionary[table]['columns'][column_name] = column_description
            column_desc = f"\"{column_name}\"= {column_description}"
            columns_info_list[table]['columns'].append(column_desc)
        else:
            print(f"Warning: Parent table '{parent}' not found for column '{name}'.")
    print(columns_info_list)
    
for table in tables:
    columns_info = " .".join(columns_info_list[table]['columns'])
    table_obj.append((SQLTableSchema(table_name=table, context_str=('description of the table: ' + data_dictionary[table]['description'] + 'These are columns in the table and their description ' + columns_info))))
print(data_dictionary)

table_node_mapping = SQLTableNodeMapping(sql_database)

#store schema information for each table.
table_schema_objs = table_obj  

obj_index = ObjectIndex.from_objects(
    table_schema_objs,
    table_node_mapping,
    VectorStoreIndex,
)

custom_txt2sql_prompt = """Given an input question, construct a syntactically correct SQL query to run, then look at the results of the query and return a comprehensive and detailed answer. Ensure that you:
            - Select only the relevant columns needed to answer the question.
            - Use correct column and table names as provided in the schema description. Avoid querying for columns that do not exist.
            - Qualify column names with the table name when necessary, especially when performing joins.
            - Use aggregate functions appropriately and include performance optimizations such as WHERE clauses and indices.
            - Add additional related information for the user.
            - Use background & definitions provided for more detailed answer. Follow the instructions.
            - Your are provided with several tables each for a different proram area, ensure you retrive the relevant table.
            - do not hallucinate column names. If you can't  find a column name, do not write the sql query say I'm not sure.
    
            
             Special Instructions:
            - Please provide numerical results when asked for numbers and proportions when asked for proportions.
            - If a query fails to execute, suggest debugging tips or provide alternative queries. Ensure to handle common SQL errors gracefully."
            - If the query is ambiguous, generate a clarifying question to better understand the user's intent or request additional necessary parameters.
            - Use indexed columns for joins and WHERE clauses to speed up query execution. Use `EXPLAIN` plans for complex queries to ensure optimal performance.


            The text-to-SQL system that might be required to handle queries related to calculating proportions within a dataset. Your system should be able to generate SQL queries to calculate the proportion of a certain category within a dataset table.

            This example is a hint to show you how to calculate proprtion, 
            Example 1 :
            If a user asks, "What proportion of dreams clients completed primary care package by district", your system should generate a SQL query like:

            SELECT district of service AS District, COUNT(CASE WHEN Completed Primary Package = 'Yes' THEN 1 END) * 1.0 / COUNT(*) AS ProportionCompleted FROM Dreamsdataset GROUP BY District of Service;

            Example 2
            percentage of AGYWs received contraceptive mix

             SELECT (COUNT(CASE WHEN contraceptive_mix IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) AS percentage_of_agyws_received_contraceptive_mix FROM AGYWData WHERE age BETWEEN 10 AND 24;
          Example
             what proportion of beneficiaries aged more than 10 completed primary care package in 2023 

              SELECT COUNT(*) AS total_beneficiaries, COUNT(CASE WHEN Completed Primary Package = 'Yes' THEN 1 END) AS num_completed_primary_care, SELECT (COUNT(CASE WHEN Completed Primary Package = 'Yes' THEN 1 END) * 100.0 / COUNT(*)) AS proportion_completed_primary_care FROM Dreamsdataset WHERE age > 10 AND YEAR(Date of Enrollment/Earliest service date) = 2023;


          
         
        """

from llama_index.core.retrievers import NLSQLRetriever

# default retrieval (return_raw=True)
nl_sql_retriever = NLSQLRetriever(
    sql_database,

)

data_dict= table_schema_objs[0]

question= "% of AGYWs received contraceptive mix"
custom_prompt= (f"Write a SQL query to answer the following question: {question}, using the table, the column names are in double quotes, {data_dict}."
                f"You can refer to {custom_txt2sql_prompt} for examples and instructions on how to generate a SQL statement. ")


response = nl_sql_retriever.retrieve_with_metadata(custom_prompt)
response_list, metadata_dict = response
print(metadata_dict["sql_query"])
sql_query= metadata_dict["sql_query"]