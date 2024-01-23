#importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
from PIL import Image
import git
import subprocess
import json
import os
import logging
import time
from streamlit_folium import folium_static
import geopandas as gpd
import folium
from decimal import Decimal

# Main Streamlit app
if __name__ == "__main__":
    # Main Streamlit code starts
    # SETTING PAGE CONFIGURATIONS
    icon = Image.open("guvi_logo.png")
    st.set_page_config(
        page_title="Phonepe Data Visualization and Exploration | By IRG",
        page_icon= icon,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={'About': """# This application is created for GUVI Capstone Project2 by *IRG!*"""}
    )
    st.title("Phonepe Data Visualization and Exploration | By IRG")
    
    # CREATING OPTION MENU
    with st.sidebar:
        selected = option_menu(None, ["Home", "Explore Data", "Insights"], 
                                icons=["house-door-fill","search","lightbulb"],
                                default_index=0,
                                orientation="vertical",
                                styles={"nav-link": {"font-size": "24px", "text-align": "centre", "margin": "0px", 
                                                        "--hover-color": "#FF4B4B"},
                                        "icon": {"font-size": "24px"},
                                        "container" : {"max-width": "7000px"},
                                        "nav-link-selected": {"background-color": "Reds"}})
    # MySQL connection parameters
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': #'XXXXXXX', Enter your MySQL Password
        'auth_plugin': 'mysql_native_password',  # Use a compatible authentication plugin

    }

    # Establish the connection and create a cursor
    mydb = sql.connect(**config)
    mycursor = mydb.cursor(buffered=True)

    # Create the 'phonepe_db' database if it doesn't exist
    mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_db")

    # Commit the changes and close the connection
    mydb.commit()
    mydb.close()

    # To connect to the 'phonepe_db' database
    config['database'] = 'phonepe_db'
    mydb = sql.connect(**config)
    mycursor = mydb.cursor(buffered=True)

    #aggregated insurance table (AI)
    mycursor.execute('''CREATE TABLE if not exists aggregated_insurance (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Insurance_type varchar(50),
                        Insurance_count bigint,
                        Insurance_amount bigint
                        )'''
    )

    #aggregated transaction table (AT)
    mycursor.execute('''CREATE TABLE if not exists aggregated_transaction (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Transaction_type varchar(50),
                        Transaction_count bigint,
                        Transaction_amount bigint
                        )'''
    )

    #aggregated user table (AU)
    mycursor.execute('''CREATE TABLE if not exists aggregated_user (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Brands varchar(50),
                        Transaction_count bigint,
                        Percentage float
                        )'''
    )

    #map_insurance_table (MI)
    mycursor.execute('''CREATE TABLE if not exists map_insurance (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        District varchar(50),
                        Transaction_count bigint,
                        Transaction_amount float
                        )'''
    )

    #map_transaction_table (MT)
    mycursor.execute('''CREATE TABLE if not exists map_transaction (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        District varchar(50),
                        Transaction_count bigint,
                        Transaction_amount float
                        )'''
    )

    #map_user_table (MU)
    mycursor.execute('''CREATE TABLE if not exists map_user (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        District varchar(50),
                        RegisteredUser bigint,
                        AppOpens bigint
                        )'''
    )
    
    #top_insurance_table (TI)
    mycursor.execute('''CREATE TABLE if not exists top_insurance (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Pincodes int,
                        Transaction_count bigint,
                        Transaction_amount bigint
                        )'''
    )
    
    #top_transaction_table (TT)
    mycursor.execute('''CREATE TABLE if not exists top_transaction (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Pincodes int,
                        Transaction_count bigint,
                        Transaction_amount bigint
                        )'''
    )

    #top_user_table (TU)
    mycursor.execute('''CREATE TABLE if not exists top_user (
                        States varchar(50),
                        Years int,
                        Quarter int,
                        Pincodes int,
                        RegisteredUser bigint
                        )'''
    )
    # Commit the changes and close the connection
    mydb.commit()
    mydb.close()

    # Define function to load / refresh data
    def dataclone():
        # Directory to clone the repository
        repo_directory = "pulse"

        # Check if the repository already exists
        if os.path.exists(repo_directory):
            # If the repository exists, fetch the latest changes and reset to remote HEAD
            repo = git.Repo(repo_directory)
            origin = repo.remote(name='origin')
            origin.fetch()
            repo.git.reset('--hard', 'origin/master')
        else:
            # If the repository doesn't exist, clone it
            clone_command = f"git clone --depth 1 https://github.com/PhonePe/pulse.git {repo_directory}"
            subprocess.run(clone_command, shell=True)
    
    # Define function to process and load to MySQL
    def dataprocess():

        # Log start time
        start_time = time.time()
        logging.info("Script execution started.")

        # Get the current folder's path
        current_folder = os.getcwd()
        # Define the fixed path from pulse folder
        agg_insr_file_path = "pulse/data/aggregated/insurance/country/india/state/"
        agg_tran_file_path = "pulse/data/aggregated/transaction/country/india/state/"
        agg_user_file_path = "pulse/data/aggregated/user/country/india/state/"
        
        map_insr_file_path = "pulse/data/map/insurance/hover/country/india/state/"
        map_tran_file_path = "pulse/data/map/transaction/hover/country/india/state/"
        map_user_file_path = "pulse/data/map/user/hover/country/india/state/"

        top_insr_file_path = "pulse/data/top/insurance/country/india/state/"
        top_tran_file_path = "pulse/data/top/transaction/country/india/state/"
        top_user_file_path = "pulse/data/top/user/country/india/state/"

        # Aggregate_Insurance (AI)
        # Combine the current folder path with the remaining fixed path
        path_ai = os.path.join(current_folder, agg_insr_file_path)
        # Convert backslashes to forward slashes in the path
        path_ai = path_ai.replace("\\", "/")

        agg_insur_list= os.listdir(path_ai)
        columns_ai= {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }

        for state in agg_insur_list:
            cur_states =path_ai+state+"/"
            agg_year_list = os.listdir(cur_states)
            
            for year in agg_year_list:
                cur_years = cur_states+year+"/"
                agg_file_list = os.listdir(cur_years)

                for file in agg_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    A = json.load(data)

                    for i in A["data"]["transactionData"]:
                        name = i["name"]
                        count = i["paymentInstruments"][0]["count"]
                        amount = i["paymentInstruments"][0]["amount"]
                        columns_ai["Insurance_type"].append(name)
                        columns_ai["Insurance_count"].append(count)
                        columns_ai["Insurance_amount"].append(amount)
                        columns_ai["States"].append(state)
                        columns_ai["Years"].append(year)
                        columns_ai["Quarter"].append(int(file.strip(".json")))


        aggre_insurance = pd.DataFrame(columns_ai)

        aggre_insurance["States"] = aggre_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        aggre_insurance["States"] = aggre_insurance["States"].str.replace("-"," ")
        aggre_insurance["States"] = aggre_insurance["States"].str.title()
        aggre_insurance['States'] = aggre_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Aggregate_Transaction (AT)
        # Combine the current folder path with the remaining fixed path
        path_at = os.path.join(current_folder, agg_tran_file_path)
        # Convert backslashes to forward slashes in the path
        path_at = path_at.replace("\\", "/")

        agg_tran_list = os.listdir(path_at)
        columns_at ={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }

        for state in agg_tran_list:
            cur_states =path_at+state+"/"
            agg_year_list = os.listdir(cur_states)
            
            for year in agg_year_list:
                cur_years = cur_states+year+"/"
                agg_file_list = os.listdir(cur_years)

                for file in agg_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    B = json.load(data)

                    for i in B["data"]["transactionData"]:
                        name = i["name"]
                        count = i["paymentInstruments"][0]["count"]
                        amount = i["paymentInstruments"][0]["amount"]
                        columns_at["Transaction_type"].append(name)
                        columns_at["Transaction_count"].append(count)
                        columns_at["Transaction_amount"].append(amount)
                        columns_at["States"].append(state)
                        columns_at["Years"].append(year)
                        columns_at["Quarter"].append(int(file.strip(".json")))

        aggre_transaction = pd.DataFrame(columns_at)

        aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
        aggre_transaction["States"] = aggre_transaction["States"].str.title()
        aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Aggregate_User (AU)
        # Combine the current folder path with the remaining fixed path
        path_au = os.path.join(current_folder, agg_user_file_path)
        # Convert backslashes to forward slashes in the path
        path_au = path_au.replace("\\", "/")
                
        agg_user_list = os.listdir(path_au)
        columns_au = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

        for state in agg_user_list:
            cur_states = path_au+state+"/"
            agg_year_list = os.listdir(cur_states)
            
            for year in agg_year_list:
                cur_years = cur_states+year+"/"
                agg_file_list = os.listdir(cur_years)
                
                for file in agg_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    C = json.load(data)
                    try:

                        for i in C["data"]["usersByDevice"]:
                            brand = i["brand"]
                            count = i["count"]
                            percentage = i["percentage"]
                            columns_au["Brands"].append(brand)
                            columns_au["Transaction_count"].append(count)
                            columns_au["Percentage"].append(percentage)
                            columns_au["States"].append(state)
                            columns_au["Years"].append(year)
                            columns_au["Quarter"].append(int(file.strip(".json")))
                    
                    except:
                        pass

        aggre_user = pd.DataFrame(columns_au)

        aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
        aggre_user["States"] = aggre_user["States"].str.title()
        aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Map_Insurance (MI)
        # Combine the current folder path with the remaining fixed path
        path_mi = os.path.join(current_folder, map_insr_file_path)
        # Convert backslashes to forward slashes in the path
        path_mi = path_mi.replace("\\", "/")

        map_insur_list= os.listdir(path_mi)
        columns_mi= {"States":[], "Years":[], "Quarter":[], "District":[], "Transaction_count":[],"Transaction_amount":[] }

        for state in map_insur_list:
            cur_states =path_mi+state+"/"
            agg_year_list = os.listdir(cur_states)
            
            for year in agg_year_list:
                cur_years = cur_states+year+"/"
                agg_file_list = os.listdir(cur_years)

                for file in agg_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    D = json.load(data)

                    for i in D["data"]["hoverDataList"]:
                        name = i["name"]
                        count = i["metric"][0]["count"]
                        amount = i["metric"][0]["amount"]
                        columns_mi["District"].append(name)
                        columns_mi["Transaction_count"].append(count)
                        columns_mi["Transaction_amount"].append(amount)
                        columns_mi["States"].append(state)
                        columns_mi["Years"].append(year)
                        columns_mi["Quarter"].append(int(file.strip(".json")))


        map_insurance = pd.DataFrame(columns_mi)

        map_insurance["States"] = map_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        map_insurance["States"] = map_insurance["States"].str.replace("-"," ")
        map_insurance["States"] = map_insurance["States"].str.title()
        map_insurance['States'] = map_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Map_Transaction (MT)
        # Combine the current folder path with the remaining fixed path
        path_mt = os.path.join(current_folder, map_tran_file_path)
        # Convert backslashes to forward slashes in the path
        path_mt = path_mt.replace("\\", "/")

        map_tran_list = os.listdir(path_mt)
        columns_mt = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}

        for state in map_tran_list:
            cur_states = path_mt+state+"/"
            map_year_list = os.listdir(cur_states)
            
            for year in map_year_list:
                cur_years = cur_states+year+"/"
                map_file_list = os.listdir(cur_years)
                
                for file in map_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    E = json.load(data)

                    for i in E['data']["hoverDataList"]:
                        name = i["name"]
                        count = i["metric"][0]["count"]
                        amount = i["metric"][0]["amount"]
                        columns_mt["District"].append(name)
                        columns_mt["Transaction_count"].append(count)
                        columns_mt["Transaction_amount"].append(amount)
                        columns_mt["States"].append(state)
                        columns_mt["Years"].append(year)
                        columns_mt["Quarter"].append(int(file.strip(".json")))

        map_transaction = pd.DataFrame(columns_mt)

        map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
        map_transaction["States"] = map_transaction["States"].str.title()
        map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
                        
        # Map_User (MU)
        # Combine the current folder path with the remaining fixed path
        path_mu = os.path.join(current_folder, map_user_file_path)
        # Convert backslashes to forward slashes in the path
        path_mu = path_mu.replace("\\", "/")

        map_user_list = os.listdir(path_mu)
        columns_mu = {"States":[], "Years":[], "Quarter":[], "District":[], "RegisteredUser":[], "AppOpens":[]}

        for state in map_user_list:
            cur_states = path_mu+state+"/"
            map_year_list = os.listdir(cur_states)
            
            for year in map_year_list:
                cur_years = cur_states+year+"/"
                map_file_list = os.listdir(cur_years)
                
                for file in map_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    F = json.load(data)

                    for i in F["data"]["hoverData"].items():
                        district = i[0]
                        registereduser = i[1]["registeredUsers"]
                        appopens = i[1]["appOpens"]
                        columns_mu["District"].append(district)
                        columns_mu["RegisteredUser"].append(registereduser)
                        columns_mu["AppOpens"].append(appopens)
                        columns_mu["States"].append(state)
                        columns_mu["Years"].append(year)
                        columns_mu["Quarter"].append(int(file.strip(".json")))

        map_user = pd.DataFrame(columns_mu)

        map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        map_user["States"] = map_user["States"].str.replace("-"," ")
        map_user["States"] = map_user["States"].str.title()
        map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Top_Insurance (TI)
        # Combine the current folder path with the remaining fixed path
        path_ti = os.path.join(current_folder, top_insr_file_path)
        # Convert backslashes to forward slashes in the path
        path_ti = path_ti.replace("\\", "/")

        top_insurance_list = os.listdir(path_ti)
        columns_ti = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

        for state in top_insurance_list:
            cur_states = path_ti+state+"/"
            top_year_list = os.listdir(cur_states)

            for year in top_year_list:
                cur_years = cur_states+year+"/"
                top_file_list = os.listdir(cur_years)

                for file in top_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    G = json.load(data)

                    for i in G["data"]["pincodes"]:
                        entityName = i["entityName"]
                        count = i["metric"]["count"]
                        amount = i["metric"]["amount"]
                        columns_ti["Pincodes"].append(entityName)
                        columns_ti["Transaction_count"].append(count)
                        columns_ti["Transaction_amount"].append(amount)
                        columns_ti["States"].append(state)
                        columns_ti["Years"].append(year)
                        columns_ti["Quarter"].append(int(file.strip(".json")))

        top_insurance = pd.DataFrame(columns_ti)

        top_insurance["States"] = top_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        top_insurance["States"] = top_insurance["States"].str.replace("-"," ")
        top_insurance["States"] = top_insurance["States"].str.title()
        top_insurance['States'] = top_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    
        # Top_Transaction (TT)
        # Combine the current folder path with the remaining fixed path
        path_tt = os.path.join(current_folder, top_tran_file_path)
        # Convert backslashes to forward slashes in the path
        path_tt = path_tt.replace("\\", "/")
    
        top_tran_list = os.listdir(path_tt)
        columns_tt = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

        for state in top_tran_list:
            cur_states = path_tt+state+"/"
            top_year_list = os.listdir(cur_states)
            
            for year in top_year_list:
                cur_years = cur_states+year+"/"
                top_file_list = os.listdir(cur_years)
                
                for file in top_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    H = json.load(data)

                    for i in H["data"]["pincodes"]:
                        entityName = i["entityName"]
                        count = i["metric"]["count"]
                        amount = i["metric"]["amount"]
                        columns_tt["Pincodes"].append(entityName)
                        columns_tt["Transaction_count"].append(count)
                        columns_tt["Transaction_amount"].append(amount)
                        columns_tt["States"].append(state)
                        columns_tt["Years"].append(year)
                        columns_tt["Quarter"].append(int(file.strip(".json")))

        top_transaction = pd.DataFrame(columns_tt)

        top_transaction["States"] = top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
        top_transaction["States"] = top_transaction["States"].str.title()
        top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Top_User (TU)
        # Combine the current folder path with the remaining fixed path
        path_tu = os.path.join(current_folder, top_user_file_path)
        # Convert backslashes to forward slashes in the path
        path_tu = path_tu.replace("\\", "/")

        top_user_list = os.listdir(path_tu)
        columns_tu = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

        for state in top_user_list:
            cur_states = path_tu+state+"/"
            top_year_list = os.listdir(cur_states)

            for year in top_year_list:
                cur_years = cur_states+year+"/"
                top_file_list = os.listdir(cur_years)

                for file in top_file_list:
                    cur_files = cur_years+file
                    data = open(cur_files,"r")
                    I = json.load(data)

                    for i in I["data"]["pincodes"]:
                        name = i["name"]
                        registeredusers = i["registeredUsers"]
                        columns_tu["Pincodes"].append(name)
                        columns_tu["RegisteredUser"].append(registeredusers)
                        columns_tu["States"].append(state)
                        columns_tu["Years"].append(year)
                        columns_tu["Quarter"].append(int(file.strip(".json")))

        top_user = pd.DataFrame(columns_tu)

        top_user["States"] = top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
        top_user["States"] = top_user["States"].str.replace("-"," ")
        top_user["States"] = top_user["States"].str.title()
        top_user['States'] = top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

        # Reconnect to the 'phonepe_db' database
        config['database'] = 'phonepe_db'
        mydb = sql.connect(**config)
        mycursor = mydb.cursor(buffered=True)

        # Function to bulk insert data
        def bulk_insert(table_name, columns, data):
            insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
            mycursor.executemany(insert_query, data)
            mydb.commit()
            inserted_rows = mycursor.rowcount  # Count of rows inserted
            return inserted_rows

        # Dataframes
        dataframes = [
            aggre_insurance,
            aggre_transaction,
            aggre_user,
            map_insurance,
            map_transaction,
            map_user,
            top_insurance,
            top_transaction,
            top_user
        ]

        # Corresponding table names and unique columns
        table_info = {
            'aggregated_insurance': ['States', 'Years', 'Quarter', 'Insurance_type'],
            'aggregated_transaction': ['States', 'Years', 'Quarter', 'Transaction_type'],
            'aggregated_user': ['States', 'Years', 'Quarter', 'Brands'],
            'map_insurance': ['States', 'Years', 'Quarter', 'District'],
            'map_transaction': ['States', 'Years', 'Quarter', 'District'],
            'map_user': ['States', 'Years', 'Quarter', 'District'],
            'top_insurance': ['States', 'Years', 'Quarter', 'Pincodes'],
            'top_transaction': ['States', 'Years', 'Quarter', 'Pincodes'],
            'top_user': ['States', 'Years', 'Quarter', 'Pincodes'],
        }

        # Loop through dataframes and delete all rows before inserting data
        for table_name in table_info:
            delete_query = f"DELETE FROM {table_name}"
            mycursor.execute(delete_query)
            logging.info(f"All rows deleted from {table_name}.")

        # Loop through dataframes and insert data using bulk insert
        total_bulk_inserted_rows = 0
        for df, table_name in zip(dataframes, table_info):
            columns = df.columns.tolist()
            data_to_insert = [tuple(row) for _, row in df.iterrows()]
            if data_to_insert:
                inserted_rows = bulk_insert(table_name, columns, data_to_insert)
                total_bulk_inserted_rows += inserted_rows
                logging.info(f"Inserted {inserted_rows} rows into {table_name} using bulk insert")
            else:
                logging.info(f"No data to insert into {table_name} using bulk insert")

        # Display total inserted rows
        logging.info(f"Total records loaded in MySQL: {total_bulk_inserted_rows}")
        st.write(f"Total records loaded in MySQL: {total_bulk_inserted_rows}")

        # Log end time and calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time

        # Convert execution time to mm:ss format
        minutes, seconds = divmod(execution_time, 60)
        time_format = "{:02}:{:02}".format(int(minutes), int(seconds))

        logging.info(f"Processed & loaded in {execution_time:.2f} seconds")
        # Display execution time in Streamlit
        st.write(f"Processed & loaded in {time_format} minutes:seconds")

        # Close the database connection
        mydb.close()

    # Function to format large numbers in millions (M) or billions (B)
    def format_large_numbers(cell):
        if isinstance(cell, str):
            try:
                if cell.endswith("M"):
                    return float(cell[:-1]) * 1e6
                elif cell.endswith("B"):
                    return float(cell[:-1]) * 1e9
                else:
                    return float(cell)
            except ValueError:
                return cell
        elif isinstance(cell, float):
            return cell
        else:
            return cell

    # Function to fetch data from MySQL
    def fetch_data(query):
        mydb = sql.connect(**config)
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        columns = [column[0] for column in mycursor.description]
        data = mycursor.fetchall()
        # Close the database connection
        mydb.close()

        # Convert Decimal columns to float and format large numbers
        data = [
            [
                format_large_numbers(float(cell)) if isinstance(cell, Decimal) else cell
                for cell in row
            ]
            for row in data
        ]

        df = pd.DataFrame(data, columns=columns)
        return df

    # Function to plot India map
    def plot_india_map(geojson_url, gdf, base_column, hover_columns, click_columns):
        st.subheader("India Map Visualization")

        # Load India GeoJSON data
        india_geojson = gpd.read_file(geojson_url)

        # Merge GeoJSON data with DataFrame
        merged_gdf = india_geojson.merge(gdf, left_on="ST_NM", right_on="States", how="left").fillna(0)

        # Your Mapbox Access Token
        mapbox_access_token = #'Enter Mapbox Token' Register for free and get the token from mapbox
        mapbox_tile_url = "https://api.mapbox.com/styles/v1/mapbox/dark-v10/tiles/256/{z}/{x}/{y}@2x?access_token=" + mapbox_access_token

        # Create a Folium map with the Mapbox Dark tile layer
        m = folium.Map(location=[20.5937, 78.9629], zoom_start=4)
        folium.TileLayer(tiles=mapbox_tile_url, attr='Mapbox', name='Mapbox Dark').add_to(m)

        # Choropleth map based on base_column
        folium.Choropleth(
            geo_data=geojson_url,
            name="choropleth",
            data=gdf,
            columns=["States", base_column],
            key_on="feature.properties.ST_NM",
            fill_color="Reds",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=base_column,
        ).add_to(m)

        # Add hover information
        for col in hover_columns:
            folium.features.GeoJson(
                merged_gdf,
                name=f"Hover: {col}",
                style_function=lambda x: {
                    "fillColor": "transparent",
                    "color": "transparent",
                    "fillOpacity": 0,
                },
                tooltip=folium.features.GeoJsonTooltip(
                    fields=[col], aliases=[col], labels=True, sticky=True
                ),
            ).add_to(m)

        # Add click information
        for col in click_columns:
            folium.features.GeoJson(
                merged_gdf,
                name=f"Click: {col}",
                style_function=lambda x: {
                    "fillColor": "transparent",
                    "color": "transparent",
                    "fillOpacity": 0,
                },
                tooltip=folium.features.GeoJsonTooltip(
                    fields=[col], aliases=[col], labels=True, sticky=True
                ),
            ).add_to(m)

        # Display the map
        folium_static(m)

    # Load India GeoJSON data
    india_geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    india_geojson = gpd.read_file(india_geojson_url)

    # Function to show selectbox list for year and quarter in Payment screem
    def payment_year_quarter_list():
        # Fetch the latest available Year and Quarter from the aggregated_transaction table
        query_latest_transaction = "SELECT MAX(Years) AS LatestYear, MAX(Quarter) AS LatestQuarter FROM aggregated_transaction"
        latest_transaction_data = fetch_data(query_latest_transaction)

        # Get the latest Year and Quarter values
        default_year_transaction = latest_transaction_data["LatestYear"].values[0] if not latest_transaction_data.empty else 2023
        
        # Fetch the maximum quarter for the selected year
        query_max_quarter_transaction = f"SELECT MAX(Quarter) AS MaxQuarter FROM aggregated_transaction WHERE Years = {default_year_transaction}"
        max_quarter_transaction_data = fetch_data(query_max_quarter_transaction)
        default_quarter_transaction = max_quarter_transaction_data["MaxQuarter"].values[0] if not max_quarter_transaction_data.empty else 1

        # Fetch unique values for Years and Quarters from the aggregated_transaction table
        query_years_transaction = "SELECT DISTINCT Years FROM aggregated_transaction"
        query_quarters_transaction = "SELECT DISTINCT Quarter FROM aggregated_transaction"

        years_options_transaction = fetch_data(query_years_transaction)
        quarters_options_transaction = fetch_data(query_quarters_transaction)

        # Convert the fetched data to a list
        years_list_transaction = list(years_options_transaction["Years"])
        quarters_list_transaction = list(quarters_options_transaction["Quarter"])
        
        return years_list_transaction, quarters_list_transaction, default_year_transaction, default_quarter_transaction

    # Function to plot India map for Transaction
    def plot_transaction_map(year, quarter):
        # Fetch data for Transaction map
        query = f"SELECT States, SUM(Transaction_count) as Total_Transaction_count, SUM(Transaction_amount) as Total_Transaction_amount FROM map_transaction WHERE Years={year} AND Quarter={quarter} GROUP BY States"
        transaction_data = fetch_data(query)
        # Plot India map for Transaction
        plot_india_map(india_geojson_url, transaction_data, "Total_Transaction_amount", ["Total_Transaction_count", "Total_Transaction_amount"], ["Total_Transaction_count", "Total_Transaction_amount"])

    # Function to show Transaction details for selected state
    def show_transaction_details(year, quarter,selected_state):
        st.subheader(f"Transaction Details for {selected_state}")
        # Fetch Transaction details for the selected state
        query = f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}'"
        transaction_details = fetch_data(query)

        if transaction_details.empty:
            st.write(f"No data available for the selected options")
        else:
            transaction_details.index = transaction_details.index + 1
            st.write(transaction_details)

    # Function to show Top 10 Districts and Pincodes
    def show_top_districts_pincodes(year, quarter, selected_state):
        col1,col2= st.columns(2)
        col1.write(f"**Top 10 Districts for Transaction in {selected_state}**")
        # Fetch Top 10 Districts data
        query_districts = f"SELECT District, SUM(Transaction_amount) as Total_Transaction_amount FROM map_transaction WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY District ORDER BY Total_Transaction_amount DESC LIMIT 10"
        top_districts_data = fetch_data(query_districts)
        top_districts_data.index = top_districts_data.index + 1
        col1.write(top_districts_data)

        col2.write(f"**Top 10 Pincodes for Transaction in {selected_state}**")
        # Fetch Top 10 Pincodes data
        query_pincodes = f"SELECT Pincodes, SUM(Transaction_amount) as Total_Transaction_amount FROM top_transaction WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY Pincodes ORDER BY Total_Transaction_amount DESC LIMIT 10"
        top_pincodes_data = fetch_data(query_pincodes)
        top_pincodes_data.index = top_pincodes_data.index + 1
        col2.write(top_pincodes_data)

    # Function to plot India map for User
    def plot_user_map(year, quarter):
        # Fetch data for User map
        query = f"SELECT States, SUM(RegisteredUser) as Total_RegisteredUser FROM map_user WHERE Years={year} AND Quarter={quarter} GROUP BY States"
        user_data = fetch_data(query)
        # Plot India map for User
        plot_india_map(india_geojson_url, user_data, "Total_RegisteredUser", ["Total_RegisteredUser"], ["Total_RegisteredUser"])

    # Function to show User details for selected state
    def show_user_details(year, quarter, selected_state):
        st.subheader(f"Mobile model wise usage details for {selected_state}")
        # Fetch User details for the selected state
        query = f"SELECT Brands, Transaction_count, Percentage FROM aggregated_user WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}'"
        user_details = fetch_data(query)
        if user_details.empty:
            st.write(f"No data available for the selected options")
        else:
            # Display DataFrame with index starting from 1
            user_details.index = user_details.index + 1
            st.write(user_details)

    # Function to show Top 10 Districts and Pincodes for User
    def show_top_user_districts_pincodes(year, quarter, selected_state):
        col1,col2= st.columns(2)
        col1.write(f"**Top 10 Districts by Usage in {selected_state}**")
        # Fetch Top 10 User Districts data
        query_districts = f"SELECT District, SUM(RegisteredUser) as Total_RegisteredUser FROM map_user WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY District ORDER BY Total_RegisteredUser DESC LIMIT 10"
        top_user_districts_data = fetch_data(query_districts)
        top_user_districts_data.index = top_user_districts_data.index + 1
        col1.write(top_user_districts_data)

        col2.write(f"**Top 10 Pincodes by Usage in {selected_state}**")
        # Fetch Top 10 User Pincodes data
        query_pincodes = f"SELECT Pincodes, SUM(RegisteredUser) as Total_RegisteredUser FROM top_user WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY Pincodes ORDER BY Total_RegisteredUser DESC LIMIT 10"
        top_user_pincodes_data = fetch_data(query_pincodes)
        top_user_pincodes_data.index = top_user_pincodes_data.index + 1
        col2.write(top_user_pincodes_data)
        
    def insurance_year_quarter_list():
        # Fetch the latest available Year and Quarter from the aggregated_insurance table
        query_latest_insurance = "SELECT MAX(Years) AS LatestYear, MAX(Quarter) AS LatestQuarter FROM aggregated_insurance"
        latest_insurance_data = fetch_data(query_latest_insurance)

        # Get the latest Year and Quarter values
        default_year_insurance = latest_insurance_data["LatestYear"].values[0]
        
        # Fetch the maximum quarter for the selected year
        query_max_quarter_insurance = f"SELECT MAX(Quarter) AS MaxQuarter FROM aggregated_insurance WHERE Years = {default_year_insurance}"
        max_quarter_insurance_data = fetch_data(query_max_quarter_insurance)
        default_quarter_insurance = max_quarter_insurance_data["MaxQuarter"].values[0] if not max_quarter_insurance_data.empty else 1

        # Fetch unique values for Years and Quarters from the aggregated_insurance table
        query_years_insurance = "SELECT DISTINCT Years FROM aggregated_insurance"
        query_quarters_insurance = "SELECT DISTINCT Quarter FROM aggregated_insurance"

        years_options_insurance = fetch_data(query_years_insurance)
        quarters_options_insurance = fetch_data(query_quarters_insurance)

        # Convert the fetched data to a list
        years_list_insurance = list(years_options_insurance["Years"])
        quarters_list_insurance = list(quarters_options_insurance["Quarter"])
        return years_list_insurance, quarters_list_insurance, default_year_insurance, default_quarter_insurance

    # Function to plot India map for Insurance
    def plot_insurance_map(year, quarter):
        # Fetch data for Insurance map
        query = f"SELECT States, SUM(Transaction_count) as Total_Insurance_count, SUM(Transaction_amount) as Total_Insurance_amount, ROUND(AVG(Transaction_amount),0) as Avg_Insurance_amount FROM map_insurance WHERE Years={year} AND Quarter={quarter} GROUP BY States"
        insurance_data = fetch_data(query)
        # Plot India map for Insurance
        plot_india_map(india_geojson_url, insurance_data, "Total_Insurance_amount", ["Total_Insurance_count", "Total_Insurance_amount", "Avg_Insurance_amount"], ["Total_Insurance_count", "Total_Insurance_amount", "Avg_Insurance_amount"])

    # Function to show Insurance details for selected state
    def show_insurance_details(year, quarter, selected_state):
        st.subheader(f"Insurance Details for {selected_state}")
        # Fetch Insurance details for the selected state
        query = f"SELECT Insurance_count, Insurance_amount FROM aggregated_insurance WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}'"
        insurance_details = fetch_data(query)
        if insurance_details.empty:
            st.write(f"No data available for the selected options")
        else:
            insurance_details.index = insurance_details.index + 1
            st.write(insurance_details)

    # Function to show Top 10 Districts and Pincodes for Insurance
    def show_top_districts_pincodes_insurance(year, quarter, selected_state):
        col1,col2= st.columns(2)
        col1.write(f"**Top 10 Districts for Insurance in {selected_state}**")
        # Fetch Top 10 Districts data for Insurance
        query_districts = f"SELECT District, SUM(Transaction_amount) as Total_Insurance_amount FROM map_insurance WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY District ORDER BY Total_Insurance_amount DESC LIMIT 10"
        top_districts_data = fetch_data(query_districts)
        top_districts_data.index = top_districts_data.index + 1
        col1.write(top_districts_data)

        col2.write(f"**Top 10 Pincodes for Insurance in {selected_state}**")
        # Fetch Top 10 Pincodes data for Insurance
        query_pincodes = f"SELECT Pincodes, SUM(Transaction_amount) as Total_Insurance_amount FROM top_insurance WHERE Years={year} AND Quarter={quarter} AND States='{selected_state}' GROUP BY Pincodes ORDER BY Total_Insurance_amount DESC LIMIT 10"
        top_pincodes_data = fetch_data(query_pincodes)
        top_pincodes_data.index = top_pincodes_data.index + 1
        col2.write(top_pincodes_data)
    # Close the database connection
    mydb.close()

    def datainsights():
        # Reconnect to the 'phonepe_db' database
        config['database'] = 'phonepe_db'
        mydb = sql.connect(**config)
        mycursor = mydb.cursor(buffered=True)

        #Aggregated_transsaction
        mycursor.execute("select * from aggregated_transaction;")
        mydb.commit()
        table1 = mycursor.fetchall()
        Aggre_transaction = pd.DataFrame(table1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

        #Aggregated_user
        mycursor.execute("select * from aggregated_user")
        mydb.commit()
        table2 = mycursor.fetchall()
        Aggre_user = pd.DataFrame(table2,columns = ("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))

        #Map_insurance
        mycursor.execute("select * from map_insurance")
        mydb.commit()
        table3 = mycursor.fetchall()

        Map_insurance = pd.DataFrame(table3,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count","Transaction_amount"))

        #Map_transaction
        mycursor.execute("select * from map_transaction")
        mydb.commit()
        table3 = mycursor.fetchall()
        Map_transaction = pd.DataFrame(table3,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

        #Map_user
        mycursor.execute("select * from map_user")
        mydb.commit()
        table4 = mycursor.fetchall()
        Map_user = pd.DataFrame(table4,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"))

        # 1. Top Brands Of Mobiles Used
        def ques1():
            brand = Aggre_user[["Brands", "Years", "Transaction_count"]]
            brand1 = brand.groupby(["Brands", "Years"])["Transaction_count"].sum().sort_values(ascending=False)
            brand2 = pd.DataFrame(brand1).reset_index()

            # Creating a bar chart
            fig_brands = px.sunburst(brand2, path=['Years','Brands'], 
                        values='Transaction_count',
                        color='Transaction_count',
                        color_continuous_scale='Reds',
                        title="Transaction Count and Percentage by States, Brands, and Years")

            fig_brands.update_layout(margin=dict(t=0, l=0, r=0, b=0))
            return st.plotly_chart(fig_brands)

        # 2. Top 10 Districts With Highest Transaction Amount
        def ques2():
            htd= Map_transaction[["Districts", "Transaction_amount"]]
            htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=False)
            htd2= pd.DataFrame(htd1).head(10).reset_index()

            fig_htd= px.bar(htd2, x='Districts', y='Transaction_amount',
                            title='Top 10 Districts with Lowest Transaction Amount',
                            color='Transaction_amount',
                            color_continuous_scale='Reds')  # Using Reds color scale

            # Customizing the layout
            fig_htd.update_layout(
                xaxis_title="Districts",
                yaxis_title="Transaction Amount",
                plot_bgcolor="white",  # Sets the background color for the plot
                coloraxis_showscale=False  # Hides the color scale if not needed
            )
            return st.plotly_chart(fig_htd)

        # 3. Bottom 10 Districts With Lowest Transaction Amount
        def ques3():
            ltd = Map_transaction[["Districts", "Transaction_amount"]]
            ltd1 = ltd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=True)
            ltd2 = pd.DataFrame(ltd1).head(10).reset_index()

            # Creating a bar chart with a red color scheme
            fig_htd = px.bar(ltd2, x='Districts', y='Transaction_amount',
                            title='Top 10 Districts with Lowest Transaction Amount',
                            color='Transaction_amount',
                            color_continuous_scale='Reds')  # Using Reds color scale

            # Customizing the layout
            fig_htd.update_layout(
                xaxis_title="Districts",
                yaxis_title="Transaction Amount",
                plot_bgcolor="white",  # Sets the background color for the plot
                coloraxis_showscale=False  # Hides the color scale if not needed
            )
            return st.plotly_chart(fig_htd)

        # 4. Top 10 States With App Usage
        def ques4():
            sa= Map_user[["States", "AppOpens"]]
            sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=False)
            sa2= pd.DataFrame(sa1).reset_index().head(10)

            fig_sa= px.bar(sa2, x= "States", y= "AppOpens", 
                        title="Top 10 States With App Usage",
                        color="AppOpens",
                        color_continuous_scale="Reds"
                        )
            return st.plotly_chart(fig_sa)

        # 5. Bottom 10 States With App Usage
        def ques5():
            sa= Map_user[["States", "AppOpens"]]
            sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=True)
            sa2= pd.DataFrame(sa1).reset_index().head(10)

            fig_sa= px.bar(sa2, x= "States", y= "AppOpens", 
                        title="Lowest 10 States With App Usage",
                        color="AppOpens",
                        color_continuous_scale="Reds"
                        )
            return st.plotly_chart(fig_sa)

        # 6. States With Lowest Transaction Count
        def ques6():
            stc= Aggre_transaction[["States", "Transaction_count"]]
            stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
            stc2= pd.DataFrame(stc1).reset_index().head(10)

            fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", 
                            title= "States With Lowest Transaction Count",
                            color="Transaction_count",
                            color_continuous_scale="Reds"
                            )
            return st.plotly_chart(fig_stc)

        # 7. Quarter on Quarter Growth of Transactions
        def ques7():
            lt= Aggre_transaction[["Years", "Quarter", "Transaction_count", "Transaction_amount"]]
            lt1= lt.groupby(["Years", "Quarter","Transaction_count"])["Transaction_amount"].sum().sort_values(ascending= True)
            lt2= pd.DataFrame(lt1).reset_index()

            fig_lts= px.sunburst(lt2, path=['Quarter','Years'], 
                        values='Transaction_amount',
                        color='Transaction_count',
                        color_continuous_scale='Reds',
                        title="QoQ Comparison")

            fig_lts.update_layout(margin=dict(t=0, l=0, r=0, b=0))
            return st.plotly_chart(fig_lts)

        # 8. Year on Year Growth by App Usage
        def ques8():
            stc=  Map_user[["States", "Years", "AppOpens"]]
            stc1= stc.groupby(['States', 'Years'])['AppOpens'].sum().reset_index()
            
            fig_stc = px.area(stc1, x='Years', y='AppOpens', color='States',
                        title='App Usage by Year and State')
            return st.plotly_chart(fig_stc)

        # 9. Year on Year Growth by Transaction Categories
        def ques9():
            ht= Aggre_transaction[["Transaction_type","Years", "Transaction_amount"]]
            ht1= ht.groupby(["Transaction_type","Years"])["Transaction_amount"].sum().reset_index()

            fig_cts= px.area(ht1, x='Years', y='Transaction_amount', color='Transaction_type',
                        title='Transaction Amounts by Year and Categories')
            return st.plotly_chart(fig_cts)

        # 10. Year on Year Growth of Insurance Business
        def ques10():
            dt = Map_insurance[["States", "Years", "Transaction_amount"]]
            dt1 = dt.groupby(['States', 'Years'])['Transaction_amount'].sum().reset_index()

            fig = px.area(dt1, x='Years', y='Transaction_amount', color='States',
                        title='Transaction Amounts by Year and State')     
            return st.plotly_chart(fig)

        return ques1, ques2, ques3, ques4, ques5, ques6, ques7, ques8, ques9, ques10
    ques1, ques2, ques3, ques4, ques5, ques6, ques7, ques8, ques9, ques10 = datainsights()
    # Close the connection after transformation
    mydb.close()    

    # HOME PAGE
    if selected == "Home":
        # Title Image    
        col1,col2 = st.columns(2,gap= 'medium')
        col1.image("guvi_logo.png")
        col1.markdown("#   ")
        col1.write("##### Click below LOAD/REFRESH button")
        col1.write("###### To clone data from Github and store it in SQL")
        if col1.button("LOAD/REFRESH"):
            # Add spinner
            with st.spinner("Please wait, Phonepe Github Data is getting processed and loaded to MySQL..."):
                try:
                    dataclone()
                    dataprocess()
                    st.success("Phonepe Github Data loaded to MySQL Successfully!!!")
                except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
                        import traceback
                        st.text(traceback.format_exc())
                finally:
                        # Close the connection after transformation
                        mydb.close()
        col2.markdown("#### :red[Domain] : Fintech")
        col2.markdown("#### :red[Technologies used] : Github Cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")
        col2.markdown("#### :red[Overview] : The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.")
    
    # EXPLORE DATA PAGE
    if selected == "Explore Data":
        st.subheader(":red[PhonePe Data Exploration]")
        # Tab for selection
        tab1, tab2, = st.tabs(["Payments", "Insurance"])
        with tab1:
            selected_tab = st.radio("**Select the Analysis Method**",["Transactions", "Users"],index=0, horizontal=True)
            years_list_transaction, quarters_list_transaction, default_year_transaction, default_quarter_transaction = payment_year_quarter_list()
            sorted_states = sorted(india_geojson["ST_NM"].unique())
            col1,col2= st.columns(2)
            years_transaction = col1.selectbox("Select Year", years_list_transaction, index=years_list_transaction.index(default_year_transaction))
            quarters_transaction = col2.selectbox("Select Quarter", quarters_list_transaction, index=quarters_list_transaction.index(default_quarter_transaction))
                
            if selected_tab == "Transactions":
                # Data Visualization for Transaction
                plot_transaction_map(years_transaction, quarters_transaction)
                selected_state = st.selectbox("Select State", sorted_states)
                show_transaction_details(years_transaction, quarters_transaction,selected_state)
                show_top_districts_pincodes(years_transaction, quarters_transaction, selected_state)

            elif selected_tab == "Users":
                # Data Visualization for User
                plot_user_map(years_transaction, quarters_transaction)
                selected_state = st.selectbox("Select State", sorted_states)
                show_user_details(years_transaction, quarters_transaction, selected_state)
                show_top_user_districts_pincodes(years_transaction, quarters_transaction, selected_state)
            # Close the database connection
            mydb.close()
        
        with tab2:
                # Data Visualization for Insurance
                years_list_insurance, quarters_list_insurance, default_year_insurance, default_quarter_insurance = insurance_year_quarter_list()
                col1,col2= st.columns(2)
                years_insurance = col1.selectbox("Select Year", years_list_insurance, index=years_list_insurance.index(default_year_insurance))
                quarters_insurance = col2.selectbox("Select Quarter", quarters_list_insurance, index=quarters_list_insurance.index(default_quarter_insurance))
                sorted_states = sorted(india_geojson["ST_NM"].unique())
                
                plot_insurance_map(years_insurance, quarters_insurance)
                selected_state_insurance = st.selectbox("Select State for Insurance", sorted_states)
                show_insurance_details(years_insurance, quarters_insurance, selected_state_insurance)
                show_top_districts_pincodes_insurance(years_insurance, quarters_insurance, selected_state_insurance)
                # Close the database connection
                mydb.close()
# INSIGHTS PAGE
    if selected == "Insights":
        
        st.subheader(":red[Select any question to get Insights]")
        questions = st.selectbox('Questions',
        ['Click the question that you would like to query',
        '1. Top Brands Of Mobiles Used',
        '2. Top 10 Districts With Highest Transaction Amount',
        '3. Bottom 10 Districts With Lowest Transaction Amount',
        '4. Top 10 States With App Usage',
        '5. Bottom 10 States With App Usage',
        '6. States With Lowest Trasanction Count',
        '7. Quarter on Quarter Growth of Transactions',
        '8. Year on Year Growth by App Usage',
        '9. Year on Year Growth by Transaction Categories',
        '10. Year on Year Growth of Insurance Business'])
        
        if questions=="1. Top Brands Of Mobiles Used":
            ques1()        
        elif questions=="2. Top 10 Districts With Highest Transaction Amount":
            ques2()
        elif questions=="3. Bottom 10 Districts With Lowest Transaction Amount":
            ques3()
        elif questions=="4. Top 10 States With App Usage":
            ques4()
        elif questions=="5. Bottom 10 States With App Usage":
            ques5()
        elif questions=="6. States With Lowest Trasanction Count":
            ques6()
        elif questions=="7. Quarter on Quarter Growth of Transactions":
            ques7()
        elif questions=="8. Year on Year Growth by App Usage":
            ques8()
        elif questions=="9. Year on Year Growth by Transaction Categories":
            ques9()
        elif questions=="10. Year on Year Growth of Insurance Business":
            ques10()

    # Using st.cache_data for a time-consuming function
    @st.cache_data
    def example_time_consuming_function():
        # Implementation of the time-consuming function...
        return "Result of the time-consuming function"

    # Automatically runs and caches the function
    result = example_time_consuming_function()
    # st.write("Result:", result)  # Commenting out this line to avoid displaying the result