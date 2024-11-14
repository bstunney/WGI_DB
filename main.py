import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import re

# db
import pymysql
import sys
import boto3
import os

# Import libraries
from sshtunnel import SSHTunnelForwarder
import pymysql
import mysql.connector
import warnings

# Suppress specific warning
warnings.filterwarnings("ignore", category=UserWarning, message="pandas only support SQLAlchemy connectable")

def scrape_competition_show(url,connection, insert_now= False):
    """
    get all show data from one recap
    """

    # Custom headers to mimic a real browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a request to fetch the webpage content with custom headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:

        # Use pandas to read all tables
        tables = pd.read_html(response.content)

        # Select the first table (if there are multiple tables, adjust accordingly)
        #df = tables[2]

        show_name_place_date_round = tables[0].values[0][1]
        var = show_name_place_date_round

        # get year
        year = var.split(" ")[0]

        # get date
        pattern = r'\b[A-Za-z]+, [A-Za-z]+ \d{1,2}, \d{4}\b'
        matches = re.findall(pattern, var)
        date = matches[0]

        # get round
        round = ''
        if var.split(date)[-1] != '':
            round = var.split(date)[-1].strip()

        # get showname, location
        showname_location = " ".join(var.split(date)[0].split(" ")[1:])
        for word in showname_location.split(" "):
            if word.lower() in ["prelims", "finals", "regional"]:
                showname = showname_location.split(word)[0] + word
                location = showname_location.split(word)[1].strip()

        # get round
        if round == "":
            round = showname.split(" ")[-1].strip()
            if showname.split(" ")[-2].lower() == "semi":
                round = "Semi " + round

        showname = showname.strip()

        print("-------------------------------------------------------")

        # HERE: generate PK - season_id, check if record is in db, and insert season_id, year into "season" table
        #       else: retrieve PK
        with connection.cursor() as cursor:

            # season insert
            insert_table_query1 = f"""
                    INSERT INTO `season` (`year`) 
                        SELECT {year}
                        WHERE NOT EXISTS (SELECT * FROM `season` 
                          WHERE `year`={year} LIMIT 1)
                    """
            cursor.execute(insert_table_query1)
            connection.commit()
            print("Inserted season:", year)

            # show insert
            insert_table_query2 = f"""
                                            INSERT INTO `wgi_show` (`season_id`, `location`, `name`)
                                            SELECT 
                                                (SELECT season_id FROM season WHERE year = %s LIMIT 1) AS season_id, %s, %s
                                            WHERE NOT EXISTS (
                                                SELECT 1 FROM `wgi_show` 
                                                WHERE `season_id` = (SELECT season_id FROM season WHERE year = %s LIMIT 1)
                                                  AND `name` = %s
                                                  AND `location` = %s
                                                LIMIT 1
                                            );
    
                                            """

            cursor.execute(insert_table_query2, (year, location, showname, year, showname, location))
            connection.commit()
            print("Inserted show:", showname)

            # Convert to datetime object
            date_object = datetime.strptime(date, "%A, %B %d, %Y")

            # Format to YYYY-MM-DD
            date = date_object.strftime("%Y-%m-%d")

            # show round insert
            insert_table_query3arch = f"""
            
            
                                                        INSERT INTO `show_round` (`show_id`, `show_date`, `round`)
                                                        SELECT 
                                                            (SELECT w.show_id 
                                                                 FROM wgi_show w
                                                                 JOIN season s ON w.season_id = s.season_id
                                                                 WHERE w.name = %s AND s.year = %s
                                                                 LIMIT 1) AS show_id, %s, %s
                                                        WHERE NOT EXISTS (
                                                            SELECT 1 FROM `show_round` 
                                                            WHERE `show_id` = (SELECT w.show_id 
                                                                 FROM wgi_show w
                                                                 JOIN season s ON w.season_id = s.season_id
                                                                 WHERE w.name = %s AND s.year = %s
                                                                 LIMIT 1)
                                                              AND `show_date` = %s
                                                              AND 'round' = %s
                                                            LIMIT 1
                                                        );
    
                                                        """

            # show round insert
            insert_table_query3 = f"""
    
    
                                                                        INSERT INTO `show_round` (`show_id`, `show_date`, `round_name`)
                                                                        SELECT 
                                                                            (SELECT w.show_id 
                                                                                 FROM wgi_show w
                                                                                 JOIN season s ON w.season_id = s.season_id
                                                                                 WHERE w.name = %s AND s.year = %s
                                                                                 LIMIT 1) AS show_id, %s, %s
                                                                        ;
    
                                                                        """
            #print(insert_table_query3)
            cursor.execute(insert_table_query3, (showname, year, date, round))
            connection.commit()
            print("Inserted show round:", date)


            # HERE: generate PK - show_id, check if record is in db, and insert show_id, season_id FK, showname, locationinto "show" table
            #       else: retrieve PK


            # HERE: generate PK - show_round_id, check if record is in db, and insert show_round_id, show_id FK, date into "show_round" table
            #       else: retrieve PK

        class_scores_dfs = {}
        for idx, df in enumerate(tables):
            headers = df.iloc[0]
            if "Equipment Analysis" in list(headers):
                class_scores_dfs[tables[idx -1].values[0][0]] = df



        # Display the DataFrame
        #read_table(class_scores_dfs[list(class_scores_dfs.keys())[0]])

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

def read_table(df_of_one_table):
    """
    get all score data from one recap
    """
    df = df_of_one_table

    # get column headers
    category_headers = list(df.iloc[0])
    category_header_type = list(df.iloc[2])
    judges = list(df.iloc[1])
    teams_of_this_round = list(df.iloc[:, 0])
    hometowns_of_this_round = list(df.iloc[:, 1])

    #print([val for val in teams_of_this_round if not pd.isna(val) ])
    #print([val for val in hometowns_of_this_round if not pd.isna(val)])
    print(category_headers)
    print(category_header_type)
    print(teams_of_this_round)
    print(hometowns_of_this_round)
    print(judges)
    # go thru each team's scores
    for i in range(3, len(df)):
        print()
        print(teams_of_this_round[i], ":", hometowns_of_this_round[i])
        team_scores = list(df.iloc[i])[2:]
        #print(team_scores)

        # go thru each caption for each team
        for idx in range(len(team_scores)):
            print(category_headers[idx+2], ":", category_header_type[idx+2])
            placement = "".join(team_scores[idx][len(team_scores[idx]) - 1:])

            score = "".join(team_scores[idx][:len(team_scores[idx]) -1])

            if score != "":
                print(score, placement)
            if score == "":
                penalty = placement
                print("penalty:", penalty)
            if "." in score and len(score) == 2:
                penalty = "." + placement
                print("penalty:", penalty)

            print()

            # HERE: generate PK - judge_id, check if record is in db, and insert judge_id, name into "judge" table
            #       else: retrieve PK

            # HERE: generate PK - team_id, check if record is in db, and insert team_id, name, hometown into "team" table
            #       else: retrieve PK

            # HERE: generate PK - performance_id, check if record is in db, and insert performance_id, show_round_id FK,
            # team_id FK, judge_id FK into "performance" table
            #       else: retrieve PK

            # HERE: generate PK - score_id, check if record is in db, and insert score_id, performance_id FK, caption,
            # type (vocab/exc, total), placement into "score" table
            #       else: retrieve PK

    print([team for team in teams_of_this_round if isinstance(team, str)])
    print([home for home in hometowns_of_this_round if isinstance(home, str)])

def create_connection():
    """
    db connection
    """
    connection = pymysql.connect(
        host = "wgi.c72q0qo8oeio.us-east-2.rds.amazonaws.com",
        user = "HERE",
        password = "HERE",
        database = "wgi"
    )

    return connection

def get_recent_season_shows(season_page_url,connection):
    """
    collect data from recent season pages
    """

    # Send a request to fetch the webpage content with custom headers
    response = requests.get(season_page_url, headers=None)
    shownames = []
    showname_rounds = []
    # Check if the request was successful
    if response.status_code == 200:
        # Use pandas to read all tables
        # Parse the webpage content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        recap_links = soup.find_all('a', href=lambda href: href and "recaps.competitionsuite.com" in href)

        for i, link in enumerate(recap_links):
           recap_url = link.get('href')
           scrape_competition_show(recap_url,connection)

def get_historical_season_shows(season_page_url):
    """
    collect urls for historical seasons
    """
    # Send a request to fetch the webpage content with custom headers
    response = requests.get(season_page_url, headers=None)

    # Check if the request was successful
    if response.status_code == 200:
        # Use pandas to read all tables
        # Parse the webpage content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = pd.read_html(response.content)

        headlines = soup.find_all('h2', class_="h-custom-headline h2 accent elementor-heading-title elementor-size-default")
        captions = [headline.get_text(strip=True) for headline in headlines]
        print(len(tables))
        print(len(captions))

def query(connection):
    """
    test query function
    """
    try:
        with connection.cursor() as cursor:
            # Execute the query to retrieve all tables
            # SQL query to create a table

            q = """
                                     show create table show_round
                                    """
            cursor.execute(q)
            print(cursor.fetchall())
            #connection.commit()

            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()

            # Extract the table names from the result
            table_list = [table[0] for table in tables]
            print(table_list)

            pd.set_option('display.max_columns', None)  # Show all columns
            pd.set_option('display.expand_frame_repr', False)  # Prevent line breaks

            df_season = pd.read_sql("SELECT * FROM season", connection)
            df_wgi_show = pd.read_sql("SELECT * FROM wgi_show", connection)
            df_show_round = pd.read_sql("SELECT * FROM show_round", connection)
            print(df_season)
            print(df_wgi_show)
            print(df_show_round)

    finally:
        connection.close()

def main():


    historical_scores_page = "https://www.wgi.org/color-guard/historical-scores/"

    #scrape_competition_show(url2)
    season_page_url = "https://wgi.org/color-guard/cg-scores-2023/"

    connection = create_connection()
    #query(connection)

    get_recent_season_shows(season_page_url,connection)

    #print("hello")
    historical_season_url = "https://www.wgi.org/historical_scores/2019/"
    #get_historical_season_shows(historical_season_url)

if __name__ == '__main__':
    main()
