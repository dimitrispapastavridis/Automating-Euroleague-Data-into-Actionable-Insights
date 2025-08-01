from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import requests
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.standings import Standings
from euroleague_api.game_metadata import GameMetadata


default_args = {"start_date": datetime(2022, 12, 11)}


def etl_job():
    import pandas as pd
    import requests
    from datetime import datetime

    competition_code = "E"

    boxscore = BoxScoreData(competition_code)
    standings = Standings(competition_code)
    metadata = GameMetadata()

    all_box = []

    for season in range(2016,2025,1):
        df = boxscore.get_player_boxscore_stats_single_season(season)
        all_box.append(df)

    box_df = pd.concat(all_box, ignore_index=True)

    box_df['Player_full_name'] = box_df['Player'].apply(lambda x : ' '.join([component.capitalize() for component in x.split(', ')[::-1]]))   

    box_df.columns = [col.lower().strip() for col in box_df.columns]

    box_df = box_df[(box_df['player_id'] != 'Team') & (box_df['player_id'] != 'Total')]

    box_df['season_code'] = box_df['season'] * 1000 + box_df['gamecode']


    all_standings = []

    for season in range(2016,2025,1):
        total_rounds = (len(standings.get_standings(season=season, round_number=1)) - 1) * 2

        for round in range(1, total_rounds+1):

            stands_df = standings.get_standings(season=season, round_number=round)
            stands_df.insert(0, 'round', round)
            stands_df.insert(0, 'season', season)
            all_standings.append(stands_df)

    standings_df = pd.concat(all_standings, ignore_index=True)

    standings_df.rename(columns={
        "positionChange": "position_change",
        "gamesPlayed": "games_played",
        "gamesWon": "games_won",
        "gamesLost": "games_lost",
        "winPercentage": "win_percentage",
        "pointsDifference": "points_difference",
        "pointsFor": "points_for",
        "pointsAgainst": "points_against",
        "homeRecord": "home_record",
        "awayRecord": "away_record",
        "neutralRecord": "neutral_record",
        "overtimeRecord": "overtime_record",
        "lastTenRecord": "last_ten_record",
        "last5Form": "last_5_form",
        "groupName": "group_name",
        "club.code": "club_code",
        "club.name": "club_name",
        "club.abbreviatedName": "club_abbreviated_name",
        "club.editorialName": "club_editorial_name",
        "club.tvCode": "club_tv_code",
        "club.isVirtual": "club_is_virtual",
        "club.images.crest": "club_crest_url"
    }, inplace=True)


    standings_df["win_percentage"] = standings_df["win_percentage"].str.rstrip('%').astype(float)
    standings_df["points_difference"] = standings_df["points_difference"].astype(int)


    standings_df.columns = [col.lower().strip() for col in standings_df.columns]


    all_metadata = []

    for season in range(2016,2025,1):

        df = metadata.get_game_metadata_single_season(season=season)
        all_metadata.append(df)

    metadata_df = pd.concat(all_metadata, ignore_index=True)

    metadata_df = metadata_df[['Season','Phase','Gamecode','Round','Date','Stadium','Capacity','TeamA','TeamB','CodeTeamA','CodeTeamB','ScoreA','ScoreB','CoachA','GameTime']]

    metadata_df.columns = [col.lower().strip() for col in metadata_df.columns]

    return {"boxscore": box_df.to_json(orient="records", date_format="iso"),
            "standings": standings_df.to_json(orient="records", date_format="iso"),
            "metadata": metadata_df.to_json(orient='records',date_format='iso')
            }


from sqlalchemy import create_engine, text
import pandas as pd

def load_to_postgres(ti):

    data = ti.xcom_pull(task_ids='etl')
    box_df = pd.read_json(data['boxscore'])
    standings_df = pd.read_json(data['standings'])
    metadata_df = pd.read_json(data['metadata'])

    engine = create_engine(
    'postgresql+psycopg2:'
    '//postgres:'    
    'docker'            
    '@postgresdb:5432/'      
    'postgres')

    con = engine.connect()

    create_box_sql = """
        create table if not exists box_score (
        season INT,
        phase VARCHAR(20),
        round INT,
        gamecode INT,
        season_code INT,
        home INT,
        player_id VARCHAR(50),
        isstarter FLOAT,
        isplaying FLOAT,
        team VARCHAR(100),
        dorsal VARCHAR(10),
        player VARCHAR(100),
        minutes VARCHAR(10),
        points INT,
        fieldgoalsmade2 INT,
        fieldgoalsattempted2 INT,
        fieldgoalsmade3 INT,
        fieldgoalsattempted3 INT,
        freethrowsmade INT,
        freethrowsattempted INT,
        offensiverebounds INT,
        defensiverebounds INT,
        totalrebounds INT,
        assistances INT,
        steals INT,
        turnovers INT,
        blocksfavour INT,
        blocksagainst INT,
        foulscommited INT,
        foulsreceived INT,
        valuation INT,
        plusminus FLOAT,
        player_full_name VARCHAR(100)
    );
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(create_box_sql))

    box_df.to_sql("box_score", con=con, if_exists="append", index=False)

    create_standings_sql = """
        CREATE TABLE IF NOT EXISTS team_standings (
            season INT,
            round INT,
            position INT,
            position_change VARCHAR(10),
            games_played INT,
            games_won INT,
            games_lost INT,
            qualified BOOLEAN,
            win_percentage FLOAT,
            points_difference INT,
            points_for INT,
            points_against INT,
            home_record VARCHAR(20),
            away_record VARCHAR(20),
            neutral_record VARCHAR(20),
            overtime_record VARCHAR(20),
            last_ten_record VARCHAR(20),
            group_name VARCHAR(50),
            last_5_form VARCHAR(20),
            club_code VARCHAR(10),
            club_name VARCHAR(100),
            club_abbreviated_name VARCHAR(100),
            club_editorial_name VARCHAR(100),
            club_tv_code VARCHAR(10),
            club_is_virtual BOOLEAN,
            club_crest_url VARCHAR(255)
        );
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(create_standings_sql))

    standings_df.to_sql("team_standings", con=con, if_exists="append", index=False)


    create_metadata_sql = """
        CREATE TABLE IF NOT EXISTS metadata (
            season INT,
            phase VARCHAR(50),
            gamecode INT,
            round INT,
            date DATE,
            stadium VARCHAR(100),
            capacity INT,
            teama VARCHAR(100),
            teamb VARCHAR(100),
            codeteama VARCHAR(10),
            codeteamb VARCHAR(10),
            scorea VARCHAR(10),
            scoreb VARCHAR(10),
            coacha VARCHAR(100),
            gametime VARCHAR(20),
            PRIMARY KEY (season, gamecode)
        );
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(create_metadata_sql))

    metadata_df.to_sql("metadata", con=con, if_exists="append", index=False)


with DAG("dimensions_history_v3", schedule=None, default_args=default_args, catchup=False) as dag:

    etl_task = PythonOperator(task_id='etl',python_callable=etl_job)

    load_to_postgre = PythonOperator(task_id='load_to_postgres',python_callable=load_to_postgres)

    dim_player = SQLExecuteQueryOperator(task_id="create_dim_player",conn_id="my_postgres",
                                         sql="""
                                        CREATE TABLE IF NOT EXISTS dim_player (
                                            player_id VARCHAR(50) PRIMARY KEY,
                                            player_name VARCHAR(100),
                                            player_full_name VARCHAR(100)
                                        );

                                        INSERT INTO dim_player (player_id, player_name, player_full_name)
                                        SELECT DISTINCT player_id, player, player_full_name
                                        FROM box_score
                                        WHERE player_id IS NOT NULL
                                        ON CONFLICT (player_id) DO NOTHING;
                                    """,
    )

    dim_team = SQLExecuteQueryOperator(task_id="create_dim_team",conn_id="my_postgres",
                                       sql="""
                                        CREATE TABLE IF NOT EXISTS dim_team (
                                        club_code VARCHAR(100) PRIMARY KEY,
                                        club_name VARCHAR(100),
                                        club_tv_code VARCHAR(10),
                                        club_crest_url VARCHAR(255)
                                    );

                                    INSERT INTO dim_team (club_code, club_name, club_tv_code, club_crest_url)
                                    SELECT DISTINCT club_code, club_name, club_tv_code, club_crest_url
                                    FROM team_standings
                                    WHERE club_code IS NOT NULL
                                    ON CONFLICT (club_code) DO NOTHING;
                                    """,
                                )
    
    

    fact_boxscore = SQLExecuteQueryOperator(task_id="create_fact_boxscore",conn_id="my_postgres",
                                            sql="""
                                        CREATE TABLE IF NOT EXISTS fact_boxscore (
                                            season_code INT,
                                            season INT,
                                            phase VARCHAR(20),
                                            round INT,
                                            gamecode INT,
                                            home INT,
                                            player_id VARCHAR(50),
                                            isstarter FLOAT,
                                            isplaying FLOAT,
                                            team VARCHAR(100),
                                            minutes VARCHAR(10),
                                            points INT,
                                            fieldgoalsmade2 INT,
                                            fieldgoalsattempted2 INT,
                                            fieldgoalsmade3 INT,
                                            fieldgoalsattempted3 INT,
                                            freethrowsmade INT,
                                            freethrowsattempted INT,
                                            offensiverebounds INT,
                                            defensiverebounds INT,
                                            totalrebounds INT,
                                            assistances INT,
                                            steals INT,
                                            turnovers INT,
                                            blocksfavour INT,
                                            blocksagainst INT,
                                            foulscommited INT,
                                            foulsreceived INT,
                                            valuation INT,
                                            plusminus FLOAT,
                                            FOREIGN KEY (player_id) REFERENCES dim_player(player_id),
                                            FOREIGN KEY (team) REFERENCES dim_team(club_code),
                                            PRIMARY KEY (season_code, player_id)
                                        );

                                        INSERT INTO fact_boxscore (
                                        season_code, season, phase, round, gamecode, home, player_id,
                                        isstarter, isplaying, team, minutes, points,
                                        fieldgoalsmade2, fieldgoalsattempted2,
                                        fieldgoalsmade3, fieldgoalsattempted3,
                                        freethrowsmade, freethrowsattempted,
                                        offensiverebounds, defensiverebounds, totalrebounds,
                                        assistances, steals, turnovers,
                                        blocksfavour, blocksagainst,
                                        foulscommited, foulsreceived,
                                        valuation, plusminus
                                        )
                                        SELECT DISTINCT 
                                            season_code, season, phase, round, gamecode, home, player_id,
                                            isstarter, isplaying, team, minutes, points,
                                            fieldgoalsmade2, fieldgoalsattempted2,
                                            fieldgoalsmade3, fieldgoalsattempted3,
                                            freethrowsmade, freethrowsattempted,
                                            offensiverebounds, defensiverebounds, totalrebounds,
                                            assistances, steals, turnovers,
                                            blocksfavour, blocksagainst,
                                            foulscommited, foulsreceived,
                                            valuation, plusminus
                                        FROM box_score
                                        WHERE player_id IS NOT NULL
                                        ON CONFLICT (season_code, player_id) DO NOTHING;
                                    """,
                                )
    

    fact_team_standings = SQLExecuteQueryOperator(task_id="create_fact_team_standings",conn_id="my_postgres",
                                                  sql = """
                                                CREATE TABLE IF NOT EXISTS fact_team_standings (
                                                    season INT,
                                                    round INT,
                                                    club_code VARCHAR(10),
                                                    position INT,
                                                    position_change VARCHAR(10),
                                                    games_played INT,
                                                    games_won INT,
                                                    games_lost INT,
                                                    qualified BOOLEAN,
                                                    win_percentage FLOAT,
                                                    points_difference INT,
                                                    points_for INT,
                                                    points_against INT,
                                                    home_record VARCHAR(20),
                                                    away_record VARCHAR(20),
                                                    neutral_record VARCHAR(20),
                                                    overtime_record VARCHAR(20),
                                                    last_ten_record VARCHAR(20),
                                                    group_name VARCHAR(50),
                                                    last_5_form VARCHAR(20),
                                                    PRIMARY KEY (season, round, club_code),
                                                    FOREIGN KEY (club_code) REFERENCES dim_team(club_code)
                                                );

                                                INSERT INTO fact_team_standings (
                                                    season, round, club_code, position, position_change, games_played, games_won,
                                                    games_lost, qualified, win_percentage, points_difference, points_for, points_against,
                                                    home_record, away_record, neutral_record, overtime_record, last_ten_record, group_name, last_5_form
                                                )
                                                SELECT DISTINCT
                                                    season, round, club_code, position, position_change, games_played, games_won,
                                                    games_lost, qualified, win_percentage, points_difference, points_for, points_against,
                                                    home_record, away_record, neutral_record, overtime_record, last_ten_record, group_name, last_5_form
                                                FROM team_standings
                                                WHERE club_code IS NOT NULL;
                                                """
    )

    #etl_task >> load_to_postgre >> [dim_player, dim_team] >> [fact_boxscore, fact_team_standings]

    etl_task >> load_to_postgre >> [dim_player, dim_team]
    dim_player >> fact_boxscore
    dim_team >> [fact_boxscore, fact_team_standings]