from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
import json
from airflow.exceptions import AirflowSkipException # Imported for rest days


UPSERT_DIM_COMPETITION_SQL = """
    INSERT INTO dim_competition (id, name, code, type)
    select  distinct (api_data #>> '{competition, id}')::INT as id,
		    (api_data #>> '{competition, name}')::VARCHAR(255) as name,
    	    (api_data #>> '{competition, code}')::VARCHAR(50) as code,
    	    (api_data #>> '{competition, type}')::VARCHAR(50) as type
        from staging_wc_stats sws 
        WHERE (api_data #>> '{competition, id}') IS NOT NULL
        -- If the API ever glitches and returns an empty dictionary, 
        -- this prevents Postgres from crashing by trying to insert a NULL primary key

    ON CONFLICT (id) 
    DO UPDATE SET 
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        type = EXCLUDED.type;
"""

UPSERT_DIM_REFEREE_SQL = """
    INSERT INTO dim_referee (id, name, nationality)
    select  distinct 
            (referee_item ->> 'id')::INT as id,
            (referee_item ->> 'name')::VARCHAR(255) as name,
            (referee_item ->> 'nationality')::VARCHAR(100) as nationality
        from staging_wc_stats, 
        LATERAL jsonb_array_elements(api_data -> 'referees') AS referee_item
        WHERE (referee_item ->> 'id') IS NOT NULL

    ON CONFLICT (id) 
    DO UPDATE SET 
        name = EXCLUDED.name,
        nationality = EXCLUDED.nationality
        ;
"""

UPSERT_DIM_TEAM_SQL = """
    INSERT INTO dim_team (id, name, short_name, code)
        WITH combined_teams AS (
            select  (api_data #>> '{awayTeam, id}')::INT as id,
            (api_data #>> '{awayTeam, name}')::VARCHAR(255) as name,
            (api_data #>> '{awayTeam, shortName}')::VARCHAR(100) as short_name,
            (api_data #>> '{awayTeam, tla}')::VARCHAR(50) as code
            from staging_wc_stats sws 
            where (api_data #>> '{awayTeam, id}') IS NOT NULL
            union all
            select  (api_data #>> '{homeTeam, id}')::INT as id,
                    (api_data #>> '{homeTeam, name}')::VARCHAR(255) as name,
                    (api_data #>> '{homeTeam, shortName}')::VARCHAR(100) as short_name,
                    (api_data #>> '{homeTeam, tla}')::VARCHAR(50) as code
            from staging_wc_stats sws 
            where (api_data #>> '{homeTeam, id}') IS NOT NULL
            )
        SELECT DISTINCT id, name, short_name, code 
        FROM combined_teams

        ON CONFLICT (id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            short_name = EXCLUDED.short_name,
            code = EXCLUDED.code
        ;
"""

UPSERT_DIM_SEASON_SQL = """
    INSERT INTO dim_season (id, start_date, end_date, winner)
        SELECT  distinct (api_data #>> '{season, id}')::INT as id,
            (api_data #>> '{season, startDate}')::DATE as start_date,
            (api_data #>> '{season, endDate}')::DATE as end_date,
            (api_data #>> '{season, winner}')::VARCHAR(255) as winner
        FROM staging_wc_stats sws
        WHERE (api_data #>> '{season, id}') IS NOT NULL

        ON CONFLICT (id) 
        DO UPDATE SET 
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            winner = EXCLUDED.winner
    ;
"""


UPSERT_FACT_MATCH_SQL = """
    INSERT INTO fact_match (
    id, 
    match_time, 
    competition_id, 
    season_id, 
    status, 
    stage, 
    home_team_id, 
    away_team_id, 
    referee_id, 
    winner_id, 
    duration, 
    final_home_score, 
    final_away_score, 
    load_date
)
SELECT DISTINCT ON (id)
    (api_data #>> '{id}')::INT as id,
    (api_data #>> '{utcDate}')::TIMESTAMP as match_time,
    (api_data #>> '{competition, id}')::INT as competition_id,
    (api_data #>> '{season, id}')::INT as season_id,
    (api_data #>> '{status}')::VARCHAR(50) as status_id,
    (api_data #>> '{stage}')::VARCHAR(100) as stage,
    (api_data #>> '{homeTeam, id}')::INT as home_team_id,
    (api_data #>> '{awayTeam, id}')::INT as away_team_id,
    (referee_item ->> 'id')::INT as referee_id,
    CASE 
        WHEN (api_data #>> '{score, winner}') = 'HOME_TEAM' THEN (api_data #>> '{homeTeam, id}')::INT
        WHEN (api_data #>> '{score, winner}') = 'DRAW' THEN 1
        ELSE (api_data #>> '{awayTeam, id}')::INT
    END as winner_id,
    (api_data #>> '{score, duration}')::VARCHAR(50) as duration,
    (api_data #>> '{score, fullTime, home}')::INT as final_home_score,
    (api_data #>> '{score, fullTime, away}')::INT as final_away_score,
    CURRENT_TIMESTAMP as load_date

FROM staging_wc_stats sws,
LATERAL jsonb_array_elements(api_data -> 'referees') AS referee_item

-- THE UPSERT LOGIC
ON CONFLICT (id) 
DO UPDATE SET 
    match_time = EXCLUDED.match_time,
    competition_id = EXCLUDED.competition_id,
    season_id = EXCLUDED.season_id,
    status = EXCLUDED.status,
    stage = EXCLUDED.stage,
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    referee_id = EXCLUDED.referee_id,
    winner_id = EXCLUDED.winner_id,
    duration = EXCLUDED.duration,
    final_home_score = EXCLUDED.final_home_score,
    final_away_score = EXCLUDED.final_away_score,
    load_date = CURRENT_TIMESTAMP
    ;
"""


default_args = {
    'owner': 'Sven_L123',
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='wc_api_to_postgres_elt',
    default_args=default_args,
    description='Automated WC football stats pipeline',
    schedule='@daily',
    catchup=True,
    max_active_runs=1, # CRITICAL: Prevents TRUNCATING staging table during multiple sametime runs
    template_searchpath=[]  # Optional: paths to search for SQL templates
) as dag:
    
    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        create_queries = """
			CREATE TABLE IF NOT EXISTS staging_wc_stats (
				id SERIAL PRIMARY KEY,
				api_data JSONB,
				extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			);
			"""
        hook.run(create_queries)

    @task
    def extract_and_load_staging_data(**context):
        target_day = context['logical_date']
        previous_day = target_day.subtract(days=1)

        url = "http://api.football-data.org/v4/competitions/2000/matches"
        wc_api_key = Variable.get("wc_api_key")

        headers = {
            "X-Unfold-Goals": "true",
            "X-Auth-Token": wc_api_key  
        }


        querystring = {
            "dateFrom" : previous_day.strftime("%Y-%m-%d"),
            "dateTo" : target_day.strftime("%Y-%m-%d"),
            "status" : "FINISHED"
        }

        # 4. Make the request
        try:
            response = requests.get(url, headers=headers, params=querystring)
            
            # This will automatically throw an error if your API key is invalid or the limit is reached
            response.raise_for_status() 
            
            # 5. Parse the JSON response
            data = response.json()


        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            # If the internet drops or the API crashes, this catches the error, 
            # logs it to your Airflow terminal, and then crucially raises it again. 
            # Raising it tells Airflow: "This task failed, turn it red in the UI so the developer knows!"
            raise


        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        engine = hook.get_sqlalchemy_engine()

        hook.run("TRUNCATE TABLE staging_wc_stats;")

        matches = data.get('matches', [])

        # SHORT-CIRCUIT: If no matches were played, skip the rest of the DAG
        if not matches:
            raise AirflowSkipException("No matches found for this date. Skipping database loads.")

        processed_rows = [{"api_data": json.dumps(row)} for row in matches]
        df = pd.DataFrame(processed_rows)

        df.to_sql('staging_wc_stats', con=engine, index=False, if_exists='append')
    
    @task
    def load_dim_competition(**context):
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        hook.run(UPSERT_DIM_COMPETITION_SQL)

    @task
    def load_dim_referee(**context):
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        hook.run(UPSERT_DIM_REFEREE_SQL)

    @task
    def load_dim_team(**context):
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        hook.run(UPSERT_DIM_TEAM_SQL)

    @task
    def load_dim_season(**context):
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        hook.run(UPSERT_DIM_SEASON_SQL)

    @task
    def load_fact_match(**context):
        hook = PostgresHook(postgres_conn_id = 'wc_postgres_conn')
        hook.run(UPSERT_FACT_MATCH_SQL)


    create_staging_table = create_table()
    load_staging = extract_and_load_staging_data()
    load_d_competition = load_dim_competition() 
    load_d_referee = load_dim_referee() 
    load_d_team = load_dim_team()
    load_d_season = load_dim_season()
    load_f_match = load_fact_match()


create_staging_table >> load_staging
    
load_staging >> [load_d_competition, load_d_referee, load_d_team, load_d_season] >> load_f_match
