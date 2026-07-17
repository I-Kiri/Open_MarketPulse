# ttc_make_graphs.py
"""
body for test TTC graph making based on database data
"""

import os

import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px

from prefect import task
from prefect.cache_policies import NO_CACHE


# ── Helpers ───────────────────────────────────────────────────────────────────────────────────────────────────────────
def create_engine_to_database(
        tdatabase_url
) -> create_engine:
    """Creating engine for connections"""
    tengine = create_engine(
        tdatabase_url,
        echo=False,  # True for SQL logging
        pool_pre_ping=True  # ensures connections are valid before use
    )
    return tengine


def set_output_directory(
        tpath,
        tlogger
) -> None:
    """Setting directory to save file"""
    tlogger.info("Setting directory to save file...")
    if not os.path.exists(tpath):
        os.makedirs(tpath)
        tlogger.info(f"Directory to save file created: {tpath}")
    tlogger.info(f"Directory to save file existed: '{tpath}'.")



# ── Tasks ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
@task(cache_policy=NO_CACHE)
def get_data_from_pqsql_create_graphs(
        tdatabase_url,
        tdb_coded_name,
        tfact_table,
        tfact_item_table_schema,
        tsave_dir,
        tlogger
) -> list:
    """Getting data from the database"""
    tlogger.info("Creating connection to the database...")
    try:
        engine = create_engine_to_database(tdatabase_url=tdatabase_url)
        tlogger.info("Connection to the database created.")
    except Exception as e:
        tlogger.error(f"Failed to connect to the database: '{tdb_coded_name}' (db_coded_name), {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise

    tlogger.info(f"Getting data from table {tfact_item_table_schema}.{tfact_table}...")
    try:
        with engine.begin() as conn:
            # players tops
            tlogger.info(f"Getting Data for Bar graph 'Top 5 Players By Wares Sum Of All Time'...")
            query_top_sum_players = text(f"""
                select
                    player_teso_id, sum(total_price) total_sum
                from {tfact_item_table_schema}.{tfact_table}
                    group by player_teso_id 
                order by sum(total_price) desc, player_teso_id
                limit 5;
            """)
            df_top_sum_players = pd.read_sql_query(
                query_top_sum_players,
                conn
            )

            tlogger.info(f"Getting Data for Bar graph 'Top 5 Players By Wares Amount Of All Time'...")
            query_top_items_amt_players = text(f"""
                select
                    player_teso_id, sum(amount) total_items_amt
                from {tfact_item_table_schema}.{tfact_table}
                    group by player_teso_id 
                order by sum(amount) desc, player_teso_id
                limit 5;
            """)
            df_top_items_amt_players = pd.read_sql_query(
                query_top_items_amt_players,
                conn
            )

            # locations tops
            tlogger.info(f"Getting Data for Bar graph 'Top 5 Regions By Wares Amount Of All Time'...")
            query_top_items_amt_regions = text(f"""
                with cte as (
                    select 
                        location_name ,
                        trim(split_part(location_name, ':', 1)) region_name ,
                        trim(split_part(location_name, ':', 2)) city_name ,
                        amount 
                    from {tfact_item_table_schema}.{tfact_table}
                )
                select region_name, sum(amount) total_items_amt
                from cte
                    group by region_name 
                order by sum(amount) desc, region_name
                limit 5;                                      
            """)
            df_top_items_amt_regions = pd.read_sql_query(
                query_top_items_amt_regions,
                conn
            )

            tlogger.info(f"Getting Data for Bar graph 'Top 5 Cities By Wares Amount Of All Time'...")
            query_top_items_amt_cities = text(f"""
                with cte as (
                    select 
                        location_name ,
                        trim(split_part(location_name, ':', 1)) region_name ,
                        trim(split_part(location_name, ':', 2)) city_name ,
                        amount 
                    from {tfact_item_table_schema}.{tfact_table}
                )
                select city_name, sum(amount) total_items_amt
                from cte
                    group by city_name 
                order by sum(amount) desc, city_name
                limit 5;                                     
            """)
            df_top_items_amt_cities = pd.read_sql_query(
                query_top_items_amt_cities,
                conn
            )

            # trends
            tlogger.info(f"Getting Data for Line graph 'Rows Accumulation Over Time'...")
            query_data_amt_trend = text(f"""
                with cte as (
                    select
                        cast(created_dt as date) as row_date, id_fact_ttc_items
                    from dev.fact_ttc_items
                )
                select
                    row_date , count(id_fact_ttc_items) total_rows
                from cte
                    group by row_date 
                order by row_date;
            """)
            df_data_amt_trend = pd.read_sql_query(
                query_data_amt_trend,
                conn
            )

        tlogger.info(f"Got all data to create graphs.")
    except Exception as e:
        tlogger.error(f"Failed to get data to create graphs, {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise

    tlogger.info(f"Creating graphs...")
    graphs_paths = []
    try:
        set_output_directory(tpath=tsave_dir, tlogger=tlogger)

        tlogger.info(f"Creating Bar graph 'Top 5 Players By Wares Sum Of All Time'...")
        # main graph attributes
        fig_top_sum_players = px.bar(
            x=df_top_sum_players["player_teso_id"],
            y=df_top_sum_players["total_sum"],
            text=df_top_sum_players["total_sum"],
            color=df_top_sum_players["total_sum"],
            color_continuous_scale="Viridis"
        )
        # values inside bars
        fig_top_sum_players.update_traces(
            texttemplate="Wares Sum: %{text:,.0f}",
            textposition="inside",  # in the bars
            textfont=dict(size=10)
        )
        # hide color axis
        fig_top_sum_players.update_coloraxes(showscale=False)
        # formatting
        fig_top_sum_players.update_layout(
            title=dict(
                text="Top 5 Players By Wares Sum Of All Time",
                font=dict(size=20, weight="bold", family="Arial"),
                x=0.5,
                xanchor="center"
            ),
            xaxis_title="Player",
            yaxis_title="Total Wares Sum",
            plot_bgcolor="rgba(240,240,240,0.8)",
            paper_bgcolor="rgba(255,255,255,0.9)",
            font=dict(family="Arial, sans-serif"),

            xaxis=dict(
                showgrid=False,
                automargin=False,
                title_font=dict(size=14, weight="bold")
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="LightGray",
                tickformat=",.0f",
                title_font=dict(size=14, weight="bold")
            ),

            height=500,
            margin=dict(t=80, b=100, l=60, r=40),
        )
        # saving
        fig_top_sum_players.write_html(f"{tsave_dir}/top_5_plrs_by_wrs_sum.html")
        graphs_paths.append(f"{tsave_dir}/top_5_plrs_by_wrs_sum.html")


        tlogger.info(f"Creating Bar graph 'Top 5 Players By Wares Amount Of All Time'...")
        # main graph attributes
        fig_top_items_amt_players = px.bar(
            x=df_top_items_amt_players["player_teso_id"],
            y=df_top_items_amt_players["total_items_amt"],
            text=df_top_items_amt_players["total_items_amt"],
            color=df_top_items_amt_players["total_items_amt"],
            color_continuous_scale="Viridis"
        )
        # values inside bars
        fig_top_items_amt_players.update_traces(
            texttemplate="Wares Amount: %{text:,.0f}",
            textposition="inside",  # in the bars
            textfont=dict(size=10)
        )
        # hide color axis
        fig_top_items_amt_players.update_coloraxes(showscale=False)
        # formatting
        fig_top_items_amt_players.update_layout(
            title=dict(
                text="Top 5 Players By Wares Amount Of All Time",
                font=dict(size=20, weight="bold", family="Arial"),
                x=0.5,
                xanchor="center"
            ),
            xaxis_title="Player",
            yaxis_title="Total Wares Amount",
            plot_bgcolor="rgba(240,240,240,0.8)",
            paper_bgcolor="rgba(255,255,255,0.9)",
            font=dict(family="Arial, sans-serif"),

            xaxis=dict(
                showgrid=False,
                automargin=False,
                title_font=dict(size=14, weight="bold")
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="LightGray",
                tickformat=",.0f",
                title_font=dict(size=14, weight="bold")
            ),

            height=500,
            margin=dict(t=80, b=100, l=60, r=40),
        )
        # saving
        fig_top_items_amt_players.write_html(f"{tsave_dir}/top_5_plrs_by_wrs_amt.html")
        graphs_paths.append(f"{tsave_dir}/top_5_plrs_by_wrs_amt.html")


        tlogger.info(f"Creating Bar graph 'Top 5 Regions By Wares Amount Of All Time'...")
        # main graph attributes
        fig_top_items_amt_regions = px.bar(
            x=df_top_items_amt_regions["region_name"],
            y=df_top_items_amt_regions["total_items_amt"],
            text=df_top_items_amt_regions["total_items_amt"],
            color=df_top_items_amt_regions["total_items_amt"],
            color_continuous_scale="Viridis"
        )
        # values inside bars
        fig_top_items_amt_regions.update_traces(
            texttemplate="Wares Amount: %{text:,.0f}",
            textposition="inside",  # in the bars
            textfont=dict(size=10)
        )
        # hide color axis
        fig_top_items_amt_regions.update_coloraxes(showscale=False)
        # formatting
        fig_top_items_amt_regions.update_layout(
            title=dict(
                text="Top 5 Regions By Wares Amount Of All Time",
                font=dict(size=20, weight="bold", family="Arial"),
                x=0.5,
                xanchor="center"
            ),
            xaxis_title="Region",
            yaxis_title="Total Wares Amount",
            plot_bgcolor="rgba(240,240,240,0.8)",
            paper_bgcolor="rgba(255,255,255,0.9)",
            font=dict(family="Arial, sans-serif"),

            xaxis=dict(
                showgrid=False,
                automargin=False,
                title_font=dict(size=14, weight="bold")
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="LightGray",
                tickformat=",.0f",
                title_font=dict(size=14, weight="bold")
            ),

            height=500,
            margin=dict(t=80, b=100, l=60, r=40),
        )
        # saving
        fig_top_items_amt_regions.write_html(f"{tsave_dir}/top_5_rgns_by_wrs_amt.html")
        graphs_paths.append(f"{tsave_dir}/top_5_rgns_by_wrs_amt.html")


        tlogger.info(f"Creating Bar graph 'Top 5 Cities By Wares Amount Of All Time'...")
        # main graph attributes
        fig_top_items_amt_cities = px.bar(
            x=df_top_items_amt_cities["city_name"],
            y=df_top_items_amt_cities["total_items_amt"],
            text=df_top_items_amt_cities["total_items_amt"],
            color=df_top_items_amt_cities["total_items_amt"],
            color_continuous_scale="Viridis"
        )
        # values inside bars
        fig_top_items_amt_cities.update_traces(
            texttemplate="Wares Amount: %{text:,.0f}",
            textposition="inside",  # in the bars
            textfont=dict(size=10)
        )
        # hide color axis
        fig_top_items_amt_cities.update_coloraxes(showscale=False)
        # formatting
        fig_top_items_amt_cities.update_layout(
            title=dict(
                text="Top 5 Cities By Wares Amount Of All Time",
                font=dict(size=20, weight="bold", family="Arial"),
                x=0.5,
                xanchor="center"
            ),
            xaxis_title="City",
            yaxis_title="Total Wares Amount",
            plot_bgcolor="rgba(240,240,240,0.8)",
            paper_bgcolor="rgba(255,255,255,0.9)",
            font=dict(family="Arial, sans-serif"),

            xaxis=dict(
                showgrid=False,
                automargin=False,
                title_font=dict(size=14, weight="bold")
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="LightGray",
                tickformat=",.0f",
                title_font=dict(size=14, weight="bold")
            ),

            height=500,
            margin=dict(t=80, b=100, l=60, r=40),
        )
        # saving
        fig_top_items_amt_cities.write_html(f"{tsave_dir}/top_5_cts_by_wrs_amt.html")
        graphs_paths.append(f"{tsave_dir}/top_5_cts_by_wrs_amt.html")


        tlogger.info(f"Creating Line graph 'Rows Accumulation Over Time'...")
        # main graph attributes
        fig_data_amt_trend = px.line(
            x=df_data_amt_trend["row_date"],
            y=df_data_amt_trend["total_rows"],
            markers=True,
            line_shape="linear"
        )
        # values on lines and markers
        fig_data_amt_trend.update_traces(
            line=dict(color="orange", width=3),
            marker=dict(size=10, color="darkorange", symbol="circle"),
            text=df_data_amt_trend["total_rows"],   # need to hover/data labels
            texttemplate="Rows: %{text:,.0f}",
            textposition="top center",  # above markers
            textfont=dict(size=10)
        )
        # formatting
        fig_data_amt_trend.update_layout(
            title=dict(
                text="Rows Accumulation Over Time",
                font=dict(size=20, weight="bold", family="Arial"),
                x=0.5,
                xanchor="center"
            ),
            xaxis_title="Date",
            yaxis_title="Accumulated Rows",
            plot_bgcolor="rgba(240,240,240,0.8)",
            paper_bgcolor="rgba(255,255,255,0.9)",
            font=dict(family="Arial, sans-serif"),

            xaxis=dict(
                showgrid=False,
                automargin=False,
                tickformat="%Y.%m.%d",
                title_font=dict(size=14, weight="bold")
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="LightGray",
                tickformat=",.0f",
                title_font=dict(size=14, weight="bold")
            ),

            hovermode="x unified",  # hover info for all traces at once
            height=500,
            margin=dict(t=80, b=100, l=60, r=40),
        )
        # saving
        fig_data_amt_trend.write_html(f"{tsave_dir}/data_amt_trend.html")
        graphs_paths.append(f"{tsave_dir}/data_amt_trend.html")

        return graphs_paths
    except Exception as e:
        tlogger.error(f"Failed to create all graphs properly, {e}", exc_info=True)
        tlogger.error("TTC Tasks stopped. Flow is running.")
        raise


# ── Main ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pass
