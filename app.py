# # -*- coding: utf-8 -*-
# import json  # Kept for potential use by pandas or other libraries
# import dash_auth
# import traceback
# from datetime import datetime, timezone, timedelta
# import pytz
# import numpy as np
# import pandas as pd
# import dash
# from dash import dcc, html, callback, Input, Output, State, dash_table, ctx, no_update
# import dash_bootstrap_components as dbc
# import plotly.graph_objects as go
# import plotly.express as px
# import Dashauth
# import flight_client
#
#
# # --- Constants ---
# API_BASE_URL = "http://13.239.238.138:5070"
# FETCH_LIMIT = 100000
# FETCH_INTERVAL_SECONDS = 20
# FLIGHT_ID_COLUMN = "flight_id"
# ON_MAP_TEXT_SIZE = 9
# DISPLAY_TIMEZONE = pytz.timezone('Africa/Nairobi')
# PROCESSING_TIMEZONE = pytz.utc
# PLOTLY_TEMPLATE = "plotly_dark"
# CHART_FONT_COLOR = "#adb5bd"
# CHART_PAPER_BG = 'rgba(0,0,0,0)'
# CHART_PLOT_BG = 'rgba(0,0,0,0)'
# NA_REPLACE_VALUES = ['', 'nan', 'NaN', 'None', 'null', 'NONE', 'NULL', '#N/A', 'N/A', 'NA', '-']
#
# # --- Constants for JUB FIR Billing ---
# JUB_SOUTH_SUDAN_MIN_LAT = 3.0
# JUB_SOUTH_SUDAN_MAX_LAT = 13.0
# JUB_SOUTH_SUDAN_MIN_LON = 24.0
# JUB_SOUTH_SUDAN_MAX_LON = 36.0
# JUB_SPECIFIC_MIN_LAT = 4.96
# JUB_SPECIFIC_MAX_LAT = 10.76
# JUB_SPECIFIC_MIN_LON = 26.76
# JUB_SPECIFIC_MAX_LON = 32.62
# JUB_SPECIFIC_CENTER_LAT = (JUB_SPECIFIC_MIN_LAT + JUB_SPECIFIC_MAX_LAT) / 2
# JUB_SPECIFIC_CENTER_LON = (JUB_SPECIFIC_MIN_LON + JUB_SPECIFIC_MAX_LON) / 2
# JUB_BILLING_MAP_STYLE = "carto-positron"
#
# # --- Initialize Fetcher ---
# fetcher = None
# if flight_client:
#     try:
#         fetcher = flight_client.FlightDataFetcher(base_url=API_BASE_URL, api_endpoint="/api/flights/latest_unique",
#                                                   fetch_limit=FETCH_LIMIT)
#     except Exception as e:
#         print(f"ERROR: Failed to initialize FlightDataFetcher: {e}")
#         fetcher = None
# else:
#     print("ERROR: flight_client module not loaded.")
#
# # --- Initialize App ---
# app = dash.Dash(
#     __name__,
#     external_stylesheets=[dbc.themes.VAPOR],
#     suppress_callback_exceptions=True, title="Flight Investigation Tool"  # Renamed title
# )
#
# app.secret_key = 'a_default_secret_key_replace_if_needed_with_a_strong_random_key'
# server = app.server
#
# # --- Authentication ---
# try:
#     if Dashauth and hasattr(Dashauth, 'VALID_USERNAME_PASSWORD_PAIRS'):
#         auth = dash_auth.BasicAuth(app, Dashauth.VALID_USERNAME_PASSWORD_PAIRS)
#     else:
#         auth = None
# except Exception as auth_e:
#     print(f"ERROR setting up authentication: {auth_e}.")
#     auth = None
#
#
# # --- Helper Functions ---
# def create_empty_map_figure(message="No data available", map_style="carto-positron"):  # Defaulted map_style
#     # Adjusted to use CHART_FONT_COLOR instead of ON_MAP_TEXT_COLOR
#     map_font_color = CHART_FONT_COLOR if map_style != "open-street-map" else 'black'
#     layout = go.Layout(
#         mapbox=dict(style=map_style, center={"lat": JUB_SPECIFIC_CENTER_LAT, "lon": JUB_SPECIFIC_CENTER_LON}, zoom=5),
#         # Default center for JUB
#         margin=dict(r=5, t=5, l=5, b=5), paper_bgcolor=CHART_PAPER_BG,
#         plot_bgcolor=CHART_PLOT_BG, font=dict(color=map_font_color))
#     layout.annotations = [
#         go.layout.Annotation(text=message, align='center', showarrow=False, xref='paper', yref='paper', x=0.5, y=0.5,
#                              font=dict(size=16, color=map_font_color))]  # Use adjusted map_font_color
#     return go.Figure(data=[], layout=layout)
#
#
# # create_empty_analytics_figure function removed as it's no longer used.
#
# def create_kpi_card(title, value_id, tooltip_text):
#     return dbc.Card([
#         dbc.CardHeader(html.H6(title, className="text-white-50"), id=f"header-{value_id}"),
#         dbc.CardBody(html.H4(id=value_id, className="text-info text-center my-auto", children="-")),
#         dbc.Tooltip(tooltip_text, target=f"header-{value_id}", placement='bottom')
#     ], className="h-100 d-flex flex-column bg-dark")
#
#
# # --- JUB FIR Billing Data Processing Function ---
# def process_jub_billing_data(df: pd.DataFrame) -> pd.DataFrame:
#     df_processed = df.copy()
#     df_processed['LATITUDE'] = pd.to_numeric(df_processed['LATITUDE'], errors='coerce')
#     df_processed['LONGITUDE'] = pd.to_numeric(df_processed['LONGITUDE'], errors='coerce')
#     df_processed.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
#
#     cond_specific_zone = (
#             (df_processed['LATITUDE'] >= JUB_SPECIFIC_MIN_LAT) & (df_processed['LATITUDE'] <= JUB_SPECIFIC_MAX_LAT) &
#             (df_processed['LONGITUDE'] >= JUB_SPECIFIC_MIN_LON) & (df_processed['LONGITUDE'] <= JUB_SPECIFIC_MAX_LON)
#     )
#     cond_south_sudan = (
#             (df_processed['LATITUDE'] >= JUB_SOUTH_SUDAN_MIN_LAT) & (
#             df_processed['LATITUDE'] <= JUB_SOUTH_SUDAN_MAX_LAT) &
#             (df_processed['LONGITUDE'] >= JUB_SOUTH_SUDAN_MIN_LON) & (
#                     df_processed['LONGITUDE'] <= JUB_SOUTH_SUDAN_MAX_LON)
#     )
#     df_processed['Appearance'] = "Not Passed"
#     df_processed.loc[cond_south_sudan & ~cond_specific_zone, 'Appearance'] = "Investigate"
#     df_processed.loc[cond_specific_zone, 'Appearance'] = "Surely Passed"
#     return df_processed
#
#
# # --- Main Application Layout ---
# app.layout = dbc.Container([
#     dcc.Store(id='realtime-flight-data-store'),
#     # dcc.Store(id='db-data-store'), # Removed
#     dcc.Store(id='processed-billing-data-store'),
#     dcc.Store(id='jub-manual-points-store', data=[]),
#     dcc.Download(id="download-jub-selected-csv"),
#
#     dbc.Row([html.Br(),
#              dbc.Col(html.Img(src=app.get_asset_url("ss_logo.png"), height="40px", className="rounded-circle"),
#                      className="mt-3")], align="center", className="g-0", ),
#     dbc.Row([dbc.Col(html.H1("Flight Investigation Tool", className="text-center text-primary mb-2"), width=12),
#              # Renamed H1
#              dbc.Col(html.P(id='last-update-timestamp', className="text-center text-muted mb-4 small"), width=12)]),
#
#     # JUB FIR Billing Section (Retained)
#     dbc.Row([
#         dbc.Col(html.H2("I. JUB FIR Billing", className="text-info mt-4 mb-2 text-center"), width=12)
#     ], justify="center", align="center", className="mt-3"),
#     dbc.Row([
#         dbc.Col([
#             dbc.Card([
#                 dbc.CardHeader("JUB FIR Billing Map & Controls"),
#                 dbc.CardBody([
#                     dbc.Row([
#                         dbc.Col(dcc.Graph(id='jub-billing-map', style={'height': '60vh'}), md=9),
#                         dbc.Col([
#                             html.H5("Map Controls", className="card-title"),
#                             dbc.Label("Manual Point Entry:", className="mt-2"),
#                             dbc.Input(id='jub-manual-lat-input', type='number', placeholder='Latitude (e.g, 7.86)',
#                                       step=0.01, className="mb-2"),
#                             dbc.Input(id='jub-manual-lon-input', type='number', placeholder='Longitude (e.g, 29.69)',
#                                       step=0.01, className="mb-2"),
#                             dbc.Button('Add Point to Billing Map', id='jub-add-point-button', n_clicks=0,
#                                        color="primary", className="w-100 mb-3"),
#                             html.Div(id='jub-manual-point-info', className="small text-muted mb-3"),
#                             html.H6("Map Legend", className="mt-3"),
#                             html.Ul([
#                                 html.Li(html.Span("Surely Passed", style={'color': '#28a745', 'fontWeight': 'bold'})),
#                                 html.Li(html.Span("Investigate", style={'color': '#ffc107', 'fontWeight': 'bold'})),
#                                 html.Li(html.Span("Not Passed", style={'color': '#dc3545', 'fontWeight': 'bold'})),
#                                 html.Li(html.Span("new entry", style={'color': '#6f42c1', 'fontWeight': 'bold'})),
#                             ], style={'listStyleType': 'none', 'paddingLeft': 0, 'fontSize': '0.9em'}),
#                         ], md=3),
#                     ])
#                 ])
#             ], className="mb-3 bg-dark")
#         ], width=12),
#     ]),
#     dbc.Row([
#         dbc.Col(create_kpi_card("JUB FIR Flights (Live)", "jub-fir-kpi-total",
#                                 "Total flights processed for JUB FIR billing status from live data."), width=12, md=4,
#                 className="mb-3"),
#         dbc.Col(create_kpi_card("Flights in Hot Zone", "jub-fir-kpi-hotzone",
#                                 "Flights categorized as 'Surely Passed' (within specific hot zone)."), width=12, md=4,
#                 className="mb-3"),
#         dbc.Col(create_kpi_card("Flights to Investigate", "jub-fir-kpi-investigate",
#                                 "Flights in JUB FIR general airspace, outside hot zone."), width=12, md=4,
#                 className="mb-3"),
#     ], className="mb-2 g-3", justify="center"),
#
#     # Billing Data Table Section (Retained)
#     dbc.Row([
#         dbc.Col(html.H2("II. Billing Data Table", className="text-info mt-3 mb-2 text-center"), width=12)
#     ], justify="center", align="center"),
#     dbc.Row([
#         dbc.Col([
#             dbc.Card([
#                 dbc.CardHeader("Billing Adjudication Table (Based on Live Data & JUB FIR Logic)"),
#                 dbc.CardBody([
#                     dash_table.DataTable(
#                         id="jub-billing-data-table",
#                         row_selectable='multi',
#                         selected_rows=[],
#                         style_table={"overflowX": "auto", "marginTop": "10px"},
#                         style_filter={'backgroundColor': 'rgba(102,255,255,0.7)', 'border': '1px solid #55555580'},
#                         style_header={'backgroundColor': '#303030', 'color': '#adb5bd', 'fontWeight': 'bold',
#                                       'border': '1px solid #444'},
#                         style_cell={
#                             'textAlign': 'left', 'padding': '10px',
#                             'backgroundColor': '#1a1a1a', 'color': '#adb5bd',
#                             'border': '1px solid #303030',
#                             'minWidth': '100px', 'maxWidth': '200px', 'whiteSpace': 'normal', 'height': 'auto'
#                         },
#                         style_data_conditional=[
#                             {'if': {'row_index': 'odd'}, 'backgroundColor': '#222222'},
#                             {'if': {'state': 'selected'}, 'backgroundColor': 'rgba(0, 116, 217, 0.3)',
#                              'border': '1px solid #0074D9'},
#                             {'if': {'filter_query': '{Appearance} = "Surely Passed"', 'column_id': 'Appearance'},
#                              'backgroundColor': 'rgba(40, 167, 69, 0.4)', 'color': 'white'},
#                             {'if': {'filter_query': '{Appearance} = "Investigate"', 'column_id': 'Appearance'},
#                              'backgroundColor': 'rgba(255, 193, 7, 0.4)', 'color': 'black'},
#                             {'if': {'filter_query': '{Appearance} = "Not Passed"', 'column_id': 'Appearance'},
#                              'backgroundColor': 'rgba(220, 53, 69, 0.4)', 'color': 'white'},
#                         ],
#                         page_size=10,
#                         sort_action='native',
#                         filter_action='native',
#                         data=[],
#                         columns=[],
#                     ),
#                     dbc.Button(
#                         "Download Selected Billing Rows",
#                         id="download-jub-selected-button",
#                         color="info",
#                         className="mt-3",
#                         n_clicks=0
#                     )
#                 ])
#             ], className="bg-dark")
#         ], width=12)
#     ], className="mb-4"),
#
#     # Removed Historical Database Stats Section
#
#     # Modals and Intervals
#     # dbc.Modal([...], id="insights-modal", ...), # Removed
#     dcc.Interval(id="interval-component", interval=FETCH_INTERVAL_SECONDS * 1000, n_intervals=0),  # Kept for live data
#     # dcc.Interval(id="db-interval-component", ...), # Removed
#
#     # Footer
#     dbc.Row([dbc.Col([html.Footer([html.P("©️ 2025 South Sudan Civil Aviation. Powered by Crawford Capital",
#                                           className="text-center text-muted mt-4"), html.Div(
#         [dbc.Button("Report Issue", color="link", className="text-muted", href="#", id="report-issue-btn"),
#          html.Span(" | "),
#          dbc.Button("Privacy Policy", color="link", className="text-muted", id="privacy-policy-btn", n_clicks=0)],
#         className="text-center small")], className="py-4")])])
# ], fluid=True, className="dbc")
#
#
# # --- Callbacks ---
# # Callback 1: Update Real-time Data Store (Live Data) - Kept
# @callback(
#     Output('realtime-flight-data-store', 'data'),
#     Output('last-update-timestamp', 'children'),
#     Input("interval-component", "n_intervals"),
#     prevent_initial_call=False
# )
# def update_realtime_data_store(n_intervals):
#     global fetcher
#     fetch_time_display = datetime.now(DISPLAY_TIMEZONE)
#     status_msg_base = f"Last Live Check: {fetch_time_display.strftime('%Y-%m-%d %H:%M:%S %Z')}"
#     if fetcher is None: return no_update, f"Fetcher Init Error | {status_msg_base}"
#     try:
#         df_batch = fetcher.fetch_next_batch()
#         if df_batch is None: return no_update, f"Live API Fetch Error | {status_msg_base}"
#         if df_batch.empty:
#             last_ts_msg = fetcher.last_processed_timestamp or "initial"
#             return no_update, f"No New Live Data (since {last_ts_msg}) | {status_msg_base}"
#         df_processed_batch = df_batch.copy()
#         df_processed_batch.columns = df_processed_batch.columns.astype(str).str.lower().str.strip()
#         col_map = {'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'last_update': 'LAST_UPDATE_TIME',
#                    'flight_id': FLIGHT_ID_COLUMN, 'model': 'AIRCRAFT_MODEL', 'alt': 'ALTITUDE', 'speed': 'SPEED',
#                    'track': 'TRACK', 'callsign': 'FLIGHT_CALLSIGN', 'reg': 'REGISTRATION', 'origin': 'ORIGIN',
#                    'destination': 'DESTINATION', 'flight': 'FLIGHT_NUMBER'}
#         rename_map = {k: v for k, v in col_map.items() if k in df_processed_batch.columns}
#         df_processed_batch.rename(columns=rename_map, inplace=True)
#         essential_cols = ['LATITUDE', 'LONGITUDE', 'LAST_UPDATE_TIME', FLIGHT_ID_COLUMN]
#         missing_essential = [c for c in essential_cols if c not in df_processed_batch.columns]
#         if missing_essential:
#             print(f"Warning: New LIVE batch missing essential columns: {missing_essential}. Skipping batch.")
#             df_processed_batch = pd.DataFrame()
#         else:
#             df_processed_batch["LAST_UPDATE_TIME"] = pd.to_datetime(df_processed_batch["LAST_UPDATE_TIME"],
#                                                                     errors='coerce', utc=True)
#             num_cols = ['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'SPEED', 'TRACK']
#             str_cols = ['AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN', 'DESTINATION', 'FLIGHT_NUMBER', 'FLIGHT_CALLSIGN']
#             for col in num_cols:
#                 if col in df_processed_batch.columns: df_processed_batch[col] = pd.to_numeric(df_processed_batch[col],
#                                                                                               errors='coerce')
#             df_processed_batch.dropna(subset=essential_cols, inplace=True)
#             for col in str_cols:
#                 if col not in df_processed_batch.columns: df_processed_batch[col] = 'N/A'
#                 df_processed_batch[col] = df_processed_batch[col].astype(str).fillna('N/A').str.strip().replace(
#                     NA_REPLACE_VALUES, 'N/A', regex=False)
#             if FLIGHT_ID_COLUMN in df_processed_batch.columns:
#                 df_processed_batch[FLIGHT_ID_COLUMN] = df_processed_batch[FLIGHT_ID_COLUMN].astype(str).fillna(
#                     'N/A').str.strip().replace(NA_REPLACE_VALUES, 'N/A', regex=False)
#                 df_processed_batch = df_processed_batch[df_processed_batch[FLIGHT_ID_COLUMN] != 'N/A']
#             else:
#                 df_processed_batch = pd.DataFrame()
#         if df_processed_batch.empty: return no_update, f"No Valid Live Data | {status_msg_base}"
#         if 'LAST_UPDATE_TIME' in df_processed_batch.columns and pd.api.types.is_datetime64_any_dtype(
#                 df_processed_batch['LAST_UPDATE_TIME']):
#             if df_processed_batch['LAST_UPDATE_TIME'].dt.tz is None:
#                 df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.tz_localize('UTC')
#             else:
#                 df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.tz_convert('UTC')
#             df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.strftime(
#                 '%Y-%m-%dT%H:%M:%SZ')
#         else:
#             df_processed_batch['LAST_UPDATE_TIME'] = None  # Should be pd.NaT or None for object types
#         data_to_store = df_processed_batch.replace({pd.NA: None, pd.NaT: None, np.nan: None}).to_dict('records')
#         return data_to_store, f"Live View Updated {fetch_time_display.strftime('%H:%M:%S')} | {len(data_to_store)} live pts"
#     except Exception as e:
#         print(f"ERROR in update_realtime_data_store: Unexpected failure - {e}")
#         traceback.print_exc()
#         return no_update, f"Live Data Process Error | {status_msg_base}"
#
#
# # --- Callbacks for JUB FIR Billing Section --- (Kept and Unchanged from original relevant parts)
# @callback(
#     Output('processed-billing-data-store', 'data'),
#     Output('jub-fir-kpi-total', 'children'),
#     Output('jub-fir-kpi-hotzone', 'children'),
#     Output('jub-fir-kpi-investigate', 'children'),
#     Input("realtime-flight-data-store", "data"),
#     prevent_initial_call=True
# )
# def update_processed_billing_data_and_kpis(stored_data):
#     if not stored_data: return no_update, "-", "-", "-"
#     try:
#         df_realtime = pd.DataFrame(stored_data)
#         if df_realtime.empty: return no_update, "-", "-", "-"
#         required_cols = [FLIGHT_ID_COLUMN, 'LAST_UPDATE_TIME', 'LATITUDE', 'LONGITUDE', 'AIRCRAFT_MODEL', 'ORIGIN',
#                          'DESTINATION', 'FLIGHT_CALLSIGN', 'ALTITUDE',
#                          'REGISTRATION']  # Added ALTITUDE, REGISTRATION if needed by process_jub_billing_data or downstream display
#         for col in required_cols:
#             if col not in df_realtime.columns:
#                 if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:  # Added ALTITUDE
#                     df_realtime[col] = np.nan
#                 elif col == 'LAST_UPDATE_TIME':
#                     df_realtime[col] = pd.NaT
#                 else:
#                     df_realtime[col] = 'N/A'
#
#         df_realtime['LAST_UPDATE_TIME'] = pd.to_datetime(df_realtime['LAST_UPDATE_TIME'], errors='coerce', utc=True)
#         df_realtime.dropna(subset=[FLIGHT_ID_COLUMN, 'LAST_UPDATE_TIME', 'LATITUDE', 'LONGITUDE'], inplace=True)
#         df_realtime[FLIGHT_ID_COLUMN] = df_realtime[FLIGHT_ID_COLUMN].astype(str).fillna('N/A')
#         df_realtime = df_realtime[df_realtime[FLIGHT_ID_COLUMN] != 'N/A']
#         if df_realtime.empty: return no_update, "-", "-", "-"
#         df_latest_daily = pd.DataFrame()
#         try:
#             # Ensure LAST_UPDATE_TIME is datetime before .dt accessor
#             if not pd.api.types.is_datetime64_any_dtype(df_realtime['LAST_UPDATE_TIME']):
#                 df_realtime['LAST_UPDATE_TIME'] = pd.to_datetime(df_realtime['LAST_UPDATE_TIME'], errors='coerce',
#                                                                  utc=True)
#             df_realtime.dropna(subset=['LAST_UPDATE_TIME'], inplace=True)  # Drop if conversion failed
#
#             df_realtime['DATE'] = df_realtime['LAST_UPDATE_TIME'].dt.tz_convert(DISPLAY_TIMEZONE).dt.date
#             latest_idx_per_day = df_realtime.loc[
#                 df_realtime.groupby([FLIGHT_ID_COLUMN, 'DATE'])["LAST_UPDATE_TIME"].idxmax()].index
#             df_latest_daily = df_realtime.loc[latest_idx_per_day].copy()
#         except Exception as e:
#             print(f"Warning: JUB Billing daily latest logic failed ({e}), using overall latest.")
#             latest_idx = df_realtime.loc[df_realtime.groupby(FLIGHT_ID_COLUMN)["LAST_UPDATE_TIME"].idxmax()].index
#             df_latest_daily = df_realtime.loc[latest_idx].copy()
#         if df_latest_daily.empty: return no_update, "-", "-", "-"
#         df_with_appearance = process_jub_billing_data(df_latest_daily)  # This function needs LATITUDE, LONGITUDE
#
#         total_processed_for_billing = len(df_with_appearance)
#         surely_passed_count = len(df_with_appearance[df_with_appearance['Appearance'] == "Surely Passed"])
#         investigate_count = len(df_with_appearance[df_with_appearance['Appearance'] == "Investigate"])
#
#         # Ensure all columns potentially displayed in jub-billing-data-table are present before to_dict
#         # Columns listed in update_jub_billing_table: FLIGHT_ID_COLUMN, FLIGHT_CALLSIGN, AIRCRAFT_MODEL, REGISTRATION, ORIGIN, DESTINATION, LATITUDE, LONGITUDE, ALTITUDE, LAST_UPDATE_TIME, Appearance
#         # Add any missing ones with default 'N/A' or np.nan
#         display_cols_check = [FLIGHT_ID_COLUMN, 'FLIGHT_CALLSIGN', 'AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN',
#                               'DESTINATION', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'LAST_UPDATE_TIME', 'Appearance']
#         for col in display_cols_check:
#             if col not in df_with_appearance.columns:
#                 if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
#                     df_with_appearance[col] = np.nan
#                 elif col == 'LAST_UPDATE_TIME':
#                     df_with_appearance[col] = pd.NaT  # or None
#                 else:
#                     df_with_appearance[col] = 'N/A'
#
#         df_with_appearance = df_with_appearance.replace({pd.NA: None, np.nan: None, pd.NaT: None})
#         return (df_with_appearance.to_dict('records'), f"{total_processed_for_billing:,}", f"{surely_passed_count:,}",
#                 f"{investigate_count:,}")
#     except Exception as e:
#         print(f"Error in update_processed_billing_data_and_kpis: {e}");
#         traceback.print_exc()
#         return no_update, "-", "-", "-"
#
#
# @callback(
#     Output('jub-billing-data-table', 'data'),
#     Output('jub-billing-data-table', 'columns'),
#     Input('processed-billing-data-store', 'data'),
#     prevent_initial_call=True
# )
# def update_jub_billing_table(processed_billing_data):
#     if not processed_billing_data: return [], []
#     try:
#         df_billing = pd.DataFrame(processed_billing_data)
#         if df_billing.empty: return [], []
#         # Define the desired order and ensure all these columns are at least present
#         jub_billing_cols_ordered = [FLIGHT_ID_COLUMN, 'FLIGHT_CALLSIGN', 'AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN',
#                                     'DESTINATION', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'LAST_UPDATE_TIME',
#                                     'Appearance']
#
#         # Ensure all expected columns exist, add if not
#         for col in jub_billing_cols_ordered:
#             if col not in df_billing.columns:
#                 if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
#                     df_billing[col] = np.nan
#                 elif col == 'LAST_UPDATE_TIME':
#                     df_billing[col] = pd.NaT
#                 else:
#                     df_billing[col] = 'N/A'  # Default for string columns
#
#         df_display = df_billing[jub_billing_cols_ordered].copy()  # Select in desired order
#
#         if 'LAST_UPDATE_TIME' in df_display.columns:
#             df_display['LAST_UPDATE_TIME'] = pd.to_datetime(df_display['LAST_UPDATE_TIME'], errors='coerce', utc=True)
#             if pd.api.types.is_datetime64_any_dtype(df_display['LAST_UPDATE_TIME']):
#                 try:
#                     # Create a new column for display to keep original for sorting if needed
#                     df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].dt.tz_convert(
#                         DISPLAY_TIMEZONE).dt.strftime('%Y-%m-%d %H:%M:%S')
#                 except Exception:  # Handle cases where conversion might fail for some rows
#                     df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].astype(str)  # Fallback
#             else:
#                 df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].astype(str)
#
#         jub_columns = []
#         for col_id in jub_billing_cols_ordered:
#             name = col_id.replace('_', ' ').title()
#             if col_id == FLIGHT_ID_COLUMN: name = "Flight ID"
#
#             current_col_id_for_data = col_id
#             if col_id == 'LAST_UPDATE_TIME':
#                 name = 'Last Seen (EAT)'
#                 # Use the display column if it exists
#                 current_col_id_for_data = 'LAST_UPDATE_TIME_DISPLAY' if 'LAST_UPDATE_TIME_DISPLAY' in df_display.columns else 'LAST_UPDATE_TIME'
#
#             jub_columns.append({"name": name, "id": current_col_id_for_data})
#
#         # Ensure the data being sent to dict uses the potentially renamed column for time
#         final_display_cols_for_dict = [c['id'] for c in jub_columns]
#         # Make sure all these columns are actually in df_display before to_dict
#         final_display_cols_present = [col for col in final_display_cols_for_dict if col in df_display.columns]
#
#         df_dict = df_display[final_display_cols_present].replace({pd.NA: None, np.nan: None, pd.NaT: None}).to_dict(
#             "records")
#         return df_dict, jub_columns
#     except Exception as e:
#         print(f"Error updating JUB billing table: {e}");
#         traceback.print_exc();
#         return [], []
#
#
# @callback(
#     Output('jub-billing-map', 'figure'),
#     Output('jub-manual-points-store', 'data'),
#     Output('jub-manual-point-info', 'children'),
#     Output('jub-manual-lat-input', 'value'),
#     Output('jub-manual-lon-input', 'value'),
#     Input('processed-billing-data-store', 'data'),
#     Input('jub-add-point-button', 'n_clicks'),
#     Input("jub-billing-data-table", "selected_rows"),
#     State('jub-manual-lat-input', 'value'),
#     State('jub-manual-lon-input', 'value'),
#     State('jub-manual-points-store', 'data'),
#     State("jub-billing-data-table", "data")
# )
# def update_jub_billing_map(
#         processed_data,
#         n_clicks,
#         selected_rows,
#         manual_lat,
#         manual_lon,
#         existing_manual_points,
#         table_data
# ):
#     triggered_id = ctx.triggered_id
#     new_point_info_children = []
#     cleared_lat_val = dash.no_update
#     cleared_lon_val = dash.no_update
#     map_font_color_billing = 'black'
#
#     df_billing_map_base = pd.DataFrame()
#     if processed_data:
#         df_billing_map_base = pd.DataFrame(processed_data)
#
#     if triggered_id == 'jub-add-point-button':
#         if manual_lat is not None and manual_lon is not None:
#             try:
#                 lat_val = float(manual_lat);
#                 lon_val = float(manual_lon)
#                 if -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
#                     new_point = {'LATITUDE': lat_val, 'LONGITUDE': lon_val, 'Appearance': 'new entry',
#                                  FLIGHT_ID_COLUMN: f'manual_{lat_val}_{lon_val}',
#                                  'FLIGHT_CALLSIGN': 'new entry', 'ALTITUDE': 15000}
#                     for col in ['REGISTRATION', 'AIRCRAFT_MODEL', 'ORIGIN', 'DESTINATION', 'SQUAWK',
#                                 'LAST_UPDATE_TIME']:
#                         if col == 'LAST_UPDATE_TIME':
#                             new_point[col] = pd.NaT
#                         else:
#                             new_point[col] = 'N/A'
#                     existing_manual_points.append(new_point)
#                     new_point_info_children = [
#                         dbc.Alert(f"Added: Lat {lat_val}, Lon {lon_val}", color="success", dismissable=True,
#                                   duration=3000)]
#                     cleared_lat_val = '';
#                     cleared_lon_val = ''
#                 else:
#                     new_point_info_children = [
#                         dbc.Alert("Error: Lat/Lon out of range.", color="danger", dismissable=True, duration=3000)]
#             except ValueError:
#                 new_point_info_children = [
#                     dbc.Alert("Error: Invalid lat/lon format.", color="danger", dismissable=True, duration=3000)]
#         else:
#             new_point_info_children = [
#                 dbc.Alert("Please enter both lat/lon.", color="warning", dismissable=True, duration=3000)]
#
#     plot_df_billing = df_billing_map_base.copy()
#     if existing_manual_points:
#         manual_points_df = pd.DataFrame(existing_manual_points)
#         if not manual_points_df.empty:
#
#             plot_df_billing = pd.concat([plot_df_billing, manual_points_df], ignore_index=True)
#
#     if selected_rows and table_data:
#         try:
#             selected_ids = {
#                 table_data[i][FLIGHT_ID_COLUMN] for i in selected_rows
#                 if
#                 i < len(table_data) and FLIGHT_ID_COLUMN in table_data[i] and table_data[i][FLIGHT_ID_COLUMN] not in [
#                     'N/A', None, '']
#             }
#             if selected_ids:
#                 plot_df_billing = plot_df_billing[
#                     plot_df_billing[FLIGHT_ID_COLUMN].isin(selected_ids) |
#                     (plot_df_billing['Appearance'] == 'new entry')
#                     ].copy()
#             else:
#                 plot_df_billing = plot_df_billing[plot_df_billing[
#                                                       'Appearance'] == 'new entry'].copy() if not plot_df_billing.empty else pd.DataFrame()
#         except Exception as e_filter:
#             print(f"Error applying JUB table selection filter to map: {e_filter}")
#             plot_df_billing = plot_df_billing[
#                 plot_df_billing['Appearance'] == 'new entry'].copy() if not plot_df_billing.empty else pd.DataFrame()
#
#     if plot_df_billing.empty:
#         return create_empty_map_figure("No JUB FIR Billing Data Matching Selection",
#                                        map_style=JUB_BILLING_MAP_STYLE), existing_manual_points, new_point_info_children, cleared_lat_val, cleared_lon_val
#
#     plot_cols_needed = ['LATITUDE', 'LONGITUDE', 'Appearance', 'ALTITUDE', 'FLIGHT_CALLSIGN', 'REGISTRATION',
#                         'AIRCRAFT_MODEL', 'ORIGIN', 'DESTINATION', 'SQUAWK', FLIGHT_ID_COLUMN, 'LAST_UPDATE_TIME']
#     for col in plot_cols_needed:
#         if col not in plot_df_billing.columns:
#             if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
#                 plot_df_billing[col] = np.nan
#             elif col == 'LAST_UPDATE_TIME':
#                 plot_df_billing[col] = pd.NaT
#             else:
#                 plot_df_billing[col] = 'N/A'
#
#     plot_df_billing.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
#     if plot_df_billing.empty:
#         return create_empty_map_figure("No Valid JUB FIR Billing Data Matching Selection",
#                                        map_style=JUB_BILLING_MAP_STYLE), existing_manual_points, new_point_info_children, cleared_lat_val, cleared_lon_val
#
#     plot_df_billing['ALTITUDE_plot'] = pd.to_numeric(plot_df_billing['ALTITUDE'], errors='coerce').fillna(
#         10000)  # for size
#     plot_df_billing['FLIGHT_CALLSIGN_display'] = plot_df_billing['FLIGHT_CALLSIGN'].fillna('N/A')
#
#     fig_billing = px.scatter_mapbox(
#         plot_df_billing, lat="LATITUDE", lon="LONGITUDE", color="Appearance", size="ALTITUDE_plot",
#         size_max=12,  # Adjusted size
#         hover_name="FLIGHT_CALLSIGN_display",  # Use cleaned callsign
#         hover_data={  # Ensure all hover data keys exist in plot_df_billing or are handled
#             "REGISTRATION": True, "LATITUDE": ":.4f", "LONGITUDE": ":.4f", "ALTITUDE": ":.0f",
#             "AIRCRAFT_MODEL": True, "ORIGIN": True, "DESTINATION": True,
#
#             FLIGHT_ID_COLUMN: True,
#             "Appearance": True
#         },
#         color_discrete_map={"Surely Passed": "#28a745", "Investigate": "#ffc107", "Not Passed": "#dc3545",
#                             "new entry": "#6f42c1"},
#         custom_data=[FLIGHT_ID_COLUMN],
#         zoom=4.8, center={"lat": JUB_SPECIFIC_CENTER_LAT, "lon": JUB_SPECIFIC_CENTER_LON}, height=550
#     )
#     fig_billing.update_layout(mapbox_style=JUB_BILLING_MAP_STYLE, margin={"r": 10, "t": 10, "l": 10, "b": 10},
#                               legend_title_text='Flight Status',
#                               legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
#                                           font=dict(color=map_font_color_billing)),
#                               font=dict(color=map_font_color_billing),
#                               uirevision=f"{str(selected_rows)}-{str(existing_manual_points)}"
#                               # To maintain zoom on data update
#                               )
#     fig_billing.update_traces(
#         textfont=dict(color=map_font_color_billing, size=ON_MAP_TEXT_SIZE - 1),  # Smaller text
#         marker=dict(sizemin=4)  # Ensure markers are visible
#     )
#     return fig_billing, existing_manual_points, new_point_info_children, cleared_lat_val, cleared_lon_val
#
#
# @callback(
#     Output("download-jub-selected-csv", "data"),
#     Input("download-jub-selected-button", "n_clicks"),
#     State("jub-billing-data-table", "selected_rows"),
#     State("jub-billing-data-table", "data"),  # This is data currently in the table component
#     prevent_initial_call=True
# )
# def download_selected_jub_billing_csv(n_clicks, selected_row_indices, all_table_data):
#     if n_clicks is None or n_clicks == 0:
#         return dash.no_update
#     if not selected_row_indices:
#         # Optionally provide user feedback e.g. via a dbc.Alert
#         print("No JUB billing rows selected for download.")
#         return dash.no_update
#     if not all_table_data:  # Check if table_data itself is empty
#         print("No data in JUB billing table to select from.")
#         return dash.no_update
#
#     try:
#         # selected_data is a list of dicts
#         selected_data = [all_table_data[i] for i in selected_row_indices if i < len(all_table_data)]
#         if not selected_data:
#             print("Selected JUB billing row indices are invalid or resulted in empty data.")
#             return dash.no_update
#
#         df_selected_billing = pd.DataFrame(selected_data)
#         timestamp = datetime.now(DISPLAY_TIMEZONE).strftime("%Y%m%d_%H%M%S")
#         filename = f"jub_billing_selected_rows_{timestamp}.csv"
#
#         # Use dcc.send_data_frame for sending pandas DataFrame as CSV
#         return dcc.send_data_frame(df_selected_billing.to_csv, filename=filename, index=False, encoding='utf-8')
#     except IndexError:
#         print("Error creating JUB selected CSV: IndexError on selection.")
#         # Potentially feedback to user
#         return dash.no_update
#     except Exception as e:
#         print(f"Error creating JUB selected CSV: {e}")
#         traceback.print_exc()
#         # Potentially feedback to user
#         return dash.no_update
#
# # --- Run the App ---
# if __name__ == "__main__":
#     print("-" * 60)
#     print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Initializing Flight Investigation Tool...")
#     print(
#         f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Live Data Logic: Latest record PER FLIGHT PER DAY for JUB Billing")
#     print(
#         f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Live Data Source: API -> In-Memory Store ({API_BASE_URL})")
#     try:
#         app.run(port=5323, debug=False)  # Consider debug=False for production
#     except KeyboardInterrupt:
#         print("\nKeyboardInterrupt received. Shutting down Dash server...")
#     finally:
#         print("Dash server stopped.")


# -*- coding: utf-8 -*-
import json
import dash_auth
import traceback
from datetime import datetime, timezone, timedelta
import pytz
import numpy as np
import pandas as pd
import dash
from dash import dcc, html, callback, Input, Output, State, dash_table, ctx, no_update
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import Dashauth  # Assuming this is your custom auth module
import flight_client  # Assuming this is your custom flight client

import requests
import csv
from io import StringIO

# --- Constants ---
API_BASE_URL = "http://13.239.238.138:5070"
FETCH_LIMIT = 100000
FETCH_INTERVAL_SECONDS = 20
FLIGHT_ID_COLUMN = "flight_id"
ON_MAP_TEXT_SIZE = 9
DISPLAY_TIMEZONE = pytz.timezone('Africa/Nairobi')
PROCESSING_TIMEZONE = pytz.utc
PLOTLY_TEMPLATE = "plotly_dark"
CHART_FONT_COLOR = "#adb5bd"
CHART_PAPER_BG = 'rgba(0,0,0,0)'
CHART_PLOT_BG = 'rgba(0,0,0,0)'
NA_REPLACE_VALUES = ['', 'nan', 'NaN', 'None', 'null', 'NONE', 'NULL', '#N/A', 'N/A', 'NA', '-']

JUB_SOUTH_SUDAN_MIN_LAT = 3.0
JUB_SOUTH_SUDAN_MAX_LAT = 13.0
JUB_SOUTH_SUDAN_MIN_LON = 24.0
JUB_SOUTH_SUDAN_MAX_LON = 36.0
JUB_SPECIFIC_MIN_LAT = 4.96
JUB_SPECIFIC_MAX_LAT = 10.76
JUB_SPECIFIC_MIN_LON = 26.76
JUB_SPECIFIC_MAX_LON = 32.62
JUB_SPECIFIC_CENTER_LAT = (JUB_SPECIFIC_MIN_LAT + JUB_SPECIFIC_MAX_LAT) / 2
JUB_SPECIFIC_CENTER_LON = (JUB_SPECIFIC_MIN_LON + JUB_SPECIFIC_MAX_LON) / 2
JUB_BILLING_MAP_STYLE = "carto-positron"

# --- Airline Data Lookup Functions & Initialization ---
AIRLINE_DATA_URL = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat'
# These will store: code -> (name, country)
iata_to_airline_map = {}
icao_to_airline_map = {}


def build_airline_dicts():
    global iata_to_airline_map, icao_to_airline_map
    temp_iata_map = {}
    temp_icao_map = {}
    print(
        f"DEBUG: [{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Attempting to fetch airline data from OpenFlights...")
    try:
        resp = requests.get(AIRLINE_DATA_URL, timeout=15)
        resp.raise_for_status()
        print(f"DEBUG: Successfully fetched airline data file. Status: {resp.status_code}")

        reader = csv.reader(StringIO(resp.text))
        for row in reader:
            if len(row) < 7:  # Ensure row has enough elements for country (index 6)
                continue

            name = row[1].strip()
            # alias = row[2].strip() # Not currently used
            iata = row[3].strip().upper()
            icao = row[4].strip().upper()
            # callsign_dat = row[5].strip() # Not currently used
            country = row[6].strip()  # Country is at index 6
            # active = row[7].strip() # Not currently used

            # Use 'N/A' for missing/invalid names or countries from the source
            if not name or name == "\\N": name = "N/A"
            if not country or country == "\\N": country = "N/A"

            info = (name, country)  # Store as (name, country) tuple

            if name != "N/A":  # Only store if airline name is valid
                if iata and iata != "\\N" and len(iata) == 2:
                    temp_iata_map[iata] = info
                if icao and icao != "\\N" and len(icao) == 3:
                    temp_icao_map[icao] = info

        iata_to_airline_map = temp_iata_map
        icao_to_airline_map = temp_icao_map
        print(
            f"DEBUG: [{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Loaded {len(iata_to_airline_map)} IATA and {len(icao_to_airline_map)} ICAO airline codes with country info.")
        if not iata_to_airline_map and not icao_to_airline_map:
            print("DEBUG: WARNING - Airline maps are empty after parsing!")

    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Failed to fetch airline data: {e}")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to parse airline data: {e}")
        traceback.print_exc()


def lookup_airline_info_by_code(code_to_lookup):
    """Looks up an airline by IATA or ICAO code."""
    code_to_lookup = str(code_to_lookup).strip().upper()
    if not code_to_lookup: return None  # No code provided

    # Prioritize ICAO for 3-char codes, IATA for 2-char codes
    if len(code_to_lookup) == 3 and code_to_lookup in icao_to_airline_map:
        return icao_to_airline_map[code_to_lookup]  # Returns (name, country)
    elif len(code_to_lookup) == 2 and code_to_lookup in iata_to_airline_map:
        return iata_to_airline_map[code_to_lookup]  # Returns (name, country)
    elif code_to_lookup in icao_to_airline_map:  # General fallback if length doesn't match typical
        return icao_to_airline_map[code_to_lookup]
    elif code_to_lookup in iata_to_airline_map:
        return iata_to_airline_map[code_to_lookup]
    return None  # (name, country) not found


def get_airline_info_from_callsign(callsign_str):
    """
    Attempts to derive airline name and country from a flight callsign.
    Returns a tuple (airline_name, country_name). Defaults to ('N/A', 'N/A').
    """
    default_info = ('N/A', 'N/A')

    if not callsign_str or pd.isna(callsign_str) or str(callsign_str).strip().upper() in NA_REPLACE_VALUES + ['N/A',
                                                                                                              'NONE',
                                                                                                              'NULL',
                                                                                                              '']:
        return default_info

    callsign_str = str(callsign_str).strip().upper()

    if not iata_to_airline_map and not icao_to_airline_map:
        return ('Airline Data Missing', 'Airline Data Missing')

    airline_info_found = None

    # Try 3-letter ICAO prefix first
    if len(callsign_str) >= 3:
        prefix = callsign_str[:3]
        if prefix.isalpha():  # ICAO codes are typically 3 letters
            info = lookup_airline_info_by_code(prefix)
            if info and info[0] != "N/A":  # Check if name is valid
                airline_info_found = info

    # If ICAO didn't yield a valid name, or if callsign is shorter, try 2-letter IATA prefix
    if (not airline_info_found or airline_info_found[0] == "N/A") and len(callsign_str) >= 2:
        prefix = callsign_str[:2]
        # IATA codes are 2 characters (can be alphanumeric, e.g., U2 for EasyJet)
        if prefix.isalnum():
            info = lookup_airline_info_by_code(prefix)
            if info and info[0] != "N/A":  # Check if name is valid
                airline_info_found = info

    return airline_info_found if airline_info_found and airline_info_found[0] != "N/A" else default_info


build_airline_dicts()  # Initialize airline data on script startup

# --- Initialize Fetcher ---
fetcher = None
if flight_client:
    try:
        fetcher = flight_client.FlightDataFetcher(base_url=API_BASE_URL, api_endpoint="/api/flights/latest_unique",
                                                  fetch_limit=FETCH_LIMIT)
    except Exception as e:
        print(f"ERROR: Failed to initialize FlightDataFetcher: {e}")
else:
    print("ERROR: flight_client module not loaded.")

# --- Initialize App ---
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.VAPOR],
    suppress_callback_exceptions=True, title="Flight Investigation Tool"
)
app.secret_key = 'a_default_secret_key_replace_if_needed_with_a_strong_random_key'
server = app.server

# --- Authentication ---
try:
    if Dashauth and hasattr(Dashauth, 'VALID_USERNAME_PASSWORD_PAIRS'):
        auth = dash_auth.BasicAuth(app, Dashauth.VALID_USERNAME_PASSWORD_PAIRS)
    else:
        auth = None
except Exception as auth_e:
    print(f"ERROR setting up authentication: {auth_e}.")
    auth = None


# --- Helper Functions ---
def create_empty_map_figure(message="No data available", map_style="carto-positron"):
    map_font_color = CHART_FONT_COLOR if map_style != "open-street-map" else 'black'
    layout = go.Layout(
        mapbox=dict(style=map_style, center={"lat": JUB_SPECIFIC_CENTER_LAT, "lon": JUB_SPECIFIC_CENTER_LON}, zoom=5),
        margin=dict(r=5, t=5, l=5, b=5), paper_bgcolor=CHART_PAPER_BG,
        plot_bgcolor=CHART_PLOT_BG, font=dict(color=map_font_color))
    layout.annotations = [
        go.layout.Annotation(text=message, align='center', showarrow=False, xref='paper', yref='paper', x=0.5, y=0.5,
                             font=dict(size=16, color=map_font_color))]
    return go.Figure(data=[], layout=layout)


def create_kpi_card(title, value_id, tooltip_text):
    return dbc.Card([
        dbc.CardHeader(html.H6(title, className="text-white-50"), id=f"header-{value_id}"),
        dbc.CardBody(html.H4(id=value_id, className="text-info text-center my-auto", children="-")),
        dbc.Tooltip(tooltip_text, target=f"header-{value_id}", placement='bottom')
    ], className="h-100 d-flex flex-column bg-dark")


def process_jub_billing_data(df: pd.DataFrame) -> pd.DataFrame:
    df_processed = df.copy()
    df_processed['LATITUDE'] = pd.to_numeric(df_processed['LATITUDE'], errors='coerce')
    df_processed['LONGITUDE'] = pd.to_numeric(df_processed['LONGITUDE'], errors='coerce')
    df_processed.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)

    cond_specific_zone = (
            (df_processed['LATITUDE'] >= JUB_SPECIFIC_MIN_LAT) & (df_processed['LATITUDE'] <= JUB_SPECIFIC_MAX_LAT) &
            (df_processed['LONGITUDE'] >= JUB_SPECIFIC_MIN_LON) & (df_processed['LONGITUDE'] <= JUB_SPECIFIC_MAX_LON)
    )
    cond_south_sudan = (
            (df_processed['LATITUDE'] >= JUB_SOUTH_SUDAN_MIN_LAT) & (
            df_processed['LATITUDE'] <= JUB_SOUTH_SUDAN_MAX_LAT) &
            (df_processed['LONGITUDE'] >= JUB_SOUTH_SUDAN_MIN_LON) & (
                    df_processed['LONGITUDE'] <= JUB_SOUTH_SUDAN_MAX_LON)
    )
    df_processed['Appearance'] = "Not Passed"
    df_processed.loc[cond_south_sudan & ~cond_specific_zone, 'Appearance'] = "Investigate"
    df_processed.loc[cond_specific_zone, 'Appearance'] = "Surely Passed"
    return df_processed


# --- Main Application Layout ---
app.layout = dbc.Container([
    dcc.Store(id='realtime-flight-data-store'),
    dcc.Store(id='processed-billing-data-store'),
    dcc.Store(id='jub-manual-points-store', data=[]),
    dcc.Download(id="download-jub-selected-csv"),

    dbc.Row([html.Br(),
             dbc.Col(html.Img(src=app.get_asset_url("ss_logo.png"), height="40px", className="rounded-circle"),
                     className="mt-3")], align="center", className="g-0"),
    dbc.Row([dbc.Col(html.H1("Flight Investigation Tool", className="text-center text-primary mb-2"), width=12),
             dbc.Col(html.P(id='last-update-timestamp', className="text-center text-muted mb-4 small"), width=12)]),

    # --- Section I: JUB FIR Billing (Map & Controls) ---
    dbc.Row([
        dbc.Col(html.H2("I. JUB FIR Billing", className="text-info mt-4 mb-2 text-center"), width=12)
    ], justify="center", align="center", className="mt-3"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("JUB FIR Billing Map & Controls"),
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col(dcc.Graph(id='jub-billing-map', style={'height': '60vh'}), md=9),
                        dbc.Col([  # Map Controls Column
                            html.H5("Map Controls", className="card-title"),
                            dbc.Label("Manual Point Entry:", className="mt-2"),
                            dbc.Input(id='jub-manual-lat-input', type='number', placeholder='Latitude (e.g, 7.86)',
                                      step=0.01, className="mb-2"),
                            dbc.Input(id='jub-manual-lon-input', type='number', placeholder='Longitude (e.g, 29.69)',
                                      step=0.01, className="mb-2"),
                            dbc.Button('Add Point to Billing Map', id='jub-add-point-button', n_clicks=0,
                                       color="primary", className="w-100 mb-3"),
                            html.Div(id='jub-manual-point-info', className="small text-muted mb-3"),
                            html.H6("Map Legend", className="mt-3"),
                            html.Ul([
                                html.Li(html.Span("Surely Passed", style={'color': '#28a745', 'fontWeight': 'bold'})),
                                html.Li(html.Span("Investigate", style={'color': '#ffc107', 'fontWeight': 'bold'})),
                                html.Li(html.Span("Not Passed", style={'color': '#dc3545', 'fontWeight': 'bold'})),
                                html.Li(html.Span("new entry", style={'color': '#6f42c1', 'fontWeight': 'bold'})),
                            ], style={'listStyleType': 'none', 'paddingLeft': 0, 'fontSize': '0.9em'}),
                        ], md=3),  # End of Map Controls Column (md=3)
                    ])  # End of Inner Row for map and controls
                ])  # End of CardBody for map section
            ], className="mb-3 bg-dark")  # End of Card for map section
        ], width=12),  # End of Column for map section card
    ]),  # End of Row for map section
    dbc.Row([  # KPIs Row (still part of Section I conceptually or follows it)
        dbc.Col(create_kpi_card("JUB FIR Flights (Live)", "jub-fir-kpi-total",
                                "Total flights processed for JUB FIR billing status from live data."), width=12, md=4,
                className="mb-3"),
        dbc.Col(create_kpi_card("Flights in Hot Zone", "jub-fir-kpi-hotzone",
                                "Flights categorized as 'Surely Passed' (within specific hot zone)."), width=12, md=4,
                className="mb-3"),
        dbc.Col(create_kpi_card("Flights to Investigate", "jub-fir-kpi-investigate",
                                "Flights in JUB FIR general airspace, outside hot zone."), width=12, md=4,
                className="mb-3"),
    ], className="mb-2 g-3", justify="center"),
    # --- End of Section I ---

    # --- Section II: Billing Data Table ---
    dbc.Row([
        dbc.Col(html.H2("II. Billing Data Table", className="text-info mt-3 mb-2 text-center"), width=12)
    ], justify="center", align="center"),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Billing Adjudication Table (Based on Live Data & JUB FIR Logic)"),
                dbc.CardBody([
                    dash_table.DataTable(
                        id="jub-billing-data-table",
                        row_selectable='multi',
                        selected_rows=[],
                        style_table={"overflowX": "auto", "marginTop": "10px"},
                        style_filter={'backgroundColor': 'rgba(102,255,255,0.7)', 'border': '1px solid #55555580'},
                        style_header={'backgroundColor': '#303030', 'color': '#adb5bd', 'fontWeight': 'bold',
                                      'border': '1px solid #444'},
                        style_cell={
                            'textAlign': 'left', 'padding': '10px',
                            'backgroundColor': '#1a1a1a', 'color': '#adb5bd',
                            'border': '1px solid #303030',
                            'minWidth': '100px', 'maxWidth': '200px', 'whiteSpace': 'normal', 'height': 'auto'
                        },
                        style_data_conditional=[
                            {'if': {'row_index': 'odd'}, 'backgroundColor': '#222222'},
                            {'if': {'state': 'selected'}, 'backgroundColor': 'rgba(0, 116, 217, 0.3)',
                             'border': '1px solid #0074D9'},
                            {'if': {'filter_query': '{Appearance} = "Surely Passed"', 'column_id': 'Appearance'},
                             'backgroundColor': 'rgba(40, 167, 69, 0.4)', 'color': 'white'},
                            {'if': {'filter_query': '{Appearance} = "Investigate"', 'column_id': 'Appearance'},
                             'backgroundColor': 'rgba(255, 193, 7, 0.4)', 'color': 'black'},
                            {'if': {'filter_query': '{Appearance} = "Not Passed"', 'column_id': 'Appearance'},
                             'backgroundColor': 'rgba(220, 53, 69, 0.4)', 'color': 'white'},
                        ],
                        page_size=10,
                        sort_action='native',
                        filter_action='native',
                        data=[],
                        columns=[],
                    ),
                    dbc.Button(
                        "Download Selected Billing Rows",
                        id="download-jub-selected-button",
                        color="info",
                        className="mt-3",
                        n_clicks=0
                    )
                ])  # End of CardBody for table section
            ], className="bg-dark")  # End of Card for table section
        ], width=12)  # End of Column for table section card
    ], className="mb-4"),  # End of Row for table section
    # --- End of Section II ---

    # --- Section III: Airline Lookup Tool ---
    dbc.Row([
        dbc.Col(html.H2("III. Airline Lookup Tool", className="text-info mt-4 mb-2 text-center"), width=12)
    ], justify="center", align="center", className="mt-3"),  # Title row for Section III
    dbc.Row([  # Content row for Section III
        dbc.Col([  # Column to center the card
            dbc.Card([
                dbc.CardHeader("Find Airline by Callsign/Code"),
                dbc.CardBody([
                    dbc.InputGroup([
                        dbc.Input(id='airline-lookup-input',
                                  placeholder='Enter Callsign Prefix (e.g., UAE, BAW, ET) or Code', type='text',
                                  className="mb-0"),
                        dbc.Button('Lookup Airline', id='airline-lookup-button', n_clicks=0, color="primary")
                    ]),
                    html.Div(id='airline-lookup-result', className="mt-3 p-2 border rounded bg-light text-dark",
                             style={'minHeight': '40px'})
                ])  # End of CardBody for lookup tool
            ], className="mb-3 bg-dark")  # End of Card for lookup tool
        ], width=12, md=8, lg=6)  # Centering column for the card
    ], justify="center", className="mb-4"),  # End of content row for Section III
    # --- End of Section III ---

    dcc.Interval(id="interval-component", interval=FETCH_INTERVAL_SECONDS * 1000, n_intervals=0),
    dbc.Row([dbc.Col([html.Footer([html.P("©️ 2025 South Sudan Civil Aviation. Powered by Crawford Capital",
                                          className="text-center text-muted mt-4"), html.Div(
        [dbc.Button("Report Issue", color="link", className="text-muted", href="#", id="report-issue-btn"),
         html.Span(" | "),
         dbc.Button("Privacy Policy", color="link", className="text-muted", id="privacy-policy-btn", n_clicks=0)],
        className="text-center small")], className="py-4")])])
], fluid=True, className="dbc")  # End of main dbc.Container


# --- Callbacks ---
@callback(
    Output('realtime-flight-data-store', 'data'),
    Output('last-update-timestamp', 'children'),
    Input("interval-component", "n_intervals"),
    prevent_initial_call=False
)
def update_realtime_data_store(n_intervals):
    global fetcher
    fetch_time_display = datetime.now(DISPLAY_TIMEZONE)
    status_msg_base = f"Last Live Check: {fetch_time_display.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    if fetcher is None: return no_update, f"Fetcher Init Error | {status_msg_base}"
    try:
        df_batch = fetcher.fetch_next_batch()
        if df_batch is None: return no_update, f"Live API Fetch Error | {status_msg_base}"
        if df_batch.empty:
            last_ts_msg = fetcher.last_processed_timestamp or "initial"
            return no_update, f"No New Live Data (since {last_ts_msg}) | {status_msg_base}"

        df_processed_batch = df_batch.copy()
        df_processed_batch.columns = df_processed_batch.columns.astype(str).str.lower().str.strip()
        col_map = {'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'last_update': 'LAST_UPDATE_TIME',
                   'flight_id': FLIGHT_ID_COLUMN, 'model': 'AIRCRAFT_MODEL', 'alt': 'ALTITUDE', 'speed': 'SPEED',
                   'track': 'TRACK', 'callsign': 'FLIGHT_CALLSIGN', 'reg': 'REGISTRATION', 'origin': 'ORIGIN',
                   'destination': 'DESTINATION', 'flight': 'FLIGHT_NUMBER'}
        rename_map = {k: v for k, v in col_map.items() if k in df_processed_batch.columns}
        df_processed_batch.rename(columns=rename_map, inplace=True)

        essential_cols = ['LATITUDE', 'LONGITUDE', 'LAST_UPDATE_TIME', FLIGHT_ID_COLUMN]
        if 'FLIGHT_CALLSIGN' not in df_processed_batch.columns:
            df_processed_batch['FLIGHT_CALLSIGN'] = 'N/A'

        missing_essential = [c for c in essential_cols if c not in df_processed_batch.columns]
        if missing_essential:
            print(f"Warning: New LIVE batch missing essential columns: {missing_essential}. Skipping batch.")
            df_processed_batch = pd.DataFrame()
        else:
            df_processed_batch["LAST_UPDATE_TIME"] = pd.to_datetime(df_processed_batch["LAST_UPDATE_TIME"],
                                                                    errors='coerce', utc=True)
            num_cols = ['LATITUDE', 'LONGITUDE', 'ALTITUDE', 'SPEED', 'TRACK']
            str_cols = ['AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN', 'DESTINATION', 'FLIGHT_NUMBER', 'FLIGHT_CALLSIGN']

            for col in num_cols:
                if col in df_processed_batch.columns:
                    df_processed_batch[col] = pd.to_numeric(df_processed_batch[col], errors='coerce')

            df_processed_batch.dropna(subset=essential_cols, inplace=True)

            for col in str_cols:
                if col not in df_processed_batch.columns:
                    df_processed_batch[col] = 'N/A'
                df_processed_batch[col] = df_processed_batch[col].astype(str).fillna('N/A').str.strip().replace(
                    NA_REPLACE_VALUES, 'N/A', regex=False)

            if FLIGHT_ID_COLUMN in df_processed_batch.columns:
                df_processed_batch[FLIGHT_ID_COLUMN] = df_processed_batch[FLIGHT_ID_COLUMN].astype(str).fillna(
                    'N/A').str.strip().replace(NA_REPLACE_VALUES, 'N/A', regex=False)
                df_processed_batch = df_processed_batch[df_processed_batch[FLIGHT_ID_COLUMN] != 'N/A']
            else:
                df_processed_batch = pd.DataFrame()

        if df_processed_batch.empty: return no_update, f"No Valid Live Data After Processing | {status_msg_base}"

        if 'LAST_UPDATE_TIME' in df_processed_batch.columns and pd.api.types.is_datetime64_any_dtype(
                df_processed_batch['LAST_UPDATE_TIME']):
            if df_processed_batch['LAST_UPDATE_TIME'].dt.tz is None:
                df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.tz_localize('UTC')
            else:
                df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.tz_convert('UTC')
            df_processed_batch['LAST_UPDATE_TIME'] = df_processed_batch['LAST_UPDATE_TIME'].dt.strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        else:
            df_processed_batch['LAST_UPDATE_TIME'] = None

        data_to_store = df_processed_batch.replace({pd.NA: None, pd.NaT: None, np.nan: None}).to_dict('records')
        return data_to_store, f"Live View Updated {fetch_time_display.strftime('%H:%M:%S')} | {len(data_to_store)} live pts"
    except Exception as e:
        print(f"ERROR in update_realtime_data_store: Unexpected failure - {e}")
        traceback.print_exc()
        return no_update, f"Live Data Process Error | {status_msg_base}"


@callback(
    Output('processed-billing-data-store', 'data'),
    Output('jub-fir-kpi-total', 'children'),
    Output('jub-fir-kpi-hotzone', 'children'),
    Output('jub-fir-kpi-investigate', 'children'),
    Input("realtime-flight-data-store", "data"),
    prevent_initial_call=True
)
def update_processed_billing_data_and_kpis(stored_data):
    if not stored_data: return no_update, "-", "-", "-"
    try:
        df_realtime = pd.DataFrame(stored_data)
        if df_realtime.empty: return no_update, "-", "-", "-"

        required_cols = [FLIGHT_ID_COLUMN, 'LAST_UPDATE_TIME', 'LATITUDE', 'LONGITUDE', 'AIRCRAFT_MODEL', 'ORIGIN',
                         'DESTINATION', 'FLIGHT_CALLSIGN', 'ALTITUDE', 'REGISTRATION']
        for col in required_cols:
            if col not in df_realtime.columns:
                if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
                    df_realtime[col] = np.nan
                elif col == 'LAST_UPDATE_TIME':
                    df_realtime[col] = pd.NaT
                else:
                    df_realtime[col] = 'N/A'

        df_realtime['LAST_UPDATE_TIME'] = pd.to_datetime(df_realtime['LAST_UPDATE_TIME'], errors='coerce', utc=True)
        df_realtime.dropna(subset=[FLIGHT_ID_COLUMN, 'LAST_UPDATE_TIME', 'LATITUDE', 'LONGITUDE'], inplace=True)
        df_realtime[FLIGHT_ID_COLUMN] = df_realtime[FLIGHT_ID_COLUMN].astype(str).fillna('N/A')
        df_realtime = df_realtime[df_realtime[FLIGHT_ID_COLUMN] != 'N/A']
        if df_realtime.empty: return no_update, "-", "-", "-"

        df_latest_daily = pd.DataFrame()
        try:
            if not pd.api.types.is_datetime64_any_dtype(df_realtime['LAST_UPDATE_TIME']):
                df_realtime['LAST_UPDATE_TIME'] = pd.to_datetime(df_realtime['LAST_UPDATE_TIME'], errors='coerce',
                                                                 utc=True)
            df_realtime.dropna(subset=['LAST_UPDATE_TIME'], inplace=True)

            df_realtime['DATE'] = df_realtime['LAST_UPDATE_TIME'].dt.tz_convert(DISPLAY_TIMEZONE).dt.date
            latest_idx_per_day = df_realtime.loc[
                df_realtime.groupby([FLIGHT_ID_COLUMN, 'DATE'])["LAST_UPDATE_TIME"].idxmax()].index
            df_latest_daily = df_realtime.loc[latest_idx_per_day].copy()
        except Exception as e:
            print(f"Warning: JUB Billing daily latest logic failed ({e}), using overall latest.")
            latest_idx = df_realtime.loc[df_realtime.groupby(FLIGHT_ID_COLUMN)["LAST_UPDATE_TIME"].idxmax()].index
            df_latest_daily = df_realtime.loc[latest_idx].copy()

        if df_latest_daily.empty: return no_update, "-", "-", "-"

        df_with_appearance = process_jub_billing_data(df_latest_daily)

        if 'FLIGHT_CALLSIGN' in df_with_appearance.columns:
            airline_info_tuples = df_with_appearance['FLIGHT_CALLSIGN'].apply(get_airline_info_from_callsign)
            df_with_appearance['AIRLINE_NAME'] = [info[0] for info in airline_info_tuples]
            df_with_appearance['AIRLINE_COUNTRY'] = [info[1] for info in airline_info_tuples]
        else:
            df_with_appearance['AIRLINE_NAME'] = 'N/A'
            df_with_appearance['AIRLINE_COUNTRY'] = 'N/A'

        total_processed_for_billing = len(df_with_appearance)
        surely_passed_count = len(df_with_appearance[df_with_appearance['Appearance'] == "Surely Passed"])
        investigate_count = len(df_with_appearance[df_with_appearance['Appearance'] == "Investigate"])

        display_cols_check = [FLIGHT_ID_COLUMN, 'FLIGHT_CALLSIGN', 'AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN',
                              'DESTINATION', 'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'LAST_UPDATE_TIME',
                              'Appearance', 'AIRLINE_NAME', 'AIRLINE_COUNTRY']
        for col in display_cols_check:
            if col not in df_with_appearance.columns:
                if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
                    df_with_appearance[col] = np.nan
                elif col == 'LAST_UPDATE_TIME':
                    df_with_appearance[col] = pd.NaT
                else:
                    df_with_appearance[col] = 'N/A'

        processed_records = df_with_appearance.replace({pd.NA: None, np.nan: None, pd.NaT: None}).to_dict('records')
        return (
        processed_records, f"{total_processed_for_billing:,}", f"{surely_passed_count:,}", f"{investigate_count:,}")
    except Exception as e:
        print(f"Error in update_processed_billing_data_and_kpis: {e}");
        traceback.print_exc()
        return no_update, "-", "-", "-"


@callback(
    Output('jub-billing-data-table', 'data'),
    Output('jub-billing-data-table', 'columns'),
    Input('processed-billing-data-store', 'data'),
    prevent_initial_call=True
)
def update_jub_billing_table(processed_billing_data):
    if not processed_billing_data:
        return [], []

    try:
        df_billing = pd.DataFrame(processed_billing_data)
        if df_billing.empty:
            return [], []

        jub_billing_cols_ordered = [
            FLIGHT_ID_COLUMN, 'FLIGHT_CALLSIGN', 'AIRLINE_NAME', 'AIRLINE_COUNTRY',
            'AIRCRAFT_MODEL', 'REGISTRATION', 'ORIGIN', 'DESTINATION',
            'LATITUDE', 'LONGITUDE', 'ALTITUDE', 'LAST_UPDATE_TIME', 'Appearance'
        ]

        for col in jub_billing_cols_ordered:
            if col not in df_billing.columns:
                if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
                    df_billing[col] = np.nan
                elif col == 'LAST_UPDATE_TIME':
                    df_billing[col] = pd.NaT
                else:
                    df_billing[col] = 'N/A'

        df_display = df_billing[jub_billing_cols_ordered].copy()

        if 'LAST_UPDATE_TIME' in df_display.columns:
            df_display['LAST_UPDATE_TIME'] = pd.to_datetime(df_display['LAST_UPDATE_TIME'], errors='coerce', utc=True)
            if pd.api.types.is_datetime64_any_dtype(df_display['LAST_UPDATE_TIME']):
                try:
                    df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].dt.tz_convert(
                        DISPLAY_TIMEZONE).dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:  # Fallback if conversion fails
                    df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].astype(str)
            else:
                df_display['LAST_UPDATE_TIME_DISPLAY'] = df_display['LAST_UPDATE_TIME'].astype(str)
        else:  # Ensure the column exists even if source is missing
            df_display['LAST_UPDATE_TIME_DISPLAY'] = 'N/A'

        jub_columns = []
        for col_id in jub_billing_cols_ordered:
            name = col_id.replace('_', ' ').title()
            if col_id == FLIGHT_ID_COLUMN: name = "Flight ID"
            if col_id == 'AIRLINE_NAME': name = "Airline Name"
            if col_id == 'AIRLINE_COUNTRY': name = "Airline Country"

            current_col_id_for_data = col_id
            if col_id == 'LAST_UPDATE_TIME':
                name = 'Last Seen (EAT)'
                current_col_id_for_data = 'LAST_UPDATE_TIME_DISPLAY'

            jub_columns.append({"name": name, "id": current_col_id_for_data, "editable": False})

        # Ensure all columns for dict are present in df_display
        dict_cols = [c['id'] for c in jub_columns]
        final_df_for_dict = pd.DataFrame()
        for c_id in dict_cols:
            if c_id in df_display.columns:
                final_df_for_dict[c_id] = df_display[c_id]
            else:  # Should mostly be handled by pre-population, but a failsafe.
                final_df_for_dict[c_id] = 'N/A'

        df_dict = final_df_for_dict.replace({pd.NA: None, np.nan: None, pd.NaT: None}).to_dict("records")
        return df_dict, jub_columns
    except Exception as e:
        print(f"Error updating JUB billing table: {e}");
        traceback.print_exc();
        return [], []


@callback(
    Output('jub-billing-map', 'figure'),
    Output('jub-manual-points-store', 'data'),
    Output('jub-manual-point-info', 'children'),
    Output('jub-manual-lat-input', 'value'),
    Output('jub-manual-lon-input', 'value'),
    Input('processed-billing-data-store', 'data'),
    Input('jub-add-point-button', 'n_clicks'),
    Input("jub-billing-data-table", "selected_rows"),
    State('jub-manual-lat-input', 'value'),
    State('jub-manual-lon-input', 'value'),
    State('jub-manual-points-store', 'data'),
    State("jub-billing-data-table", "data")
)
def update_jub_billing_map(
        processed_data,
        n_clicks_add_point,
        selected_table_rows,
        manual_lat,
        manual_lon,
        existing_manual_points_in_store,
        table_data_from_component_state
):
    triggered_id = ctx.triggered_id
    new_point_info_children = []
    cleared_lat_val = dash.no_update
    cleared_lon_val = dash.no_update
    current_manual_points = existing_manual_points_in_store if existing_manual_points_in_store is not None else []

    map_font_color_billing = 'black' if JUB_BILLING_MAP_STYLE == "open-street-map" else CHART_FONT_COLOR

    df_billing_map_base = pd.DataFrame()
    if processed_data:
        df_billing_map_base = pd.DataFrame(processed_data)

    if triggered_id == 'jub-add-point-button' and n_clicks_add_point > 0:
        if manual_lat is not None and manual_lon is not None:
            try:
                lat_val = float(manual_lat);
                lon_val = float(manual_lon)
                if -90 <= lat_val <= 90 and -180 <= lon_val <= 180:
                    new_point = {'LATITUDE': lat_val, 'LONGITUDE': lon_val, 'Appearance': 'new entry',
                                 FLIGHT_ID_COLUMN: f'manual_{lat_val}_{lon_val}_{n_clicks_add_point}',
                                 'FLIGHT_CALLSIGN': 'New Manual Entry', 'ALTITUDE': 15000,
                                 'AIRLINE_NAME': 'N/A', 'AIRLINE_COUNTRY': 'N/A'}  # Added country for manual
                    for col in ['REGISTRATION', 'AIRCRAFT_MODEL', 'ORIGIN', 'DESTINATION', 'SQUAWK',
                                'LAST_UPDATE_TIME']:
                        new_point[col] = pd.NaT if col == 'LAST_UPDATE_TIME' else 'N/A'

                    current_manual_points.append(new_point)
                    new_point_info_children.append(
                        dbc.Alert(f"Added: Lat {lat_val}, Lon {lon_val}", color="success", dismissable=True,
                                  duration=3000))
                    cleared_lat_val = '';
                    cleared_lon_val = ''
                else:
                    new_point_info_children.append(
                        dbc.Alert("Error: Lat/Lon out of range.", color="danger", dismissable=True, duration=3000))
            except ValueError:
                new_point_info_children.append(
                    dbc.Alert("Error: Invalid lat/lon format.", color="danger", dismissable=True, duration=3000))
        else:
            new_point_info_children.append(
                dbc.Alert("Please enter both lat/lon.", color="warning", dismissable=True, duration=3000))

    plot_df_billing = df_billing_map_base.copy()
    if current_manual_points:
        manual_points_df = pd.DataFrame(current_manual_points)
        if not manual_points_df.empty:
            plot_df_billing = pd.concat([plot_df_billing, manual_points_df], ignore_index=True)

    if selected_table_rows and table_data_from_component_state:
        try:
            selected_flight_ids = {
                table_data_from_component_state[i][FLIGHT_ID_COLUMN] for i in selected_table_rows
                if
                i < len(table_data_from_component_state) and FLIGHT_ID_COLUMN in table_data_from_component_state[i] and \
                table_data_from_component_state[i][FLIGHT_ID_COLUMN] not in ['N/A', None, '']
            }
            if selected_flight_ids:
                plot_df_billing = plot_df_billing[
                    plot_df_billing[FLIGHT_ID_COLUMN].isin(selected_flight_ids) |
                    (plot_df_billing['Appearance'] == 'new entry')  # Always show manual points if selected
                    ].copy()
            elif not plot_df_billing.empty and 'Appearance' in plot_df_billing.columns:  # If selection cleared, but manual points exist
                plot_df_billing = plot_df_billing[plot_df_billing['Appearance'] == 'new entry'].copy()

        except Exception as e_filter:
            print(f"Error applying JUB table selection filter to map: {e_filter}")
            if not plot_df_billing.empty and 'Appearance' in plot_df_billing.columns:
                plot_df_billing = plot_df_billing[plot_df_billing['Appearance'] == 'new entry'].copy()
            else:
                plot_df_billing = pd.DataFrame()

    if plot_df_billing.empty:
        return create_empty_map_figure("No JUB FIR Billing Data Matching Selection", map_style=JUB_BILLING_MAP_STYLE), \
            current_manual_points, new_point_info_children if new_point_info_children else dash.no_update, \
            cleared_lat_val, cleared_lon_val

    plot_cols_needed = ['LATITUDE', 'LONGITUDE', 'Appearance', 'ALTITUDE', 'FLIGHT_CALLSIGN', 'REGISTRATION',
                        'AIRCRAFT_MODEL', 'ORIGIN', 'DESTINATION', 'SQUAWK', FLIGHT_ID_COLUMN,
                        'LAST_UPDATE_TIME', 'AIRLINE_NAME', 'AIRLINE_COUNTRY']
    for col in plot_cols_needed:
        if col not in plot_df_billing.columns:
            if col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
                plot_df_billing[col] = np.nan
            elif col == 'LAST_UPDATE_TIME':
                plot_df_billing[col] = pd.NaT
            else:
                plot_df_billing[col] = 'N/A'

    plot_df_billing.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
    if plot_df_billing.empty:
        return create_empty_map_figure("No Valid JUB FIR Billing Data (After NaN drop)",
                                       map_style=JUB_BILLING_MAP_STYLE), \
            current_manual_points, new_point_info_children if new_point_info_children else dash.no_update, \
            cleared_lat_val, cleared_lon_val

    plot_df_billing['ALTITUDE_plot'] = pd.to_numeric(plot_df_billing['ALTITUDE'], errors='coerce').fillna(10000)
    plot_df_billing['FLIGHT_CALLSIGN_display'] = plot_df_billing['FLIGHT_CALLSIGN'].fillna('N/A')
    plot_df_billing['AIRLINE_NAME_display'] = plot_df_billing['AIRLINE_NAME'].fillna('N/A').astype(str)
    plot_df_billing['AIRLINE_COUNTRY_display'] = plot_df_billing['AIRLINE_COUNTRY'].fillna('N/A').astype(str)

    fig_billing = px.scatter_mapbox(
        plot_df_billing, lat="LATITUDE", lon="LONGITUDE", color="Appearance", size="ALTITUDE_plot",
        size_max=12, hover_name="FLIGHT_CALLSIGN_display",
        hover_data={
            "REGISTRATION": True, "LATITUDE": ":.4f", "LONGITUDE": ":.4f", "ALTITUDE": ":.0f",
            "AIRCRAFT_MODEL": True, "ORIGIN": True, "DESTINATION": True,
            FLIGHT_ID_COLUMN: True, "Appearance": True,
            "AIRLINE_NAME_display": True, "AIRLINE_COUNTRY_display": True
        },
        color_discrete_map={"Surely Passed": "#28a745", "Investigate": "#ffc107", "Not Passed": "#dc3545",
                            "new entry": "#6f42c1"},
        custom_data=[FLIGHT_ID_COLUMN],
        zoom=4.8, center={"lat": JUB_SPECIFIC_CENTER_LAT, "lon": JUB_SPECIFIC_CENTER_LON}, height=550
    )

    def update_trace_hovertemplate(trace):
        if hasattr(trace, 'hovertemplate') and trace.hovertemplate is not None:
            current_template = trace.hovertemplate
            current_template = current_template.replace(f"AIRLINE_NAME_display:", "Airline Name:")
            current_template = current_template.replace(f"AIRLINE_COUNTRY_display:", "Airline Country:")
            current_template = current_template.replace(f"{FLIGHT_ID_COLUMN}:", "Flight ID:")
            trace.hovertemplate = current_template
        else:
            pass

    fig_billing.for_each_trace(update_trace_hovertemplate)

    fig_billing.update_layout(mapbox_style=JUB_BILLING_MAP_STYLE, margin={"r": 10, "t": 10, "l": 10, "b": 10},
                              legend_title_text='Flight Status',
                              legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1,
                                          font=dict(color=map_font_color_billing)),
                              font=dict(color=map_font_color_billing),
                              uirevision=f"{str(selected_table_rows)}-{str(current_manual_points)}"
                              )
    fig_billing.update_traces(textfont=dict(color=map_font_color_billing, size=ON_MAP_TEXT_SIZE - 1),
                              marker=dict(sizemin=4))

    return fig_billing, current_manual_points, new_point_info_children if new_point_info_children else dash.no_update, cleared_lat_val, cleared_lon_val


@callback(
    Output("download-jub-selected-csv", "data"),
    Input("download-jub-selected-button", "n_clicks"),
    State("jub-billing-data-table", "selected_rows"),
    State("jub-billing-data-table", "data"),
    prevent_initial_call=True
)
def download_selected_jub_billing_csv(n_clicks, selected_row_indices, all_table_data):
    if n_clicks is None or n_clicks == 0: return dash.no_update
    if not selected_row_indices: return dash.no_update
    if not all_table_data: return dash.no_update

    try:
        selected_data = [all_table_data[i] for i in selected_row_indices if i < len(all_table_data)]
        if not selected_data: return dash.no_update

        df_selected_billing = pd.DataFrame(selected_data)
        # Rename display columns to more user-friendly names if needed for CSV
        # For example, 'LAST_UPDATE_TIME_DISPLAY' might already be handled by 'Last Seen (EAT)' in table
        # but ensure consistency or rename here if raw IDs were used.
        # The current table setup uses 'id' that might be 'LAST_UPDATE_TIME_DISPLAY', which is fine.

        timestamp = datetime.now(DISPLAY_TIMEZONE).strftime("%Y%m%d_%H%M%S")
        filename = f"jub_billing_selected_rows_{timestamp}.csv"
        return dcc.send_data_frame(df_selected_billing.to_csv, filename=filename, index=False, encoding='utf-8')
    except IndexError:
        print("Error creating JUB selected CSV: IndexError on selection.")
        return dash.no_update
    except Exception as e:
        print(f"Error creating JUB selected CSV: {e}")
        traceback.print_exc()
        return dash.no_update


# --- Airline Lookup UI Callback ---
@callback(
    Output('airline-lookup-result', 'children'),
    Input('airline-lookup-button', 'n_clicks'),
    State('airline-lookup-input', 'value'),
    prevent_initial_call=True
)
def display_airline_lookup_result(n_clicks, code_input_value):
    if not code_input_value:
        return dbc.Alert("Please enter a callsign prefix or code.", color="warning", dismissable=True)

    code_input_processed = str(code_input_value).strip().upper()

    if not iata_to_airline_map and not icao_to_airline_map:
        print("DEBUG: display_airline_lookup_result - Airline data maps are empty.")
        return dbc.Alert(f"Airline data is currently unavailable. Cannot lookup '{code_input_processed}'.",
                         color="danger", dismissable=True)

    airline_info = lookup_airline_info_by_code(code_input_processed)

    # If direct lookup fails, and input might be a partial callsign prefix, try heuristic:
    # (This part is a simple heuristic for the UI; core data processing uses get_airline_info_from_callsign)
    if not airline_info or airline_info[0] == 'N/A':
        temp_name, temp_country = ('N/A', 'N/A')
        if len(code_input_processed) >= 3:  # Try as 3-letter prefix
            prefix3 = code_input_processed[:3]
            if prefix3.isalpha():  # Common for ICAO
                info3 = lookup_airline_info_by_code(prefix3)
                if info3 and info3[0] != 'N/A':
                    temp_name, temp_country = info3

        if temp_name == 'N/A' and len(code_input_processed) >= 2:  # Try as 2-letter prefix
            prefix2 = code_input_processed[:2]
            if prefix2.isalnum():  # Common for IATA
                info2 = lookup_airline_info_by_code(prefix2)
                if info2 and info2[0] != 'N/A':
                    temp_name, temp_country = info2
        airline_info = (temp_name, temp_country)

    if airline_info and airline_info[0] != 'N/A':  # Check if name is valid
        name, country = airline_info
        country_display = f", Country: {country}" if country and country != 'N/A' else ""
        return dbc.Alert(f"Code/Prefix '{code_input_value.upper()}' → Airline: {name}{country_display}",
                         color="success", dismissable=True)
    else:
        return dbc.Alert(f"No airline found for code/prefix '{code_input_value.upper()}'.", color="danger",
                         dismissable=True)


# --- Run the App ---
if __name__ == "__main__":
    print("-" * 60)
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Initializing Flight Investigation Tool...")
    print(
        f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Live Data Logic: Latest record PER FLIGHT PER DAY for JUB Billing")
    print(
        f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] Live Data Source: API -> In-Memory Store ({API_BASE_URL})")
    try:
        app.run(port=5323, debug=True)
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Shutting down Dash server...")
    finally:
        print("Dash server stopped.")
