import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px

# Load the CSV file
file_path = 'final.csv'  # Update path if needed
df = pd.read_csv(file_path)

# Clean the data
df.columns = df.columns.str.strip()
df['תאריך קטיף'] = pd.to_datetime(df['תאריך קטיף'], format='%d/%m/%Y')

# Handle NaN values
df['וירוס צבעcolor virus'] = df['וירוס צבעcolor virus'].fillna(0)
df['וירוס שריטותscratches virus'] = df['וירוס שריטותscratches virus'].fillna(0)

# Initialize the Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# App layout
app.layout = html.Div([
    html.H1("Dashboard"),
    html.Label("Select Sorting Type:"),
    dcc.Dropdown(
        id='sorting-type-dropdown',
        options=[{'label': s, 'value': s} for s in df['סוג מיון'].dropna().unique()],
        placeholder="Select Sorting Type"
    ),
    html.Label("Select Test Type:"),
    dcc.Dropdown(
        id='test-type-dropdown',
        options=[{'label': t, 'value': t} for t in df['סוג בדיקה'].dropna().unique()],
        placeholder="Select Test Type"
    ),
    html.Button('Filter Data', id='filter-button', n_clicks=0),
    html.Div(id='graph-selector-container', style={'display': 'none'}),
    html.Div(id='graph-container')
])

@app.callback(
    Output('graph-selector-container', 'style'),
    Output('graph-selector-container', 'children'),
    [Input('filter-button', 'n_clicks')],
    [State('sorting-type-dropdown', 'value'),
     State('test-type-dropdown', 'value')]
)
def filter_data(n_clicks, selected_sorting, selected_test):
    if n_clicks > 0 and selected_sorting and selected_test:
        filtered_df = df[(df['סוג מיון'] == selected_sorting) & (df['סוג בדיקה'] == selected_test)]       
        if not filtered_df.empty:
            return {'display': 'block'}, dcc.Dropdown(
                id='graph-selector',
                options=[
                    {'label': 'Graph 1: Interactive Line Plot', 'value': 'graph1'}
                ],
                placeholder="Select a graph"
            )
        else:
            return {'display': 'none'}, html.Div("No data available for the selected filters.")
    return {'display': 'none'}, ""

@app.callback(
    Output('graph-container', 'children'),
    [Input('graph-selector', 'value')],
    [State('sorting-type-dropdown', 'value'),
     State('test-type-dropdown', 'value')]
)
def display_graph(selected_graph, selected_sorting, selected_test):
    if selected_graph == 'graph1':
        return render_graph1(selected_sorting, selected_test)
    else:
        return html.Div()

def render_graph1(selected_sorting, selected_test):
    filtered_df = df[(df['סוג מיון'] == selected_sorting) & (df['סוג בדיקה'] == selected_test)]
    return html.Div([
        html.H1("Interactive Line Plot"),
        html.Label("Select Y-Axis Column:"),
        dcc.Dropdown(
            id='y-axis-dropdown',
            options=[{'label': col, 'value': col} for col in df.columns[8:18]],
            value='יצוא אשכולות'  # default value
        ),
        html.Label("Select Experiment:"),
        dcc.Dropdown(
            id='experiment-dropdown',
            options=[{'label': cat, 'value': cat} for cat in filtered_df['סוג בדיקה'].dropna().unique()],
            value=filtered_df['סוג בדיקה'].dropna().unique()[0] if filtered_df['סוג בדיקה'].notna().any() else None  # default value
        ),
        html.Label("Select Herb Name:"),
        dcc.Dropdown(
            id='herb-dropdown',
            options=[]  # Will be populated based on experiment selection
        ),
        html.Label("Select Parcel Size:"),
        dcc.Dropdown(
            id='parcel-dropdown',
            options=[]  # Will be populated based on herb selection
        ),
        dcc.Graph(id='scatter-plot')
    ])

@app.callback(
    [Output('herb-dropdown', 'options'),
     Output('herb-dropdown', 'value')],
    Input('experiment-dropdown', 'value'),
    [State('sorting-type-dropdown', 'value'),
     State('test-type-dropdown', 'value')]
)
def update_herb_options(selected_experiment, selected_sorting, selected_test):
    filtered_df = df[(df['סוג מיון'] == selected_sorting) & (df['סוג בדיקה'] == selected_test) & (df['סוג בדיקה'] == selected_experiment)]
    herb_options = [{'label': i, 'value': i} for i in filtered_df['זן'].unique()]
    selected_herb = herb_options[0]['value'] if herb_options else None
    return herb_options, selected_herb

@app.callback(
    [Output('parcel-dropdown', 'options'),
     Output('parcel-dropdown', 'value')],
    [Input('experiment-dropdown', 'value'),
     Input('herb-dropdown', 'value')],
    [State('sorting-type-dropdown', 'value'),
     State('test-type-dropdown', 'value')]
)
def update_parcel_options(selected_experiment, selected_herb, selected_sorting, selected_test):
    filtered_df = df[(df['סוג מיון'] == selected_sorting) &
                     (df['סוג בדיקה'] == selected_test) &
                     (df['סוג בדיקה'] == selected_experiment) &
                     (df['זן'] == selected_herb)]
    parcel_options = [{'label': i, 'value': i} for i in filtered_df['גודל חלקה במר'].unique()]
    selected_parcel = parcel_options[0]['value'] if parcel_options else None
    return parcel_options, selected_parcel

@app.callback(
    Output('scatter-plot', 'figure'),
    [Input('y-axis-dropdown', 'value'),
     Input('experiment-dropdown', 'value'),
     Input('herb-dropdown', 'value'),
     Input('parcel-dropdown', 'value')],
    [State('sorting-type-dropdown', 'value'),
     State('test-type-dropdown', 'value')]
)
def update_plot(selected_y_axis, selected_experiment, selected_herb, selected_parcel, selected_sorting, selected_test):
    df['תאריך קטיף'] = pd.to_datetime(df['תאריך קטיף'], format='%d/%m/%Y')
    filtered_df = df[(df['סוג מיון'] == selected_sorting) &
                     (df['סוג בדיקה'] == selected_test) &
                     (df['סוג בדיקה'] == selected_experiment) &
                     (df['זן'] == selected_herb) &
                     (df['גודל חלקה במר'] == selected_parcel)]

    filtered_df = filtered_df.sort_values(by='תאריך קטיף')
    filtered_df['תאריך קטיף'] = pd.to_datetime(filtered_df['תאריך קטיף'])
    filtered_df['תאריך קטיף'] = filtered_df['תאריך קטיף'].dt.date

    numeric_columns = [selected_y_axis]

    grouped = filtered_df.groupby(['חלקה', 'תאריך קטיף', 'סוג בדיקה', 'בדיקה'])[numeric_columns].mean().reset_index()
    grouped1 = grouped.groupby(['תאריך קטיף', 'סוג בדיקה', 'בדיקה'])[numeric_columns].mean().reset_index()

    if not grouped1.empty:
        fig = px.line(
            grouped1,
            x='תאריך קטיף',
            y=selected_y_axis,
            color='בדיקה',
            line_dash='בדיקה',
            title=f'Experiment: {selected_experiment}, Herb: {selected_herb}, Parcel Size: {selected_parcel}',
            hover_data={'תאריך קטיף': True, selected_y_axis: True, 'בדיקה': True}
        )
        fig.update_layout(xaxis_title='Harvest Date', yaxis_title='Harvest Weight', legend_title='בדיקה')
    else:
        fig = px.line(title="No data available for the selected filters")

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
