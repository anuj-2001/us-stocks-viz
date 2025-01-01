from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd

app = Dash(__name__)

# Sample data
index_performance = pd.DataFrame({"date": pd.date_range("2023-12-01", "2023-12-31"), "value": range(31)})
fig = px.line(index_performance, x="date", y="value", title="Index Performance")

app.layout = html.Div([
    dcc.Graph(figure=fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)
