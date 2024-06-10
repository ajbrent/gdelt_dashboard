import boto3
import io
import pyarrow.parquet as pq
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px
import logging

logger = logging.getLogger()

app = Dash(__name__)

colors = {
    'background': '#111111',
    'text': '#67F882'
}

# Will be using boto3 here later
# df = pd.read_parquet('./test_data/day-scores.parquet')
# df = df.sort_values('day_scores', ascending=False).head(20)
# df = df.sort_values('day_scores', ascending=True)
df = None
try:
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='gdelt-data-prod', Key='day-scores.parquet')
    buffer = io.BytesIO(obj['Body'].read())
    table = pq.read_table(buffer)
    df = table.to_pandas()
    df = df.sort_values('day_scores', ascending=False).head(20)
    df = df.sort_values('day_scores', ascending=True)
except Exception as e:
    logger.error(f'Error reading parquet file from S3: {e}')
    raise e

fig = px.bar(df, y='topics', x='day_scores', title='News Topics Mentioned Today', color_discrete_sequence=['#00B020'])

fig.update_layout(
    plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'],
    font_color=colors['text'],
    height=800
)

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='News Bar Graph',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='News Discussed Today (Past 24 hrs).', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Graph(
        id='topic-bar-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)