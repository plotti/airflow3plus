import plotly.graph_objects as go
import plotly.offline
import pandas as pd
import Airflow_Variables
import shutil

var = Airflow_Variables.AirflowVariables()

PATH = var.flask_path
CHANNELS = var.relevant_channels + ['3+: First Runs']
threeplus = var.shows_3plus
fourplus = var.shows_4plus
fiveplus = var.shows_5plus
sixplus = var.shows_6plus
tvtwentyfour = var.shows_TV24
tvtwentyfive = var.shows_TV25
sone = var.shows_S1
list_EPs = [['Der Bachelor', '3+: First Runs'], ['Die Bachelorette', '3+: First Runs'],
            ['Adieu Heimat - Schweizer wandern aus', '3+: First Runs'],
            ['Bumann, der Restauranttester', '3+: First Runs'], ['Bauer, ledig, sucht ...', '3+: First Runs']]
"""
Plotly functions to create the div.txt file of the heavy viewers table. Based on the previously computed Heavy 
Viewers Table. Saved in various places specially in drobbox for the flask app
"""
# ----------------------------------------------------------------------------------------------------------------------


def filtering_channel(df, channel):
    df = df[df['channel'] == channel]
    return df


def filtering_show(df, channel, show):
    df = df[(df['channel'] == channel) & (df['show'] == show)]
    return df


def filtering_max_values(df, channel, show, instances):

    df = df[(df['channel'] == channel) & (df['show'] == show)]

    df = df.drop(columns=['channel', 'show']).reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(columns=['Sorry, nothing to display'])

    df = df.T.nlargest(instances+1, columns=[0]).T
    return df.drop(df.columns[0], axis=1)
# ----------------------------------------------------------------------------------------------------------------------


def generate_plotly_table():

    y = 1.02
    x = 0

    annotation_english = [dict(text="<b>Choose the Example filter for a case description<br>"
                                    "and read below for a more general explenation</b><br>"
                                    "<br>"
                                    "<b>General Description:</b><br>"
                                    "This table shows the 10 shows who shared the most<br>"
                                    "viewers with the selected show. To use this table<br>"
                                    "to the full potential consider following use case:<br><br>"
                                    "By selecting a show, the table displays 10 shows who<br>"
                                    "share the most viewers with the chosen show.<br>"
                                    "Advertising for the selected show makes the most sense<br>"
                                    "in the displayed ones as in terms of speaking to<br>"
                                    "common viewers and attracting potential viewers.<br>"
                                    "The number displayed in the columns is the percentage<br>"
                                    "of viewers the selected show shares with the top show.<br>"
                                    "<br>"
                                    "Feel free to contact the Data Science Team for<br>"
                                    "suggestions and improvements of the tool.<br>"
                                    "If shows should be added that are currently not present<br>"
                                    "also contact the Data Science Team so we can add them.",
                               showarrow=False, x=x, xref='paper',
                               y=y - 1.00, yref="paper",
                               align="left", xanchor='right')]

    annotation_example = [dict(text="<b>A case example for Der Bachelor at 3+</b><br>"
                                    "<br>"
                                    "This table shows the 10 shows who share the most<br>"
                                    "viewers with the show Der Bachelor when it runs at<br>"
                                    "3+ during primetime. Let's have a look at the first<br>"
                                    "number. This number means that 67.83 percentage of<br>"
                                    "the Bachelor viewers also watched Die Bachelorette which<br>"
                                    "means that these two shows attract a lot of the same kind<br>"
                                    "of viewers. The following numbers present the same concept<br>"
                                    "or other shows against the Bachelor. You can choose now<br>"
                                    "whatever show you would like to have a look at and see<br>"
                                    "with which other shows it shares viewers with.<br>"
                                    "To summarize the number shows a percentage of viewers shared<br>"
                                    "between the show on the left side to the show writen on top<br>"
                                    "This relation can be used to determine good trailer<br>"
                                    "placement of other shows and many more things.",
                               showarrow=False, x=x+0.02, xref='paper',
                               y=y - 0.9, yref="paper",
                               align="left", xanchor='right')]

    # Load dataset and list of shows
    df = pd.read_excel(PATH + 'Heavy_Viewers/' + 'table_heavy_viewers_stealing.xlsx', header=0)
    df = df.drop(columns=['Unnamed: 0']).reset_index(drop=True)

    list_list_shows = list()
    list_list_shows.append(sorted(list_EPs))
    list_list_shows.append(sorted(threeplus))
    list_list_shows.append(sorted(fourplus))
    list_list_shows.append(sorted(fiveplus))
    list_list_shows.append(sorted(sixplus))
    list_list_shows.append(sorted(tvtwentyfour))
    list_list_shows.append(sorted(tvtwentyfive))
    list_list_shows.append(sorted(sone))

    # Create a list of buttons for each individual show
    show_list = list()
    for channel in list_list_shows:
        temp_list = list()
        temp_list.append(dict(label=str(channel[0][1]),
                              method='update',
                              args=[{'header': {'values': None},
                                     "cells": {'values': None}},
                                    {'title': '<b>Choose a show:</b>',
                                     'width': 0,
                                     'annotations': annotation_english}
                                    ]
                              )
                         )
        for shows in channel:
            chan = shows[1]
            show = shows[0]
            temp_list.append(dict(label=str(show),
                                  method='update',
                                  args=[{'header': {'values': filtering_max_values(df, chan, show, 10).columns,
                                                    'line_color': 'darkslategray'},
                                         'cells': {'values': filtering_max_values(df, chan, show, 10).T.values,
                                                   'line_color': 'darkslategray',
                                                   'height': 90}},
                                        {'title': chan + ' ' + show,
                                         'width': 1950,
                                         'annotations': annotation_english}
                                        ]
                                  )
                             )

        show_list.append(go.layout.Updatemenu(
            type='dropdown',
            buttons=list(
                temp_list
            ),
            direction='down',
            pad={"r": 10, "t": 15},
            showactive=True,
            visible=True,
            active=0,
            x=x,
            xanchor='right',
            y=y,
            yanchor='top',
        )
        )
        y -= 0.05

    show_list.append(go.layout.Updatemenu(
        type='dropdown',
        buttons=list([
            dict(label='Clear',
                 args=[{'header': {'values': None},
                        "cells": {'values': None,
                                  'height': 0}},
                       {'title': '<b>Choose a show:</b>',
                        'width': 1950,
                        'height': 900,
                        'annotations': annotation_english}
                       ],
                 method="update"
                 ),
            dict(label='Example',
                 args=[{'header': {'values': filtering_max_values(df, '3+', 'Der Bachelor', 10).columns,
                                   'line_color': 'darkslategray'},
                       'cells': {'values': filtering_max_values(df, '3+', 'Der Bachelor', 10).T.values,
                                 'line_color': 'darkslategray',
                                 'height': 90}},
                       {'title': 'Example on 3+ airing Der Bachelor',
                        'width': 1950,
                        'height': 900,
                        'annotations': annotation_example}
                       ],
                 method='update'
                 )
        ]),
        direction='down',
        pad={"r": 10, "t": 15},
        visible=True,
        x=x,
        xanchor='right',
        y=y,
        yanchor='top',
        active=1
        )
    )

    # Initialize figure
    fig = go.Figure()
    fig.add_trace(go.Table(
        name='Heavy_viewers',
        columnwidth=30,
        header=dict(
            values=None,
            line_color='darkslategray',
            align='center',
        ),
        cells=dict(
            values=None,
            line_color='darkslategray',
            align='center',
            font_size=10,
            height=60
        ),
        visible=True
        )
    )

    fig.update_layout(updatemenus=show_list, dragmode='pan', width=1950, height=900, margin=dict(l=400),
                      title_text="<b>Choose a show:</b>",
                      annotations=annotation_english
                      )

    plotly.offline.plot(fig, filename=PATH + 'Heavy_Viewers/' + 'HeavyViewersTool.html',
                        auto_open=False, auto_play=False)

    div = plotly.offline.plot(fig, show_link=False, output_type="div", include_plotlyjs=True)

    with open(PATH + 'Heavy_Viewers/hv_tool_div.txt', 'w') as f:
        f.write(div)

    shutil.copy(PATH + 'Heavy_Viewers/hv_tool_div.txt',
                f'/home/floosli/PycharmProjects/Flask_App/metric_app/static/hv_tool_div.txt')
