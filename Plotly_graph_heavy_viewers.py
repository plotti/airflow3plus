import plotly.graph_objects as go
import plotly.offline
import pandas as pd
from Airflow_Utils import Airflow_variables

var = Airflow_variables.AirflowVariables()

PATH = var.steal_pot_path
CHANNELS = var.relevant_channels
threeplus = var.shows_3plus
fourplus = var.shows_4plus
fiveplus = var.shows_5plus
sixplus = var.shows_6plus
tvtwentyfour = var.shows_TV24
tvtwentyfive = var.shows_TV25
sone = var.shows_S1


# Define filter functions
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
        return df
    df = df.T.nlargest(instances+1, columns=[0]).T
    return df.drop(df.columns[0], axis=1)


def generate_plotly_table():
    """
    Generate a plotly table based on the computed excel matrix and save the html file locally for further usage
    :return: None
    """
    # Load dataset and list of shows
    df = pd.read_excel(PATH + 'table_heavy_viewers_stealing.xlsx', header=0)
    df = df.drop(columns=['Unnamed: 0']).reset_index(drop=True)

    list_list_shows = list()
    list_list_shows.append(sorted(threeplus))
    list_list_shows.append(sorted(fourplus))
    list_list_shows.append(sorted(fiveplus))
    list_list_shows.append(sorted(sixplus))
    list_list_shows.append(sorted(tvtwentyfour))
    list_list_shows.append(sorted(tvtwentyfive))
    list_list_shows.append(sorted(sone))

    # Create a list of buttons for each individual show
    show_list = list()
    y = 1.02
    x = 0
    for channel in list_list_shows:
        temp_list = list()
        temp_list.append(dict(label=str(channel[0][1]),
                              method='update',
                              args=[{'header': {'values': None},
                                     "cells": {'values': None}},
                                    {'title': '<b>Choose a show:</b>',
                                     'width': 0}
                                    ]
                              )
                         )
        for shows in channel:
            channel = shows[1]
            show = shows[0]
            temp_list.append(dict(label=str(show),
                                  method='update',
                                  args=[{'header': {'values': filtering_max_values(df, channel, show, 10).columns,
                                                    'line_color': 'darkslategray'},
                                         'cells': {'values': filtering_max_values(df, channel, show, 10).T.values,
                                                   'line_color': 'darkslategray',
                                                   'height': 90}},
                                        {'title': channel + ' ' + show,
                                         'width': 1950}
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
            dict(label='All Shows',
                 args=[{'header': {'values': list(df.columns), 'line_color': 'darkslategray'},
                        "cells": {'values': df.T.values,
                                  'height': 60}},
                       {'title': None,
                        'width': 8000}],
                 method="update"
                 ),
            dict(label='Clear',
                 args=[{'header': {'values': None},
                        "cells": {'values': None,
                                  'height': 0}},
                       {'title': '<b>Choose a show:</b>',
                        'width': 1950}],
                 method="update"
                 )
        ]),
        direction='down',
        pad={"r": 10, "t": 15},
        visible=True,
        x=x,
        xanchor='right',
        y=y,
        yanchor='top'
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
            align='center'
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

    English = [go.layout.Annotation(text="<b>Description:</b><br>"
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
                                    y=y - 0.26, yref="paper",
                                    align="left", xanchor='right')
               ]

    Deutsch = [go.layout.Annotation(text="<b>Beschreibung:</b><br>"
                                         "Diese Tabelle zeigt die 10 Shows welche die meisten<br>"
                                         "Zuschauer mit der ausgewählten Show teilt. Um diese<br>"
                                         "Tabelle optimal zu nutzen beachte folgendes Fallbeispiel:<br>"
                                         "Wählt man eine Show aus, dann zeigt die Tabelle die<br>"
                                         "10 Shows mit denn meisten geteilten Zuschauern.<br>"
                                         "Folglich, Trailers für die ausgewählten Show macht<br>"
                                         "am meisten Sinn in the angezeigten Einträgen bezüglich<br>"
                                         "ansprechen von ähnlichen Zuschauern und anlocken von<br>"
                                         "potentiellen Zuschauern.<br>"
                                         "Die Nummer in der Zelle ist der Prozentanteil welche<br>"
                                         "die ausgewählte Show mit der Show in der Tabelle teilt.<br><br>"
                                         "Für Anmerkungen und Verbesserungsvorschlägen wenden<br>"
                                         "Sie sich bitte an das Data Science Team. Falls Shows<br>"
                                         "noch hinzugefügt werden sollen, wenden Sie sich auch<br>"
                                         "an das Data Science Team und es wird unverzüglich angepasst",
                                    showarrow=False, x=x, xref='paper',
                                    y=y - 0.26, yref="paper",
                                    align="left", xanchor='right')
               ]

    # Add the layout and some annotations
    fig.update_layout(updatemenus=show_list, dragmode='pan', width=1950, margin=dict(l=400),
                      title_text="<b>Choose a show:</b>",
                      annotations=English
                      )

    plotly.offline.plot(fig, filename=PATH + 'HeavyViewersTool.html',
                        auto_open=False, auto_play=False)
