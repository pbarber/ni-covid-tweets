import logging
import os
import datetime
import io

from selenium import webdriver
import altair

def get_chrome_driver():
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-gpu")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--disable-infobars")
    options.add_argument("--enable-logging")
    options.add_argument("--log-level=0")
    options.add_argument("--v=99")
    options.add_argument("--single-process")
    options.add_argument("--user-data-dir=/tmp/user-data/")
    options.add_argument("--data-path=/tmp/data/")
    options.add_argument("--homedir=/tmp/homedir/")
    options.add_argument("--disk-cache-dir=/tmp/disk-cache/")
    options.add_argument("--disable-async-dns")
    driver = None
    for attempt in range(3):
        try:
            driver = webdriver.Chrome(service_log_path='/tmp/chromedriver.log', options=options)
        except:
            logging.exception('Failed to setup chromium')
            with open('/tmp/chromedriver.log') as log:
                logging.warning(log.read())
            logging.error([f for f in os.listdir('/tmp/')])
        else:
            break
    else:
        logging.error('Failed to set up webdriver after %d attempts' %(attempt+1))
    return driver

def points_average_and_trend(points, line, colour, date_col, x_title, y_title, scale='linear', width=800, height=450, x_type='temporal', colour_domain=[], colour_range=[]):
    if scale=='log':
        y_title += ' (log scale)'
        line_df = line[(~line.isna()) & (line != 0)].reset_index(name='line')
    else:
        line_df = line[~line.isna()].reset_index(name='line')
    encode_point_args = {
        'x': altair.X(
            field=date_col,
            type=x_type,
            axis=altair.Axis(title=x_title),
        ),
        'y': altair.Y(
            field='points',
            type='quantitative',
            aggregate='sum',
            axis=altair.Axis(title=''),
            scale=altair.Scale(
                type=scale
            ),
        ),
    }
    encode_line_args = {
        'x': altair.X(
            field=date_col,
            type=x_type
        ),
        'y': altair.Y(
            field='line',
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type=scale
            ),
            axis=altair.Axis(title=y_title),
        ),
    }
    mark_point_args = {
        'opacity':0.7,
        'filled':True,
        'size':15,
    }
    mark_line_args = {
    }
    if colour in line_df.columns:
        if len(colour_domain) == 0:
            encode_point_args['color'] = colour
            encode_line_args['color'] = colour
        else:
            encode_point_args['color'] = altair.Color(
                field=colour,
                type='nominal',
                scale=altair.Scale(
                    domain=colour_domain,
                    range=colour_range
                )
            )
            encode_line_args['color'] = encode_point_args['color']
    else:
        mark_point_args['color'] = colour
        mark_line_args['color'] = colour
    charts = [
        altair.Chart(
            line_df
        ).mark_line(
            **mark_line_args
        ).encode(
            **encode_line_args
        ).properties(
            width=width,
            height=height
        ),
    ]
    if points is not None:
        if scale=='log':
            points_df = points[(~points.isna()) & (points != 0)].reset_index(name='points')
        else:
            points_df = points[~points.isna()].reset_index(name='points')
        charts.append(
            altair.Chart(
                points_df
            ).mark_point(
                **mark_point_args
            ).encode(
                **encode_point_args
            )
        )
    return altair.layer(*charts
    )

def plot_points_average_and_trend(configs, title, footer):
    return altair.concat(
        altair.vconcat(
            *[points_average_and_trend(
                **c
            ) for c in configs]
        ).resolve_scale(
            x='shared'
        ).properties(
            title=altair.TitleParams(
                footer,
                baseline='bottom',
                orient='bottom',
                anchor='end',
                fontWeight='normal',
                fontSize=10,
                dy=10
            ),
        )
    ).properties(
        title=altair.TitleParams(
            title,
            anchor='middle',
        )
    )

def plot_key_ni_stats_date_range(df, admissions, deaths, start_date, end_date, scale):
    return plot_points_average_and_trend(
        [
            {
                'points': df[(df['Sample_Date'] >= start_date) & (df['Sample_Date'] <= end_date)].set_index('Sample_Date')['INDIVIDUALS TESTED POSITIVE'],
                'line': df[(df['Sample_Date'] >= start_date) & (df['Sample_Date'] <= end_date)].set_index('Sample_Date')['New cases 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Sample_Date',
                'x_title': 'Specimen Date',
                'y_title': 'New cases',
                'scale': scale,
                'height': 225
            },
            {
                'points': admissions[(admissions['Admission Date'] >= start_date) & (admissions['Admission Date'] <= end_date)].set_index('Admission Date')['Number of Admissions'],
                'line': admissions[(admissions['Admission Date'] >= start_date) & (admissions['Admission Date'] <= end_date)].set_index('Admission Date')['Number of Admissions 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Admission Date',
                'x_title': 'Date of Admission',
                'y_title': 'Hospital admissions',
                'scale': scale,
                'height': 225
            },
            {
                'points': deaths[(deaths['Date of Death'] >= start_date) & (deaths['Date of Death'] <= end_date)].set_index('Date of Death')['Number of Deaths'],
                'line': deaths[(deaths['Date of Death'] >= start_date) & (deaths['Date of Death'] <= end_date)].set_index('Date of Death')['Number of Deaths 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Date of Death',
                'x_title': 'Date of Death',
                'y_title': 'Deaths within 28 days of positive test',
                'scale': scale,
                'height': 225
            },
        ],
        '%s COVID-19 %s between %s and %s' %(
            'NI',
            'cases, admissions and deaths',
            start_date.strftime('%-d %B %Y'),
            end_date.strftime('%-d %B %Y'),
        ),
        [
            'Dots show daily reports, line is 7-day rolling average',
            'Cases, admissions and deaths data from DoH daily data',
            'Last two days likely to be revised upwards due to reporting delays',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ],
    )

def plot_heatmap(df, x, x_sort, x_title, y, y_sort, y_title, color, color_title):
    return altair.Chart(df).mark_rect().encode(
        x = altair.X(
            field=x,
            type='ordinal',
            sort=altair.SortField(
                x_sort
            ),
            title=x_title
        ),
        y = altair.Y(
            field=y,
            type='ordinal',
            sort=altair.SortField(
                y_sort
            ),
            title=y_title,
        ),
        color = altair.Color(
            field=color,
            type='quantitative',
            aggregate='sum',
            title=color_title,
        )
    )


def output_plot(p, plots, driver, name):
    try:
        plot = {'name': None, 'store': io.BytesIO()}
        p.save(fp=plot['store'], format='png', method='selenium', webdriver=driver)
        plots.append(plot)
    except:
        logging.exception('Failed to output plot')
        with open('/tmp/chromedriver.log') as log:
            logging.warning(log.read())
        logging.error([f for f in os.listdir('/tmp/')])
    else:
        plots[-1]['store'].seek(0)
        plots[-1]['name'] = name
    return plots
