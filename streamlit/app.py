import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import timedelta
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

# Weather code descriptions
weather_code_description = {
    "0": "Sunny",
    "1": "Mainly Sunny",
    "2": "Partly Cloudy",
    "3": "Cloudy",
    "45": "Foggy",
    "48": "Rime Fog",
    "51": "Light Drizzle",
    "53": "Drizzle",
    "55": "Heavy Drizzle",
    "56": "Light Freezing Drizzle",
    "57": "Freezing Drizzle",
    "61": "Light Rain",
    "63": "Rain",
    "65": "Heavy Rain",
    "66": "Light Freezing Rain",
    "67": "Freezing Rain",
    "71": "Light Snow",
    "73": "Snow",
    "75": "Heavy Snow",
    "77": "Snow Grains",
    "80": "Light Showers",
    "81": "Showers",
    "82": "Heavy Showers",
    "85": "Light Snow Showers",
    "86": "Snow Showers",
    "95": "Thunderstorm",
    "96": "Light Thunderstorms With Hail",
    "99": "Thunderstorm With Hail"
}


def write_temperature_and_humidity_insights(city_data, temp_unit):
    st.subheader("Temperature and Humidity Insights")
    # Calculate and display key insights
    max_temp = city_data['temperature'].max()
    min_temp = city_data['temperature'].min()
    max_humidity = city_data['humidity'].max()
    min_humidity = city_data['humidity'].min()

    temp_metric_col1, temp_metric_col2 = st.columns(2)
    temp_metric_col1.metric("Highest Temperature", value=f"{max_temp} {temp_unit}")
    temp_metric_col2.metric("Lowest Temperature", value=f"{min_temp} {temp_unit}")

    humidity_metric_col1, humidity_metric_col2 = st.columns(2)
    humidity_metric_col1.metric("Highest Humidity", value=f"{max_humidity} %")
    humidity_metric_col2.metric("Lowest Humidity", value=f"{min_humidity} %")


def write_temperature_and_humidity_charts(city_data, temp_unit, city, days):
    st.subheader(f"Temperature and Humidity Trends in {city} (Last {days} Day(s))")

    # Create a two-column layout for the charts
    chart_col1, chart_col2 = st.columns(2)

    # Plot temperature over time (Red Line with Red Markers)
    temp_chart = go.Figure()
    temp_chart.add_trace(
        go.Scatter(
            x=city_data['timestamp'],
            y=city_data['temperature'],
            mode='lines+markers',
            line=dict(color='red'),
            marker=dict(color='red', size=8),
            name=f'Temperature ({temp_unit})',
            hovertemplate=
            '<b>Date</b>: %{x}<br>' +
            '<b>Temperature</b>: %{y}' + f' {temp_unit}<extra></extra>'  # Custom tooltip format
        )
    )

    temp_chart.update_layout(
        title=f'Temperature ({temp_unit}) Over Time',
        xaxis_title='Date/Time',
        yaxis_title=f'Temperature ({temp_unit})',
        height=400,
        margin=dict(l=0, r=0, t=50, b=50),
    )

    # Display temperature chart in the first column
    chart_col1.plotly_chart(temp_chart, use_container_width=True)

    # Plot humidity over time (Blue Line with Blue Markers)
    humidity_chart = go.Figure()
    humidity_chart.add_trace(
        go.Scatter(
            x=city_data['timestamp'],
            y=city_data['humidity'],
            mode='lines+markers',
            line=dict(color='blue'),
            marker=dict(color='blue', size=8),
            name='Humidity (%)',
            hovertemplate=
            '<b>Date</b>: %{x}<br>' +
            '<b>Humidity</b>: %{y}%<extra></extra>'  # Custom tooltip format
        )
    )

    humidity_chart.update_layout(
        title='Humidity (%) Over Time',
        xaxis_title='Date/Time',
        yaxis_title='Humidity (%)',
        height=400,
        margin=dict(l=0, r=0, t=50, b=50),
    )

    # Display humidity chart in the second column
    chart_col2.plotly_chart(humidity_chart, use_container_width=True)


def write_map(latest_data):
    # Use the latitude and longitude of the selected city to plot a map
    st.subheader("Location on Map")

    # Create the map using Plotly
    fig = px.scatter_mapbox(
        [latest_data],
        lat='latitude',
        lon='longitude',
        hover_name='city',
        hover_data=['temperature', 'humidity'],
        zoom=10,  # Adjust zoom level
        height=400
    )

    # Set Mapbox style and token (you can use 'open-street-map' style without needing a token)
    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )

    # Display the map in Streamlit
    st.plotly_chart(fig, use_container_width=True)


def main():
    # Set the layout to wide
    st.set_page_config(
        page_title="Weather Data Dashboard",
        page_icon="🌦",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Fetch environment variable for database connection
    DATABASE_URL = os.environ.get("DATABASE_URL")

    # Create a connection to the PostgreSQL database
    engine = create_engine(DATABASE_URL)

    # Sidebar with auto-refresh interval in seconds
    refresh_interval = st.sidebar.slider("Refresh Interval (Minutes)", min_value=15, max_value=60, value=30)

    # Automatically refresh the page using st_autorefresh
    st_autorefresh(interval=refresh_interval * 1000 * 60, key="autorefresh")

    # Sidebar with temperature unit selection
    st.sidebar.header("Temperature Unit")
    unit = st.sidebar.radio("Choose temperature unit:", ("°C", "°F"), index=0)

    with st.spinner("Fetching weather data..."):
        # Fetch data from PostgreSQL
        query = """
        SELECT city, latitude, longitude, timestamp, timezone, temperature, humidity, apparent_temperature, rain, wind_direction, weather_code 
        FROM weather_data 
        ORDER BY timestamp DESC
        """
        df = pd.read_sql(query, engine)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Convert temperatures to Fahrenheit if the user selects °F
    if unit == "°F":
        df['temperature'] = df['temperature'] * 9 / 5 + 32
        df['temperature'] = df['temperature'].round(2)
        df['apparent_temperature'] = df['apparent_temperature'] * 9 / 5 + 32
        df['apparent_temperature'] = df['apparent_temperature'].round(2)

    # Streamlit app layout
    st.title("Weather Data Dashboard")

    # Sidebar with city selection dropdown
    st.sidebar.header("Filter Options")

    # City selection dropdown
    city = st.sidebar.selectbox("City:", df['city'].unique())

    # Slider for selecting the number of days worth of data
    days = st.sidebar.slider("Select number of days of data", min_value=1, max_value=10, value=1)

    # Filter data for the selected city and number of days
    city_data = df[(df['city'] == city) & (df['timestamp'] >= (pd.Timestamp.now() - timedelta(days=days)))]

    # Error handling: Show warning if no data is available
    if city_data.empty:
        st.warning(f"No data available for {city} in the last {days} day(s).")
        return

    # Display latest weather summary
    latest_data = city_data.iloc[0]
    st.subheader(f"Latest Weather for {city}")

    # Fetch the description of the weather condition from the weather_code
    description = weather_code_description.get(str(int(latest_data['weather_code'])), "Unknown Condition")

    # Use st.metric for Temperature and Humidity with appropriate symbols
    col1, col2 = st.columns(2)
    temp_unit = "°F" if unit == "°F" else "°C"
    col1.metric(label="Current Temperature", value=f"{latest_data['temperature']} {temp_unit}")
    col2.metric(label="Current Humidity", value=f"{latest_data['humidity']} %")
    st.write(f"**Temperature feels like:** {latest_data['apparent_temperature']} {temp_unit}")
    st.write(f"**Condition:** {description}")
    st.write(f"As of {latest_data['timestamp']} UTC")

    write_temperature_and_humidity_charts(city_data, temp_unit, city, days)

    write_temperature_and_humidity_insights(city_data, temp_unit)

    write_map(latest_data)


if __name__ == "__main__":
    main()
