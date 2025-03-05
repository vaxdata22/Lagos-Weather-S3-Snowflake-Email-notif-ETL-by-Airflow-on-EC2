import requests
import pandas as pd
from datetime import datetime

def kelvin_to_celsius(temp):
    return round(temp - 273.15, 2)

def fetch_weather_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data, status code: {response.status_code}")
        return None

def transform_weather_data(data):
    weather_list = []
    
    for entry in data["list"]:
        date_of_record = entry["dt_txt"].split(" ")[0]  # Extract only the date
        time_of_record = entry["dt_txt"].split(" ")[1]  # Extract only the time
        temp_celsius = kelvin_to_celsius(entry["main"]["temp"])
        feels_like_celsius = kelvin_to_celsius(entry["main"]["feels_like"])
        min_temp_celsius = kelvin_to_celsius(entry["main"]["temp_min"])
        max_temp_celsius = kelvin_to_celsius(entry["main"]["temp_max"])
        pressure = entry["main"]["pressure"]
        sea_level_pressure = entry["main"].get("sea_level", "N/A")
        ground_level_pressure = entry["main"].get("grnd_level", "N/A")
        humidity = entry["main"]["humidity"]
        weather_description = entry["weather"][0]["description"]
        wind_speed = entry["wind"]["speed"]
        cloud_cover = entry["clouds"]["all"]
        visibility = entry.get("visibility", "N/A")
        precipitation_probability = entry.get("pop", "N/A")
        wind_gust = entry["wind"].get("gust", "N/A")
        part_of_day = "Daytime" if entry["sys"]["pod"] == "d" else "Nighttime"        

        weather_list.append({
            "Date Of Record": date_of_record,
            "Time Of Record": time_of_record,
            "Temperature (C)": temp_celsius,
            "Feels Like (C)": feels_like_celsius,
            "Minimum Temperature (C)": min_temp_celsius,
            "Maximum Temperature (C)": max_temp_celsius,
            "Pressure (hPa)": pressure,
            "Sea Level Pressure (hPa)": sea_level_pressure,
            "Ground Level Pressure (hPa)": ground_level_pressure,
            "Humidity (%)": humidity,
            "Weather Description": weather_description,
            "Wind Speed (m/s)": wind_speed,
            "Cloud Cover (%)": cloud_cover,
            "Visibility (m)": visibility,
            "Precipitation Probability (%)": precipitation_probability,
            "Wind Gust (m/s)": wind_gust,
            "Part of Day": part_of_day
        })
    
    return pd.DataFrame(weather_list)

def main():
    api_url = "https://api.openweathermap.org/data/2.5/forecast?q=lagos&appid=59250d7y8k082p9023ij683t478rnvxt"  # The endpoint URL with API key
    weather_data = fetch_weather_data(api_url)
    if weather_data:
        df = transform_weather_data(weather_data)
        df.to_csv("weather_data.csv", index=False)
        print("Weather data successfully saved to weather_data.csv")

if __name__ == "__main__":
    main()
