# Data Sync for Smart Energy Management System

This repository provides a data synchronization script that pulls real-time data from the SolarmanPV API and updates a PostgreSQL database.  
The data is used as part of my larger smart energy management system project.

## Features

- Fetches solar and energy data from the SolarmanPV API.
- Reformats the incoming data to match the rest of the db.
- Inserts the time-series data the PostgreSQL database.
- Designed to run as a Render Cron Job every 5 minutes.

## Repository Structure

- `sync_data.py`: Main Python script for data synchronization.
- `requirements.txt`: Python dependencies.

## Usage

1. **Clone the repo:**  
 > git clone https://github.com/ThatObiGuy/dataSyncSEMS.git

2. **Set required environment variables:**  
DATABASE_URL: PostgreSQL or other DB connection string.  
API_KEY: SolarmanPV API token.  
API_ENDPOINT: SolarmanPV API base URL.  
STATION_ID: Station ID to query.  

3. **Adjust fields as needed**  
Your fields won't be the same as mine
I have a site_id and solar_status that suit for my implementation.
Yours requires will differ.

5. **Run the sync script:**  
 > python sync_data.py


## Notes

- Data is floored to 5-minute intervals to prevent duplicate entries and to match the rest of the entrees in the db.

This is part of a project for personal and research use as part of a smart energy management system.  
See https://github.com/ThatObiGuy/smartEnergyManagementSystem for more information.
