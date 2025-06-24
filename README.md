# Data Sync for Smart Energy Management System

This repository provides a data synchronization script that pulls real-time data from the SolarmanPV API and updates a PostgreSQL database.  
The data is used as part of my larger smart energy management system project.

## Features

- Fetches solar and energy data from the SolarmanPV API.
- Inserts or updates time-series data in a PostgreSQL database.
- Can be run manually or scheduled to run automatically (see GitHub Actions workflow).

## Repository Structure

- `sync_data.py`: Main Python script for data synchronization.
- `requirements.txt`: Python dependencies.
- `.github/workflows/data-sync.yml`: GitHub Actions workflow for scheduled/automated syncs.

## Usage

1. **Clone the repo:**  
 > git clone https://github.com/your-username/dataSyncSEMS.git

2. **Set required environment variables:**  
DATABASE_URL: PostgreSQL or other DB connection string.  
API_KEY: SolarmanPV API token.  
API_ENDPOINT: SolarmanPV API base URL.  
STATION_ID: Station ID to query.  

3. **Adjust fields as needed**  
Your fields probably won't be the same as mine
I have a site_id and solar_status that many implementations won't require. Yours may require more.

4. **Run the sync script:**  
 > python sync_data.py


**Automation**  
This repository includes a GitHub Actions workflow that runs the sync every 3 minutes or can be triggered manually.


This is part of a project for personal and research use as part of a smart energy management system.  
See https://github.com/ThatObiGuy/smartEnergyManagementSystem for more information.