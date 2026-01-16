# EveryLot Albany Bot

A bot that posts images of every property lot in Albany, NY to Bluesky and/or Twitter.

## Features

- Fetches property data from Albany County ArcGIS Feature Service
- Supports posting to both Bluesky and Twitter
- Uses Google Street View for property images
- Maintains a local SQLite database of properties
- Can start from a specific Parcel ID

## Setup

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file:
```bash
cp .env.example .env
```

4. Configure `.env` with your Google Street View API key and social credentials.

## Initial Data Import

Fetch property data for the City of Albany:

```bash
python data_ingest.py --city "City of Albany"
```

This creates `albany_lots.db` with parcel data.

## Running the Bot

```bash
python -m everylot.bot
```

## Attribution
Based on [everylotbot](https://github.com/fitnr/everylotbot) and its forks.
