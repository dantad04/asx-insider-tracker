# Data Directory

This directory is for storing JSON data files that will be imported into the database.

## Usage

Place your ASX director transaction JSON file here (e.g., `asx_director_transactions.json`).

To import data:
```bash
docker-compose exec backend python -m app.scripts.seed_from_json /app/data/asx_director_transactions.json
```

## JSON Format

Each record should have these fields:
- `issuer_name` - Company full name
- `director_name` - Director full name
- `date_of_change` - Trade date (YYYY-MM-DD)
- `transaction_type` - Purchase, Sale, Exercise, etc.
- `quantity` - Number of shares (negative for sales)
- `per_share` - Price per share (nullable)
- `nature_of_change` - Free text describing the change
- `symbol` - ASX ticker code
- `lodgeable` - Lodge datetime
- Other fields (optional)

## Notes

- JSON files are ignored by git to avoid committing large datasets
- The directory itself is tracked to ensure it exists in the repository
