# Viewership Upload Pipeline

Streamlit application for uploading and processing viewership data from various platforms (YouTube, Roku, etc.) into Snowflake.

## 🚀 Quick Start

```bash
# Run app
streamlit run app.py

# Deploy
python sql/deploy/deploy.py --env prod

# Check health
./check prod

# Debug issues
./check fix
```

**See [`CHEATSHEET.md`](CHEATSHEET.md) for all commands**

## 📁 Directory Structure

```
├── app.py                  # Main Streamlit application
├── config.py               # Configuration & credentials
├── requirements.txt        # Python dependencies
│
├── src/                    # Application modules
│   ├── column_mapper.py       # Column mapping logic
│   ├── snowflake_utils.py     # Snowflake connection utilities
│   ├── transformations.py     # Data transformation engine
│   └── wide_format_handler.py # Wide format detection & conversion
│
├── lambda/                 # AWS Lambda functions
│   ├── index.js               # Lambda entry point
│   └── snowflake-helpers.js   # Snowflake operations
│
├── sql/                    # SQL deployment system
│   ├── deploy/                # Orchestrated deployment
│   ├── migrations/            # Modular SQL files
│   ├── templates/             # Stored procedures
│   └── utils/                 # Utility scripts
│
├── docs/                   # Documentation
└── scripts/                # Deployment & setup scripts
```

## 🎯 Features

- Multi-file upload with preview
- Visual column mapping
- Multi-territory template support (select multiple territories per template)
- Data transformations
- Asset matching (multiple strategies)
- Automatic validation
- Lambda-triggered post-processing

## ⚙️ Configuration

Edit `config.py` or set environment variables for Snowflake and AWS credentials.

See `sql/deploy/config.yaml` for environment-specific database names.

## 📚 Documentation

- **[CHEATSHEET.md](CHEATSHEET.md)** ⭐ **START HERE** - All commands you need
- **[DEPLOYMENT_VERIFICATION.md](DEPLOYMENT_VERIFICATION.md)** - How verification works
- **[docs/ASSET_MATCHING_ARCHITECTURE.md](docs/ASSET_MATCHING_ARCHITECTURE.md)** - Data flow & architecture
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions
- **[MULTI_TERRITORY_SUPPORT_2025_12_08.md](MULTI_TERRITORY_SUPPORT_2025_12_08.md)** - Multi-territory feature implementation (Dec 2025)

## 🛠️ Common Commands

```bash
# Deploy & verify
python sql/deploy/deploy.py --env prod
./check prod

# Debug upload
./check prod Roku file.csv

# Compare environments
./check compare

# Clean test data
python sql/utils/cleanup.py --platform Youtube --filename "test.csv"
```

## 🚢 Deployment

1. Install dependencies: `pip install -r requirements.txt`
2. Configure credentials in `config.py`
3. Deploy SQL: `python sql/deploy/deploy.py --env staging`
4. Run app: `streamlit run app.py`

For detailed instructions, see `sql/deploy/README.md`
