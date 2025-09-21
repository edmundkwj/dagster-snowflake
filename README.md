# Dagster DLT Snowflake Pipeline

A data pipeline that extracts movie data from MongoDB, transforms it, and loads it into Snowflake using Dagster and DLT (Data Loading Tool). The pipeline includes automated scheduling, partitioning, and materialization of movie analytics.

## Features

- **Data Extraction**: MongoDB data extraction using DLT
- **Data Transformation**: Movie analytics and user engagement analysis
- **Data Loading**: Snowflake warehouse integration
- **Automation**: Scheduled jobs and sensors for data pipeline execution
- **Visualization**: Movie engagement charts and reports
- **Partitioning**: Monthly partitioned data processing

## Architecture

```
MongoDB (Source) → DLT → Snowflake (Destination) → Dagster Assets (Analytics)
```

## Prerequisites

- Python 3.9-3.13
- [uv](https://docs.astral.sh/uv/) package manager
- MongoDB Atlas account with sample_mflix database
- Snowflake account with appropriate privileges
- Git

## Quick Start

### 1. Fork and Clone Repository

```bash
git clone https://github.com/edmundkwj/dagster-snowflake.git
cd dagster-snowflake
```

### 2. Environment Setup

```bash
cd dagster-dlt-snowflake
# Install uv if not already installed
brew install uv
# Create virtual environment and install dependencies
uv sync
```

### 3. Configuration

Create a `.env` file in the project root:

```bash
touch .env
```

Update `.env` with your credentials:

```env
# MongoDB Configuration
SOURCES__MONGODB__CONNECTION_URL="mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority&appName=YourApp"
# Snowflake Configuration
DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE="your_database"
DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD=${snowflake_password}
DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME=${snowflake_user}
DESTINATION__SNOWFLAKE__CREDENTIALS__HOST=${snowflake_account}
DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE="your_warehouse"
DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE="your_role"
# Snowflake Credentials
SNOWFLAKE_ACCOUNT=${snowflake_account}
SNOWFLAKE_USER=${snowflake_user}
SNOWFLAKE_PASSWORD=${snowflake_password}
```

### 4. MongoDB Setup

1. Create a MongoDB Atlas account at [mongodb.com](https://www.mongodb.com/atlas)
2. Load the sample dataset including `sample_mflix`
3. Create a database user and get connection string
4. Update the MongoDB connection URL in your `.env` file

### 5. Snowflake Setup

1. Create a Snowflake account
2. Create database, warehouse, and role:

```sql
-- Use admin access
USE role accountadmin; 
-- Create warehouse
CREATE WAREHOUSE your_warehouse;
-- Create database
CREATE DATABASE your_database;
-- Create role and user
CREATE ROLE your_role;
-- Grant permissions
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE your_role;
GRANT ROLE your_role TO USER your_username;
GRANT ALL ON DATABASE your_database TO ROLE your_role;
```

### 6. Run the Pipeline

Start Dagster development server:
```bash
uv run dagster dev
```

Navigate to `http://localhost:3000` to access the Dagster UI.

## Project Structure

```
dagster-dlt-snowflake/
├── src/dagster_dlt_snowflake/
│   ├── definitions.py          # Main Dagster definitions
│   ├── sources/
│   │   └── mongodb/           # MongoDB source configurations
│   └── defs/
│       ├── assets/            # Data assets (mongodb.py, movies.py)
│       ├── jobs/              # Dagster jobs
│       ├── schedules/         # Scheduled executions
│       ├── sensors/           # Event-driven triggers
│       └── partitions/        # Data partitioning logic
├── pyproject.toml             # Project dependencies
├── uv.lock                    # Dependency lock file
└── .env                       # Environment variables
```

## Assets

### MongoDB Assets
- `dlt_mongodb_comments`: User comments data
- `dlt_mongodb_embedded_movies`: Movie metadata and details

### Analytics Assets
- `user_engagement`: Movies ranked by user comments
- `top_movies_by_month`: Best movies by genre and month (partitioned)
- `top_movies_by_engagement`: Visualization of top 10 movies
- `debug_tables`: Database schema validation

## Usage

### Manual Execution
1. Open Dagster UI at `http://localhost:3000`
2. Navigate to "Assets" tab
3. Select assets to materialize
4. Click "Materialize selected"

### Scheduled Execution
- Movies data refreshes monthly via `movies_schedule`
- Ad-hoc processing triggered by `adhoc_sensor`

### Asset Mapping
![Dagster Asset Lineage Graph](<Pasted Graphic.png>)

### Data Output
Generated files are saved to `data/` directory:
- `movie_engagement.csv`: User engagement metrics
- `top_movies_by_month.csv`: Monthly movie rankings
- `top_movie_engagement.png`: Engagement visualization
- `tnse_visualizations.png`: ML visualization

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   - Verify connection string format
   - Check network access and IP whitelist
   - Ensure sample_mflix database is loaded

2. **Snowflake Authentication Error**
   - Verify account identifier format
   - Check user permissions and role assignments
   - Ensure warehouse is running

3. **Missing Dependencies**
   ```bash
   uv sync
   ```

4. **Environment Variables Not Loaded**
   - Ensure `.env` file is in project root
   - Restart Dagster development server with `uv run dagster dev`

### Logs and Debugging
- Check Dagster UI "Runs" tab for execution logs
- Use `debug_tables` asset to verify Snowflake schema
- Enable DLT logging for detailed pipeline information

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Resources

- [UV Documentation](https://docs.astral.sh/uv/)
- [Dagster Documentation](https://docs.dagster.io/)
- [DLT Documentation](https://dlthub.com/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)