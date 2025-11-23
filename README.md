# PGR2 Latest Changes API

A Node.js application that scrapes Project Gotham Racing 2 leaderboard data from Insignia.live, tracks changes over time, and provides a REST API to query the latest changes. The system automatically posts new leaderboard changes to Discord via webhooks.

## Features

- 🔄 **Automated Data Scraping**: Uses Puppeteer to scrape leaderboard data from Insignia.live
- 📊 **Change Detection**: Compares current leaderboard data with previous syncs to detect changes
- 🗄️ **SQLite Database**: Stores all leaderboard data and change history
- 🔌 **REST API**: Provides paginated API endpoints to query latest changes
- 💬 **Discord Integration**: Automatically posts new changes to Discord with color-coded embeds
- 🧹 **Automatic Cleanup**: Removes sync data older than 48 hours (keeps LatestChanges history)
- 🚫 **Duplicate Prevention**: Multi-layer duplicate detection to prevent duplicate entries

## Project Structure

```
pgr2latestchangesapi/
├── apilc.js              # Express.js API server
├── fetchlc.js             # Data scraping and processing script
├── latestchanges.db       # SQLite database
├── package.json           # Node.js dependencies
├── .env                   # Environment variables (create this)
├── .gitignore            # Git ignore file
└── README.md             # This file
```

## Installation

1. **Clone the repository** (or download the project files)

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Create a `.env` file** in the project root:
   ```env
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
   ```
   
   To get a Discord webhook URL:
   - Go to your Discord server settings
   - Navigate to Integrations → Webhooks
   - Create a new webhook or use an existing one
   - Copy the webhook URL

4. **Run the database initialization** (the database will be created automatically on first run)

## Usage

### Running the Data Fetcher

The `fetchlc.js` script scrapes leaderboard data and detects changes:

```bash
node fetchlc.js
```

This script will:
- Clean up sync data older than 48 hours
- Scrape all leaderboard data from Insignia.live
- Compare with previous sync to detect changes
- Insert new/changed entries into the `LatestChanges` table
- Post new changes to Discord (if webhook URL is configured)

### Running the API Server

Start the Express.js API server:

```bash
node apilc.js
```

The server will start on port **3002** by default.

## API Endpoints

### GET `/api/latestchanges`

Returns paginated latest changes from the leaderboard.

**Query Parameters:**
- `page` (optional): Page number (default: 1)
- `limit` (optional): Results per page (default: 50)
- `name` (optional): Filter by player name (partial match)
- `data_date` (optional): Filter by data date (finds closest match)
- `folder_date` (optional): Filter by folder date (finds closest match)
- `sync_id` (optional): Filter by sync ID

**Example Request:**
```bash
curl "http://localhost:3002/api/latestchanges?page=1&limit=50"
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 1,
      "leaderboard_name": "Kudos World Series - Coupe Series - Event 2 (Street Race)",
      "name": "PlayerName",
      "oldRank": 5,
      "newRank": 3,
      "oldScore": 1000,
      "newScore": 1200,
      "time": "11/16/2025, 3:12:28 AM",
      "data_date": "2025-11-16 03:12:28",
      "sync_id": "uuid-here"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 50,
    "total": 150,
    "totalPages": 3,
    "hasMore": true,
    "nextPage": 2
  }
}
```

## Database Schema

### LatestChanges Table

The main table that stores all detected changes:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key (auto-increment) |
| `leaderboard_id` | INTEGER | Leaderboard ID number |
| `leaderboard` | TEXT | Leaderboard identifier (e.g., "leaderboard_038") |
| `name` | TEXT | Player name |
| `oldRank` | INTEGER | Previous rank (0 for new entries) |
| `newRank` | INTEGER | Current rank |
| `oldScore` | INTEGER | Previous score (0 for new entries) |
| `newScore` | INTEGER | Current score |
| `folder_date` | DATETIME | Folder date timestamp |
| `data_date` | DATETIME | Data date timestamp (UTC) |
| `sync_id` | TEXT | UUID of the sync that created this entry |
| `leaderboard_name` | TEXT | Human-readable leaderboard name |
| `discord` | INTEGER | Timestamp when posted to Discord (NULL if not posted) |

### Sync Table

Tracks each data synchronization run:

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key (auto-increment) |
| `sync_id` | TEXT | Unique UUID for this sync |
| `sync_date` | DATETIME | When the sync occurred |

### Other Tables

The database also contains tables for storing raw leaderboard data:
- `XBLTotal` - Xbox Live total leaderboards
- `XBLCity` - Xbox Live city leaderboards
- `GeometryWars` - Geometry Wars leaderboards
- `KudosWorldSeries` - Kudos World Series leaderboards
- `LeaderboardChallengeKudos` - Challenge leaderboards (kudos)
- `LeaderboardChallengeTime` - Challenge leaderboards (time)
- `TimeAttack` - Time Attack leaderboards
- `Matches` - Match data

**Note**: Data in these tables is automatically cleaned up after 48 hours. Only the `LatestChanges` table retains historical data permanently.

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
```

### Developer Settings

In `fetchlc.js`, you can configure logging:

```javascript
const ENABLE_LOGGING = false; // Set to true to enable logging
const LOG_FILE = path.join(__dirname, 'fetchlc.log');
```

When logging is enabled, all operations are logged to both console and `fetchlc.log`.

### API Port

The API server port can be changed in `apilc.js`:

```javascript
const port = 3002; // Change this to your desired port
```

## How It Works

### Change Detection Logic

1. **Previous Sync Retrieval**: The script fetches data from the second-to-last sync for comparison
2. **Change Detection**: An entry is considered changed if:
   - It's a new entry (not in previous sync), OR
   - Rank AND score AND data_date have all changed since the last sync
3. **24-Hour Filter**: Only entries with `data_date` within the last 24 hours are inserted
4. **Duplicate Prevention**: Multiple layers prevent duplicates:
   - Initial duplicate check before processing
   - Final duplicate check right before insert
   - Database-level `WHERE NOT EXISTS` clause

### Leaderboard Filtering

The script automatically:
- Skips leaderboards 1-15
- Skips leaderboard ID 20
- Processes all other leaderboards based on their type (Kudos, Time Attack, etc.)

### Discord Integration

When new changes are detected:
- Each change is posted as an individual Discord message
- Messages are ordered oldest first (appears at top of Discord channel)
- Color coding:
  - **Red**: Rank 1 (first place)
  - **Green**: Top 10
  - **Orange**: Score improvement
  - **Blue**: Other changes
- The `discord` column is updated with timestamp after successful posting

## Leaderboard Types

The system handles multiple leaderboard types:

- **Kudos Leaderboards**: Track kudos (points) scores
- **Time Attack Leaderboards**: Track time-based scores
- **Kudos World Series**: Special kudos leaderboards
- **Leaderboard Challenges**: Both kudos and time variants

## Dependencies

- `express` - Web server framework
- `puppeteer` - Browser automation for scraping
- `cheerio` - HTML parsing
- `sqlite3` - SQLite database driver
- `cors` - CORS middleware
- `dotenv` - Environment variable management
- `uuid` - UUID generation
- `https` - HTTP client (built-in)

## Troubleshooting

### Database Issues

If you encounter database errors:
- Ensure the database file has write permissions
- Check that the database isn't locked by another process
- Verify the database schema matches the expected structure

### Discord Posting Issues

If Discord posts aren't working:
- Verify `DISCORD_WEBHOOK_URL` is set correctly in `.env`
- Check that the webhook URL is valid and active
- Enable logging to see error messages

### Scraping Issues

If scraping fails:
- Check your internet connection
- Verify the Insignia.live website is accessible
- Enable logging to see detailed error messages
- Ensure Puppeteer dependencies are installed (Chromium)

## License

ISC

## Contributing

Feel free to submit issues or pull requests if you find bugs or have suggestions for improvements.



