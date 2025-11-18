const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const app = express();
const port = 3002;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const db = new sqlite3.Database('latestchanges.db', (err) => {
    if (err) {
        console.error('Error connecting to database:', err.message);
    } else {
        console.log('Connected to the SQLite database.');
    }
});

// Helper function to execute queries
const runQuery = (query, params = []) => {
    return new Promise((resolve, reject) => {
        db.all(query, params, (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
};

// Helper function to get the latest sync_id
const getLatestSyncId = async () => {
    const rows = await runQuery('SELECT sync_id FROM Sync ORDER BY sync_date DESC LIMIT 1');
    return rows.length > 0 ? rows[0].sync_id : null;
};

// Helper function to get the closest date for a given table and column
const getClosestDate = async (table, column, targetDate, extraConditions = '', params = []) => {
    // Find the row with the closest date to targetDate
    const query = `SELECT * FROM ${table} WHERE ${column} IS NOT NULL ${extraConditions} ORDER BY ABS(strftime('%s', ${column}) - strftime('%s', ?)) ASC LIMIT 1`;
    const rows = await runQuery(query, [...params, targetDate]);
    return rows.length > 0 ? rows[0][column] : null;
};

// Helper function to format datetime to a nice readable format
const formatDateTime = (dateTimeStr) => {
    if (!dateTimeStr) return null;
    try {
        const date = new Date(dateTimeStr);
        // Format: "11/12/2025, 3:12:28 AM"
        return date.toLocaleString('en-US', {
            month: '2-digit',
            day: '2-digit',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            second: '2-digit',
            hour12: true
        });
    } catch (error) {
        return dateTimeStr;
    }
};

// LatestChanges Endpoints
app.get('/api/latestchanges', async (req, res) => {
    try {
        const { name, folder_date, data_date, sync_id, page, limit } = req.query;
        
        // Pagination parameters
        const pageNum = parseInt(page) || 1;
        const limitNum = parseInt(limit) || 50; // Default to 50 results per page
        const offset = (pageNum - 1) * limitNum;
        
        // Simple query - get ALL records from LatestChanges table
        let query = 'SELECT * FROM LatestChanges';
        let countQuery = 'SELECT COUNT(*) as total FROM LatestChanges';
        let conditions = [];
        let params = [];

        if (name) {
            conditions.push('name LIKE ?');
            params.push(`%${name}%`);
        }
        if (folder_date) {
            const closest = await getClosestDate('LatestChanges', 'folder_date', folder_date, conditions.length ? ' AND ' + conditions.join(' AND ') : '', params);
            if (closest) {
                conditions.push('folder_date = ?');
                params.push(closest);
            }
        }
        if (data_date) {
            const closest = await getClosestDate('LatestChanges', 'data_date', data_date, conditions.length ? ' AND ' + conditions.join(' AND ') : '', params);
            if (closest) {
                conditions.push('data_date = ?');
                params.push(closest);
            }
        }
        if (sync_id) {
            conditions.push('sync_id = ?');
            params.push(sync_id);
        }
        // Removed automatic filtering by latest sync_id - now returns ALL records from LatestChanges table
        
        const whereClause = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';
        
        // Get total count
        const countResult = await runQuery(countQuery + whereClause, params);
        const total = countResult[0].total;
        
        // Get paginated results
        query += whereClause;
        // Order by id DESC (most recent first) to ensure consistent ordering, handling NULL data_date values
        query += ' ORDER BY id DESC';
        query += ` LIMIT ? OFFSET ?`;
        
        const rows = await runQuery(query, [...params, limitNum, offset]);
        
        // Debug: Log what we're getting from database
        console.log(`Query returned ${rows.length} rows. Total in table: ${total}`);
        
        // Transform the data to the desired format
        const formattedRows = rows.map(row => ({
            id: row.id,
            leaderboard_name: row.leaderboard_name || null,
            name: row.name,
            oldRank: row.oldRank,
            newRank: row.newRank,
            oldScore: row.oldScore,
            newScore: row.newScore,
            time: formatDateTime(row.data_date), // Use data_date as the time, formatted nicely
            data_date: row.data_date, // Keep original for reference if needed
            sync_id: row.sync_id
        }));
        
        // Calculate pagination metadata
        const totalPages = Math.ceil(total / limitNum);
        const hasMore = pageNum < totalPages;
        
        res.json({
            data: formattedRows,
            pagination: {
                page: pageNum,
                limit: limitNum,
                total: total,
                totalPages: totalPages,
                hasMore: hasMore,
                nextPage: hasMore ? pageNum + 1 : null
            }
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Start server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});