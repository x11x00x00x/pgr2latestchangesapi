require('dotenv').config();
const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const https = require('https');
const fs = require('fs');

// Developer settings
const ENABLE_LOGGING = false; // Set to false to disable all logging
const LOG_FILE = path.join(__dirname, 'fetchlc.log');

// Logger utility function
const logger = {
    log: (...args) => {
        if (!ENABLE_LOGGING) return;
        const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : String(arg)).join(' ');
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ${message}\n`;
        console.log(...args);
        fs.appendFileSync(LOG_FILE, logMessage, 'utf8');
    },
    error: (...args) => {
        if (!ENABLE_LOGGING) return;
        const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : String(arg)).join(' ');
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ERROR: ${message}\n`;
        console.error(...args);
        fs.appendFileSync(LOG_FILE, logMessage, 'utf8');
    }
};

// Constants
const DB_PATH = path.join(__dirname, 'latestchanges.db');
const url = "https://insignia.live/games/4d53004b";
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL;

// Define leaderboard ID ranges (matching import.js)
const KUDOS_LEADERBOARDS = [
    ...Array.from({ length: 16 }, (_, i) => i + 2), // 2-17
    ...Array.from({ length: 4 }, (_, i) => i + 25), // 25-28
    ...Array.from({ length: 4 }, (_, i) => i + 33), // 33-36
    ...Array.from({ length: 60 }, (_, i) => i + 210) // 210-269
];
const KUDOS_WORLD_SERIES_LEADERBOARDS = Array.from({ length: 172 }, (_, i) => i + 38); // 38-209
const TIME_ATTACK_LEADERBOARDS = [
    ...Array.from({ length: 197 }, (_, i) => i + 271), // 271-467
    ...Array.from({ length: 36 }, (_, i) => i + 474) // 474-509
];

// Helper function to wait for table to reload
async function waitForTableToReload(page) {
    await page.waitForFunction(() => {
        const rows = document.querySelectorAll('table.table-striped tbody tr');
        return rows.length > 0;
    }, { timeout: 5000 });
}

// Helper function to sleep
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Initialize database connection
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        logger.error('Error opening database:', err);
    } else {
        logger.log('Connected to the SQLite database.');
    }
});

// Function to clean up old sync data (older than 48 hours)
function cleanupOldSyncData() {
    return new Promise((resolve, reject) => {
        try {
            // Find all sync_ids that are older than 48 hours
            const cutoffTime = new Date(Date.now() - (48 * 60 * 60 * 1000)).toISOString();
            
            db.all(`
                SELECT sync_id FROM Sync 
                WHERE datetime(sync_date) < datetime(?)
            `, [cutoffTime], (err, oldSyncs) => {
                if (err) {
                    logger.error('Error finding old syncs:', err);
                    reject(err);
                    return;
                }
                
                if (oldSyncs.length === 0) {
                    logger.log('No old sync data to clean up');
                    resolve();
                    return;
                }
                
                logger.log(`Found ${oldSyncs.length} old sync(s) to clean up`);
                
                // Delete records from all tables that use sync_id
                // NOTE: LatestChanges is NOT included - it keeps all historical records forever
                const tables = [
                    'XBLTotal',
                    'XBLCity',
                    'GeometryWars',
                    'KudosWorldSeries',
                    'LeaderboardChallengeKudos',
                    'LeaderboardChallengeTime',
                    'TimeAttack',
                    'Matches'
                ];
                
                let totalDeleteCount = 0;
                let completedOperations = 0;
                const totalOperations = oldSyncs.length * tables.length;
                
                if (totalOperations === 0) {
                    resolve();
                    return;
                }
                
                oldSyncs.forEach(sync => {
                    const syncId = sync.sync_id;
                    
                    // Delete from each table
                    tables.forEach(table => {
                        const deleteStmt = db.prepare(`DELETE FROM ${table} WHERE sync_id = ?`);
                        deleteStmt.run(syncId, function(err) {
                            if (err) {
                                logger.error(`Error deleting from ${table} for sync ${syncId}:`, err);
                            } else {
                                totalDeleteCount += this.changes;
                            }
                            completedOperations++;
                            
                            // When all table deletions are done, delete from Sync table
                            if (completedOperations === totalOperations) {
                                // Delete all old sync records from Sync table
                                const syncDeleteStmt = db.prepare('DELETE FROM Sync WHERE datetime(sync_date) < datetime(?)');
                                syncDeleteStmt.run(cutoffTime, function(err) {
                                    if (err) {
                                        logger.error('Error deleting from Sync:', err);
                                        reject(err);
                                    } else {
                                        logger.log(`Cleaned up ${oldSyncs.length} old sync(s): deleted ${totalDeleteCount} records from data tables and ${this.changes} sync records`);
                                        resolve();
                                    }
                                });
                                syncDeleteStmt.finalize();
                            }
                        });
                        deleteStmt.finalize();
                    });
                });
            });
        } catch (error) {
            logger.error('Error in cleanupOldSyncData:', error);
            reject(error);
        }
    });
}

async function fetchData() {
    // Clean up old sync data (older than 48 hours)
    await cleanupOldSyncData();
    
    // Generate a unique sync_id for this run
    const sync_id = uuidv4();
    const sync_date = new Date().toISOString();

    // Keep all historical changes - don't delete existing records
    logger.log('Keeping all historical changes - only adding new changes');

    // Insert sync record
    const syncStmt = db.prepare('INSERT INTO Sync (sync_id, sync_date) VALUES (?, ?)');
    syncStmt.run(sync_id, sync_date);
    syncStmt.finalize();

    // Get previous data for comparison - wait for it to complete
    const previousData = new Map();
    const previousStmt = db.prepare(`
        WITH current_sync AS (
            SELECT MAX(id) as max_id FROM Sync
        ),
        previous_sync AS (
            SELECT sync_id FROM Sync 
            WHERE id = (SELECT MAX(id) FROM Sync WHERE id < (SELECT max_id FROM current_sync))
        )
        SELECT leaderboard_id, name, rank, data_date,
            CASE 
                WHEN leaderboard_id = 1 THEN kudos
                WHEN leaderboard_id = 15 THEN (
                    SELECT hiscore FROM GeometryWars 
                    WHERE leaderboard_id = 15 
                    AND name = outer.name
                    AND sync_id = (SELECT sync_id FROM previous_sync)
                )
                WHEN leaderboard_id IN (SELECT value FROM json_each(?)) THEN kudos
                WHEN leaderboard_id IN (SELECT value FROM json_each(?)) THEN time
                ELSE NULL
            END as score
        FROM (
            SELECT leaderboard_id, name, rank, NULL as data_date, kudos, NULL as time
            FROM XBLTotal
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, NULL as data_date, kudos, NULL as time
            FROM XBLCity
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, NULL as data_date, NULL as kudos, NULL as time
            FROM GeometryWars
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, data_date, kudos, NULL as time
            FROM KudosWorldSeries
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, data_date, kudos, NULL as time
            FROM LeaderboardChallengeKudos
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, data_date, NULL as kudos, time
            FROM LeaderboardChallengeTime
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
            UNION ALL
            SELECT leaderboard_id, name, rank, data_date, NULL as kudos, time
            FROM TimeAttack
            WHERE sync_id = (SELECT sync_id FROM previous_sync)
        ) as outer
    `);

    const kudosLeaderboards = JSON.stringify(KUDOS_LEADERBOARDS);
    const timeAttackLeaderboards = JSON.stringify(TIME_ATTACK_LEADERBOARDS);
    
    // Wait for previous data to load completely
    await new Promise((resolve, reject) => {
        previousStmt.each([kudosLeaderboards, timeAttackLeaderboards], (err, row) => {
            if (err) {
                logger.error('Error fetching previous data:', err);
                reject(err);
                return;
            }
            previousData.set(`${row.leaderboard_id}_${row.name}`, {
                rank: row.rank,
                score: row.score,
                data_date: row.data_date
            });
        }, (err, count) => {
            if (err) {
                reject(err);
            } else {
                logger.log(`Loaded ${count} previous records for comparison`);
                resolve();
            }
        });
        previousStmt.finalize();
    });

    const browser = await puppeteer.launch({
        headless: true,
        defaultViewport: null,
        args: ['--no-sandbox']
    });
    const page = await browser.newPage();

    try {
        // Navigate to the URL
        await page.goto(url, { waitUntil: 'networkidle2' });
        logger.log('Navigated to URL successfully');

        // Scrape matches data first
        const matchesData = await page.evaluate(() => {
            const tableData = [];
            // Select only the first table
            const firstTable = document.querySelector('table.table-striped');
            if (firstTable) {
                const rows = firstTable.querySelectorAll('tbody tr');
                rows.forEach(row => {
                    const host = row.querySelector('td:first-child')?.textContent.trim() || '';
                    const players = row.querySelector('td.text-right')?.textContent.trim() || '';
                    if (host && players) {
                        tableData.push({ host, players });
                    }
                });
            }
            return tableData;
        });

        // Insert matches data into database
        if (matchesData && matchesData.length > 0) {
            const matchesStmt = db.prepare(`
                INSERT INTO Matches (
                    leaderboard_id, host, players, folder_date, data_date, sync_id
                ) VALUES (?, ?, ?, datetime('now'), datetime('now'), ?)
            `);

            matchesData.forEach(entry => {
                matchesStmt.run(
                    0, // Matches table doesn't have a specific leaderboard_id, use 0
                    entry.host,
                    entry.players,
                    sync_id
                );
            });
            matchesStmt.finalize();
            logger.log(`Inserted ${matchesData.length} matches records`);
        }

        // Get all options from the select box
        const options = await page.evaluate(() => {
            const selectElement = document.getElementById('leaderboard-select');
            return Array.from(selectElement.options).map(option => ({
                value: option.value,
                text: option.text
            }));
        });
        logger.log('Found options:', options);

        // Process each leaderboard
        for (const option of options) {
            const leaderboardId = parseInt(option.value);
            
            // Skip leaderboards 1 through 15
            if (leaderboardId >= 1 && leaderboardId <= 15) {
                logger.log(`Skipping leaderboard ${option.text} (ID: ${leaderboardId})`);
                continue;
            }
            
            // Skip leaderboard 20
            if (leaderboardId === 20) {
                logger.log(`Skipping leaderboard ${option.text} (ID: ${leaderboardId})`);
                continue;
            }
            
            logger.log(`Processing leaderboard ${option.text} (ID: ${leaderboardId})`);
            
            await page.select('#leaderboard-select', option.value);
            await sleep(1000);
            await waitForTableToReload(page);

            // Function to check if a data_date is within the last 24 hours
            // data_date is always in UTC time (format: "2025-01-21 11:46:01")
            const isWithinLast24Hours = (dataDateStr) => {
                if (!dataDateStr) return false;
                
                try {
                    // Parse the data_date string - SQLite stores UTC, so append 'Z' to ensure UTC parsing
                    const utcDateStr = dataDateStr.includes('Z') || dataDateStr.includes('+') || dataDateStr.includes('-', 10) 
                        ? dataDateStr 
                        : dataDateStr.replace(' ', 'T') + 'Z';
                    const dataDate = new Date(utcDateStr);
                    
                    // Get current time in UTC
                    const nowUTC = new Date();
                    const twentyFourHoursAgo = new Date(nowUTC.getTime() - (24 * 60 * 60 * 1000));
                    
                    // Check if data_date is after 24 hours ago and not in the future (all in UTC)
                    return dataDate >= twentyFourHoursAgo && dataDate <= nowUTC;
                } catch (error) {
                    logger.error('Error parsing data_date:', dataDateStr, error);
                    return false;
                }
            };

            // Function to check if a duplicate record already exists in LatestChanges
            const checkDuplicate = (leaderboardId, leaderboard, name, newScore, dataDate) => {
                return new Promise((resolve) => {
                    try {
                        // Normalize the score to string for comparison (handles both numeric and time strings)
                        const normalizedScore = newScore !== null && newScore !== undefined ? String(newScore) : '';
                        
                        // Normalize data_date (trim whitespace)
                        const normalizedDataDate = dataDate ? String(dataDate).trim() : '';
                        
                        if (!normalizedDataDate) {
                            // If no data_date, can't check for duplicate properly
                            logger.log(`Duplicate check skipped: no data_date for ${name}`);
                            resolve(false);
                            return;
                        }
                        
                        // Use db.get() with callback to ensure proper execution
                        db.get(`
                            SELECT id FROM LatestChanges 
                            WHERE leaderboard_id = ? 
                            AND leaderboard = ? 
                            AND name = ? 
                            AND TRIM(CAST(newScore AS TEXT)) = ?
                            AND TRIM(data_date) = ?
                            LIMIT 1
                        `, [leaderboardId, leaderboard, name, normalizedScore, normalizedDataDate], (err, result) => {
                            if (err) {
                                logger.error('Error checking for duplicate:', err);
                                logger.error(`  Parameters: leaderboard_id=${leaderboardId}, leaderboard=${leaderboard}, name=${name}, newScore=${normalizedScore}, dataDate=${normalizedDataDate}`);
                                resolve(false); // If check fails, allow insertion (safer to allow than block)
                                return;
                            }
                            
                            // result is undefined when no row is found, or an object with the selected columns when found
                            const isDup = result && typeof result === 'object' && result.id !== undefined && result.id !== null;
                            if (isDup) {
                                logger.log(`Duplicate found: leaderboard_id=${leaderboardId}, leaderboard=${leaderboard}, name=${name}, score=${normalizedScore}, date=${normalizedDataDate}, existing_id=${result.id}`);
                            } else {
                                logger.log(`No duplicate found: leaderboard_id=${leaderboardId}, leaderboard=${leaderboard}, name=${name}, score=${normalizedScore}, date=${normalizedDataDate}`);
                            }
                            resolve(isDup);
                        });
                    } catch (error) {
                        logger.error('Error in checkDuplicate:', error);
                        logger.error(`  Parameters: leaderboard_id=${leaderboardId}, leaderboard=${leaderboard}, name=${name}, newScore=${newScore}, dataDate=${dataDate}`);
                        resolve(false); // If check fails, allow insertion (safer to allow than block)
                    }
                });
            };

            // Function to detect changes and insert into LatestChanges
            const detectAndInsertChanges = async (entry, leaderboardName, actualLeaderboardName) => {
                const key = `${leaderboardId}_${entry.name}`;
                const previous = previousData.get(key);
                
                // Get current score - handle both numeric (kudos/hiscore) and string (time) values
                const currentScore = entry.kudos !== undefined ? entry.kudos : 
                                    entry.hiscore !== undefined ? entry.hiscore : 
                                    entry.time !== undefined ? entry.time : null;
                
                // Get current data_date (entry.date is the field name from scraping)
                // Handle empty strings as null
                const currentDataDate = (entry.date && entry.date.trim() !== '') ? entry.date : null;
                
                // Check if data_date is within the last 24 hours - if not, skip this entry
                // Also skip if data_date is null or empty
                if (!currentDataDate || !isWithinLast24Hours(currentDataDate)) {
                    if (!currentDataDate) {
                        logger.log(`Skipping entry ${entry.name} - no data_date`);
                    } else {
                        logger.log(`Skipping entry ${entry.name} - data_date ${currentDataDate} is older than 24 hours`);
                    }
                    return false; // Skip entries with data_date older than 24 hours or missing
                }
                
                // Normalize score for consistent comparison and insertion
                // Convert to string to handle both numeric and time-based scores consistently
                const scoreToInsert = currentScore !== null && currentScore !== undefined ? String(currentScore) : '0';
                
                // Check for duplicate before inserting
                logger.log(`Checking for duplicate: leaderboard_id=${leaderboardId}, leaderboard=${leaderboardName}, name=${entry.name}, score=${scoreToInsert}, date=${currentDataDate}`);
                const isDuplicate = await checkDuplicate(leaderboardId, leaderboardName, entry.name, scoreToInsert, currentDataDate);
                logger.log(`Duplicate check result: ${isDuplicate}`);
                if (isDuplicate) {
                    logger.log(`Skipping duplicate: ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                    return false; // Duplicate exists, skip insertion
                }
                
                // If no previous entry, this is a new entry - record it
                // Double-check data_date is not null before inserting
                if (!previous && currentDataDate) {
                    // Final duplicate check right before insert to prevent race conditions
                    const finalDuplicateCheck = await checkDuplicate(leaderboardId, leaderboardName, entry.name, scoreToInsert, currentDataDate);
                    if (finalDuplicateCheck) {
                        logger.log(`Final duplicate check: Skipping duplicate insert for ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                        return false;
                    }
                    
                    logger.log(`Inserting new entry: ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                    // Use INSERT with WHERE NOT EXISTS to prevent duplicates at database level
                    const changesStmt = db.prepare(`
                        INSERT INTO LatestChanges (
                            leaderboard_id, leaderboard, name, oldRank, newRank, 
                            oldScore, newScore, folder_date, data_date, sync_id, leaderboard_name, discord
                        )
                        SELECT ?, ?, ?, 0, ?, 0, ?, datetime('now'), ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM LatestChanges
                            WHERE leaderboard_id = ?
                            AND leaderboard = ?
                            AND name = ?
                            AND TRIM(CAST(newScore AS TEXT)) = ?
                            AND TRIM(data_date) = ?
                        )
                    `);
                    changesStmt.run(
                        leaderboardId,
                        leaderboardName,
                        entry.name,
                        entry.rank,
                        scoreToInsert, // Use normalized string score
                        currentDataDate,
                        sync_id,
                        actualLeaderboardName, // Use the actual leaderboard name from option.text
                        null, // discord column - will be updated after posting to Discord
                        // Duplicate check parameters
                        leaderboardId,
                        leaderboardName,
                        entry.name,
                        scoreToInsert,
                        currentDataDate
                    );
                    const rowsAffected = changesStmt.changes;
                    changesStmt.finalize();
                    if (!rowsAffected || rowsAffected === 0) {
                        logger.log(`Insert was skipped (duplicate detected at database level): ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                        return false;
                    }
                    return true; // Indicate change detected
                }
                
                // Compare previous and current values
                // Only record if rank AND score AND data_date all changed (if any are same, don't record)
                const rankChanged = previous.rank !== entry.rank;
                const scoreChanged = String(previous.score || '') !== String(currentScore || '');
                const dataDateChanged = String(previous.data_date || '') !== String(currentDataDate || '');
                
                if (rankChanged && scoreChanged && dataDateChanged && currentDataDate) {
                    // Changed entry - record the change
                    // Double-check data_date is not null before inserting
                    // Final duplicate check right before insert to prevent race conditions
                    const finalDuplicateCheck = await checkDuplicate(leaderboardId, leaderboardName, entry.name, scoreToInsert, currentDataDate);
                    if (finalDuplicateCheck) {
                        logger.log(`Final duplicate check: Skipping duplicate insert for ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                        return false;
                    }
                    
                    logger.log(`Inserting changed entry: ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                    // Use INSERT with WHERE NOT EXISTS to prevent duplicates at database level
                    const changesStmt = db.prepare(`
                        INSERT INTO LatestChanges (
                            leaderboard_id, leaderboard, name, oldRank, newRank, 
                            oldScore, newScore, folder_date, data_date, sync_id, leaderboard_name, discord
                        )
                        SELECT ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM LatestChanges
                            WHERE leaderboard_id = ?
                            AND leaderboard = ?
                            AND name = ?
                            AND TRIM(CAST(newScore AS TEXT)) = ?
                            AND TRIM(data_date) = ?
                        )
                    `);
                    const result = changesStmt.run(
                        leaderboardId,
                        leaderboardName,
                        entry.name,
                        previous.rank,
                        entry.rank,
                        previous.score !== null ? String(previous.score) : '0', // Normalize previous score
                        scoreToInsert, // Use normalized string score
                        currentDataDate,
                        sync_id,
                        actualLeaderboardName, // Use the actual leaderboard name from option.text
                        null, // discord column - will be updated after posting to Discord
                        // Duplicate check parameters
                        leaderboardId,
                        leaderboardName,
                        entry.name,
                        scoreToInsert,
                        currentDataDate
                    );
                    const rowsAffected = result ? result.changes : changesStmt.changes;
                    changesStmt.finalize();
                    if (!rowsAffected || rowsAffected === 0) {
                        logger.log(`Insert was skipped (duplicate detected at database level): ${leaderboardId}, ${entry.name}, score: ${scoreToInsert}, date: ${currentDataDate}`);
                        return false;
                    }
                    return true; // Indicate change detected
                }
                
                // No changes - rank, score, and data_date did not all change
                return false;
            };

            if (leaderboardId === 1) {
                // Handle XBLTotal table
                const leaderboardData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 8) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                first_place_finishes: parseInt(cells[2].textContent.trim()) || 0,
                                second_place_finishes: parseInt(cells[3].textContent.trim()) || 0,
                                third_place_finishes: parseInt(cells[4].textContent.trim()) || 0,
                                races_completed: parseInt(cells[5].textContent.trim()) || 0,
                                kudos_rank: parseInt(cells[6].textContent.trim()) || 0,
                                kudos: parseInt(cells[7].textContent.trim()) || 0
                        };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (leaderboardData && leaderboardData.length > 0) {
                const stmt = db.prepare(`
                    INSERT INTO XBLTotal (
                        leaderboard_id, rank, name, first_place_finishes, 
                        second_place_finishes, third_place_finishes, races_completed,
                            kudos_rank, kudos, folder_date, data_date, sync_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?)
                `);

                    let hasChanges = false;
                    for (const entry of leaderboardData) {
                        if (await detectAndInsertChanges(entry, 'leaderboard_001', option.text)) {
                            hasChanges = true;
                            stmt.run(
                                1,
                                entry.rank,
                                entry.name,
                                entry.first_place_finishes,
                                entry.second_place_finishes,
                                entry.third_place_finishes,
                                entry.races_completed,
                                entry.kudos_rank,
                                entry.kudos,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                            } else {
                        stmt.finalize();
                        logger.log('No changes detected for XBLTotal');
                    }
                }
            } else if (leaderboardId >= 2 && leaderboardId <= 14) {
                // Temporarily skip XBLCity and XBLCityRaces
                logger.log(`Skipping XBLCity and XBLCityRaces for leaderboard ${leaderboardId}`);
            } else if (leaderboardId === 15) {
                // Handle GeometryWars table
                const geometryWarsData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 3) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                hiscore: parseInt(cells[2].textContent.trim()) || 0
                            };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (geometryWarsData && geometryWarsData.length > 0) {
                    const stmt = db.prepare(`
                        INSERT INTO GeometryWars (
                            leaderboard_id, rank, name, hiscore, folder_date, data_date, sync_id
                        ) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), ?)
                    `);

                    let hasChanges = false;
                    for (const entry of geometryWarsData) {
                        if (await detectAndInsertChanges(entry, 'leaderboard_015', option.text)) {
                            hasChanges = true;
                            stmt.run(
                                leaderboardId,
                                entry.rank,
                                entry.name,
                                entry.hiscore,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                    } else {
                        stmt.finalize();
                        logger.log('No changes detected for GeometryWars');
                    }
                }
            } else if (KUDOS_WORLD_SERIES_LEADERBOARDS.includes(leaderboardId)) {
                // Handle KudosWorldSeries table
                const kudosData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 7) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                date: cells[2].textContent.trim() || '',
                                location: cells[3].textContent.trim() || '',
                                circuit: cells[4].textContent.trim() || '',
                                car: cells[5].textContent.trim() || '',
                                kudos: parseInt(cells[6].textContent.trim()) || 0
                            };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (kudosData && kudosData.length > 0) {
                    const stmt = db.prepare(`
                        INSERT INTO KudosWorldSeries (
                            leaderboard_id, rank, name, data_date, location, circuit, car, kudos, 
                            folder_date, sync_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    `);

                    let hasChanges = false;
                    for (const entry of kudosData) {
                        if (await detectAndInsertChanges(entry, `leaderboard_${String(leaderboardId).padStart(3, '0')}`, option.text)) {
                            hasChanges = true;
                            stmt.run(
                                leaderboardId,
                                entry.rank,
                                entry.name,
                                entry.date,
                                entry.location,
                                entry.circuit,
                                entry.car,
                                entry.kudos,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                    } else {
                        stmt.finalize();
                        logger.log(`No changes detected for KudosWorldSeries ${leaderboardId}`);
                    }
                }
            } else if (TIME_ATTACK_LEADERBOARDS.includes(leaderboardId)) {
                // Handle TimeAttack table
                const timeData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 7) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                date: cells[2].textContent.trim() || '',
                                location: cells[3].textContent.trim() || '',
                                circuit: cells[4].textContent.trim() || '',
                                car: cells[5].textContent.trim() || '',
                                time: cells[6].textContent.trim() || ''
                            };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (timeData && timeData.length > 0) {
                    const stmt = db.prepare(`
                        INSERT INTO TimeAttack (
                            leaderboard_id, rank, name, data_date, location, circuit, car, time, 
                            folder_date, sync_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    `);

                    let hasChanges = false;
                    for (const entry of timeData) {
                        if (await detectAndInsertChanges(entry, `leaderboard_${String(leaderboardId).padStart(3, '0')}`, option.text)) {
                            hasChanges = true;
                            stmt.run(
                                leaderboardId,
                                entry.rank,
                                entry.name,
                                entry.date,
                                entry.location,
                                entry.circuit,
                                entry.car,
                                entry.time,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                    } else {
                        stmt.finalize();
                        logger.log(`No changes detected for TimeAttack ${leaderboardId}`);
                    }
                }
            } else if (KUDOS_LEADERBOARDS.includes(leaderboardId)) {
                // Handle LeaderboardChallengeKudos table
                const kudosData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 7) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                date: cells[2].textContent.trim() || '',
                                location: cells[3].textContent.trim() || '',
                                circuit: cells[4].textContent.trim() || '',
                                car: cells[5].textContent.trim() || '',
                                kudos: parseInt(cells[6].textContent.trim()) || 0
                            };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (kudosData && kudosData.length > 0) {
                    const stmt = db.prepare(`
                        INSERT INTO LeaderboardChallengeKudos (
                            leaderboard_id, rank, name, data_date, location, circuit, car, kudos, 
                            folder_date, sync_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    `);

                    let hasChanges = false;
                    for (const entry of kudosData) {
                        if (await detectAndInsertChanges(entry, `leaderboard_${String(leaderboardId).padStart(3, '0')}`, option.text)) {
                            hasChanges = true;
                            stmt.run(
                                leaderboardId,
                                entry.rank,
                                entry.name,
                                entry.date,
                                entry.location,
                                entry.circuit,
                                entry.car,
                                entry.kudos,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                    } else {
                        stmt.finalize();
                        logger.log(`No changes detected for LeaderboardChallengeKudos ${leaderboardId}`);
                    }
                }
            } else {
                // Handle LeaderboardChallengeTime table
                const timeData = await page.evaluate(() => {
                    const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
                    if (!rows || rows.length === 0) return [];
                    
                    return rows.map(row => {
                        const cells = row.querySelectorAll('td');
                        if (!cells || cells.length < 7) return null;
                        
                        try {
                            return {
                                rank: parseInt(cells[0].textContent.trim()) || 0,
                                name: cells[1].textContent.trim() || '',
                                date: cells[2].textContent.trim() || '',
                                location: cells[3].textContent.trim() || '',
                                circuit: cells[4].textContent.trim() || '',
                                car: cells[5].textContent.trim() || '',
                                time: cells[6].textContent.trim() || ''
                            };
                        } catch (error) {
                            logger.error('Error parsing row:', error);
                            return null;
                        }
                    }).filter(item => item !== null);
                });

                if (timeData && timeData.length > 0) {
                    const stmt = db.prepare(`
                        INSERT INTO LeaderboardChallengeTime (
                            leaderboard_id, rank, name, data_date, location, circuit, car, time, 
                            folder_date, sync_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    `);

                    let hasChanges = false;
                    for (const entry of timeData) {
                        if (await detectAndInsertChanges(entry, `leaderboard_${String(leaderboardId).padStart(3, '0')}`, option.text)) {
                            hasChanges = true;
                            stmt.run(
                                leaderboardId,
                                entry.rank,
                                entry.name,
                                entry.date,
                                entry.location,
                                entry.circuit,
                                entry.car,
                                entry.time,
                                sync_id
                            );
                        }
                    }
                    if (hasChanges) {
                        stmt.finalize();
                    } else {
                        stmt.finalize();
                        logger.log(`No changes detected for LeaderboardChallengeTime ${leaderboardId}`);
                    }
                }
            }
        }

        await browser.close();
        
        // Post new changes to Discord and update discord column
        await postChangesToDiscord(sync_id);
        
    } catch (error) {
        logger.error('Error:', error);
        await browser.close();
    }
}

// Function to post latest changes to Discord and update discord column
async function postChangesToDiscord(sync_id) {
    if (!DISCORD_WEBHOOK_URL) {
        logger.log('Discord webhook URL not set - skipping Discord posting');
        return;
    }
    
    return new Promise((resolve, reject) => {
        // Get all changes from this sync that haven't been posted to Discord yet
        // Order by ASC so oldest appears at top of Discord channel
        db.all(`
            SELECT id, leaderboard_name, name, oldRank, newRank, oldScore, newScore, data_date
            FROM LatestChanges
            WHERE sync_id = ? AND discord IS NULL
            ORDER BY data_date ASC
        `, [sync_id], async (err, changes) => {
            if (err) {
                logger.error('Error fetching changes for Discord:', err);
                reject(err);
                return;
            }
            
            if (changes.length === 0) {
                logger.log('No new changes to post to Discord');
                resolve();
                return;
            }
            
            logger.log(`Posting ${changes.length} change(s) to Discord...`);
            
            try {
                // Format changes for Discord
                const embeds = changes.map(change => {
                    const formatScore = (score) => {
                        if (score === 0 || score === null || score === '0') return 'N/A';
                        return String(score);
                    };
                    
                    const formatDate = (dateStr) => {
                        if (!dateStr) return 'N/A';
                        try {
                            const date = new Date(dateStr);
                            // Format in UTC to match Discord's timestamp display
                            return date.toLocaleString('en-US', {
                                month: '2-digit',
                                day: '2-digit',
                                year: 'numeric',
                                hour: 'numeric',
                                minute: '2-digit',
                                second: '2-digit',
                                hour12: true,
                                timeZone: 'UTC'
                            });
                        } catch {
                            return dateStr;
                        }
                    };
                    
                    // Check if this is a new entry (oldRank and oldScore are both 0)
                    const isNewEntry = (change.oldRank === 0 || change.oldRank === null) && 
                                      (change.oldScore === 0 || change.oldScore === null || change.oldScore === '0');
                    
                    // Check if it's rank 1 (first place)
                    const isRank1 = change.newRank !== null && change.newRank === 1;
                    
                    // Check if it's top 10
                    const isTop10 = change.newRank !== null && change.newRank <= 10;
                    
                    // Check if there's a score improvement (has old score and new score, and they're different)
                    const hasImprovement = !isNewEntry && 
                                          change.oldScore !== null && change.oldScore !== '0' && change.oldScore !== 0 &&
                                          change.newScore !== null && change.newScore !== '0' && change.newScore !== 0 &&
                                          String(change.oldScore) !== String(change.newScore);
                    
                    // Determine color: Rank 1 = Red, Top 10 = Green, Improvement = Orange, Default = Blue
                    let embedColor = 0x3498db; // Default blue
                    if (isRank1) {
                        embedColor = 0xff0000; // Red for rank 1
                    } else if (isTop10) {
                        embedColor = 0x00ff00; // Green for top 10
                    } else if (hasImprovement) {
                        embedColor = 0xffa500; // Orange for improvement
                    }
                    
                    // Build description text
                    let description = '';
                    if (isNewEntry) {
                        description = `**${change.name || 'N/A'}** has a new score!\n`;
                    } else if (hasImprovement) {
                        description = `**${change.name || 'N/A'}** improved their score!\n`;
                    }
                    
                    // Build the main content with uniform formatting
                    const rankText = isNewEntry 
                        ? `**New Rank:** ${change.newRank}`
                        : `**Rank:** ${change.oldRank} → ${change.newRank}`;
                    
                    const scoreText = isNewEntry
                        ? `**New Score:** ${formatScore(change.newScore)}`
                        : `**Score:** ${formatScore(change.oldScore)} → ${formatScore(change.newScore)}`;
                    
                    return {
                        title: change.leaderboard_name || 'Leaderboard Change',
                        description: description || undefined,
                        color: embedColor,
                        fields: [
                            {
                                name: 'Player',
                                value: change.name || 'N/A',
                                inline: false
                            },
                            {
                                name: rankText,
                                value: scoreText,
                                inline: false
                            },
                            {
                                name: 'Time',
                                value: formatDate(change.data_date),
                                inline: false
                            }
                        ],
                        timestamp: change.data_date || new Date().toISOString()
                    };
                });
                
                // Post each change as an individual Discord message
                // Update discord column immediately after each successful post
                const postedTime = new Date().toISOString();
                let successCount = 0;
                let failCount = 0;
                
                for (let i = 0; i < embeds.length; i++) {
                    const embed = embeds[i];
                    const changeId = changes[i].id;
                    const payload = {
                        embeds: [embed] // Single embed per message
                    };
                    
                    try {
                        await new Promise((resolve, reject) => {
                            const url = new URL(DISCORD_WEBHOOK_URL);
                            const postData = JSON.stringify(payload);
                            
                            const options = {
                                hostname: url.hostname,
                                port: 443,
                                path: url.pathname + url.search,
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Content-Length': Buffer.byteLength(postData)
                                }
                            };
                            
                            const req = https.request(options, (res) => {
                                let data = '';
                                
                                res.on('data', (chunk) => {
                                    data += chunk;
                                });
                                
                                res.on('end', () => {
                                    if (res.statusCode >= 200 && res.statusCode < 300) {
                                        resolve();
                                    } else {
                                        reject(new Error(`Discord API error: ${res.statusCode} - ${data}`));
                                    }
                                });
                            });
                            
                            req.on('error', (error) => {
                                reject(error);
                            });
                            
                            req.write(postData);
                            req.end();
                        });
                        
                        // Only update discord column if post was successful
                        await new Promise((resolve, reject) => {
                            const updateStmt = db.prepare('UPDATE LatestChanges SET discord = ? WHERE id = ? AND discord IS NULL');
                            updateStmt.run(postedTime, changeId, function(err) {
                                if (err) {
                                    logger.error(`Error updating discord column for change ${changeId}:`, err);
                                    reject(err);
                                } else {
                                    successCount++;
                                    updateStmt.finalize();
                                    resolve();
                                }
                            });
                        });
                        
                    } catch (error) {
                        logger.error(`Failed to post change ${changeId} to Discord:`, error);
                        failCount++;
                        // Continue with next message even if this one failed
                    }
                    
                    // Small delay between messages to avoid rate limiting
                    if (i < embeds.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, 250)); // 250ms delay between messages
                    }
                }
                
                logger.log(`Discord posting complete: ${successCount} successful, ${failCount} failed`);
                resolve();
                
            } catch (error) {
                logger.error('Error posting to Discord:', error);
                reject(error);
            }
        });
    });
}

// Main function to run everything
async function runAll() {
    try {
        await fetchData();
        logger.log('Data collection completed successfully');
    } catch (err) {
        logger.error('Error in main process:', err);
    } finally {
        db.close((err) => {
            if (err) {
                logger.error('Error closing database:', err);
            } else {
                logger.log('Database connection closed');
            }
        });
    }
}

// Run the script
runAll();