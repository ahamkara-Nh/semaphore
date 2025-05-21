import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "./test.db") # Changed to a simpler path for sqlite3

def get_db_connection():
    """Creates a database connection and enables foreign keys."""
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row # Access columns by name
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def create_tables():
    """Creates the database tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # User Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id TEXT UNIQUE NOT NULL,
        onboarding_completed BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now'))
    );
    """)

    # User Preferences Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_preferences (
        preference_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE NOT NULL,
        allergy_nuts BOOLEAN DEFAULT FALSE NOT NULL,
        allergy_peanut BOOLEAN DEFAULT FALSE NOT NULL,
        allergy_gluten BOOLEAN DEFAULT FALSE NOT NULL,
        allergy_eggs BOOLEAN DEFAULT FALSE NOT NULL,
        allergy_fish BOOLEAN DEFAULT FALSE NOT NULL,
        allergy_soy BOOLEAN DEFAULT FALSE NOT NULL,
        daily_reminders BOOLEAN DEFAULT TRUE NOT NULL,
        update_notifications BOOLEAN DEFAULT TRUE NOT NULL,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)


    # FodmapGroup Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fodmap_group (
        fodmap_group_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT
    );
    """)

    # PhaseTracking Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS phase_tracking (
        phase_tracking_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        current_phase INTEGER NOT NULL DEFAULT 1,
        phase1_streak_days INTEGER DEFAULT 0,
        phase2_reintroduction_days INTEGER DEFAULT 0,
        phase2_break_days INTEGER DEFAULT 0,
        phase2_current_fodmap_group_id INTEGER,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (phase2_current_fodmap_group_id) REFERENCES fodmap_group(fodmap_group_id) ON DELETE SET NULL
    );
    """)

    # Triggers to update 'updated_at' timestamps
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_users_updated_at
    AFTER UPDATE ON users
    FOR EACH ROW
    BEGIN
        UPDATE users SET updated_at = datetime('now') WHERE id = OLD.id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_user_preferences_updated_at
    AFTER UPDATE ON user_preferences
    FOR EACH ROW
    BEGIN
        UPDATE user_preferences SET updated_at = datetime('now') WHERE preference_id = OLD.preference_id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_phase_tracking_updated_at
    AFTER UPDATE ON phase_tracking
    FOR EACH ROW
    BEGIN
        UPDATE phase_tracking SET updated_at = datetime('now') WHERE phase_tracking_id = OLD.phase_tracking_id;
    END;
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("Creating database and tables...")
    create_tables()
    print("Database and tables created successfully (if they didn't exist).")