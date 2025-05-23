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

    # Product Category Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product_category (
        category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        image_name TEXT
    );
    """)

    # Product Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category_id INTEGER NOT NULL,
        fructose_level INTEGER NOT NULL,
        lactose_level INTEGER NOT NULL,
        fructan_level INTEGER NOT NULL,
        mannitol_level INTEGER NOT NULL,
        sorbitol_level INTEGER NOT NULL,
        gos_level INTEGER NOT NULL,
        serving_title TEXT NOT NULL,
        serving_amount_grams REAL NOT NULL,
        contains_nuts BOOLEAN NOT NULL DEFAULT FALSE,
        contains_peanut BOOLEAN NOT NULL DEFAULT FALSE,
        contains_gluten BOOLEAN NOT NULL DEFAULT FALSE,
        contains_eggs BOOLEAN NOT NULL DEFAULT FALSE,
        contains_fish BOOLEAN NOT NULL DEFAULT FALSE,
        contains_soy BOOLEAN NOT NULL DEFAULT FALSE,
        replacement_name TEXT,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (category_id) REFERENCES product_category(category_id) ON DELETE CASCADE
    );
    """)

    # Trigger for product updated_at
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_product_updated_at
    AFTER UPDATE ON product
    FOR EACH ROW
    BEGIN
        UPDATE product SET updated_at = datetime('now') WHERE product_id = OLD.product_id;
    END;
    """)

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
        fructose_filter_level INTEGER DEFAULT 0 NOT NULL,
        lactose_filter_level INTEGER DEFAULT 0 NOT NULL,
        fructan_filter_level INTEGER DEFAULT 0 NOT NULL,
        mannitol_filter_level INTEGER DEFAULT 0 NOT NULL,
        sorbitol_filter_level INTEGER DEFAULT 0 NOT NULL,
        gos_filter_level INTEGER DEFAULT 0 NOT NULL,
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

    # UserList Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_list (
        list_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        list_type TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    # UserListItem Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_list_item (
        list_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        list_id INTEGER NOT NULL,
        food_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (list_id) REFERENCES user_list(list_id) ON DELETE CASCADE,
        FOREIGN KEY (food_id) REFERENCES product(product_id) ON DELETE CASCADE
    );
    """)

    # UserProducts Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_products (
        user_product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        fructose_level INTEGER NOT NULL,
        lactose_level INTEGER NOT NULL,
        fructan_level INTEGER NOT NULL,
        mannitol_level INTEGER NOT NULL,
        sorbitol_level INTEGER NOT NULL,
        gos_level INTEGER NOT NULL,
        serving_title TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (creator_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    # Trigger for user_products updated_at
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_user_products_updated_at
    AFTER UPDATE ON user_products
    FOR EACH ROW
    BEGIN
        UPDATE user_products SET updated_at = datetime('now') WHERE user_product_id = OLD.user_product_id;
    END;
    """)

    # Symptoms Diary Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS symptoms_diary (
        diary_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        wind_level INTEGER NOT NULL CHECK (wind_level BETWEEN 0 AND 10),
        bloat_level INTEGER NOT NULL CHECK (bloat_level BETWEEN 0 AND 10),
        pain_level INTEGER NOT NULL CHECK (pain_level BETWEEN 0 AND 10),
        stool_level INTEGER NOT NULL CHECK (stool_level BETWEEN 0 AND 10),
        notes TEXT,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now')),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """)

    # Trigger for symptoms_diary updated_at
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_symptoms_diary_updated_at
    AFTER UPDATE ON symptoms_diary
    FOR EACH ROW
    BEGIN
        UPDATE symptoms_diary SET updated_at = datetime('now') WHERE diary_id = OLD.diary_id;
    END;
    """)

    # Recipes Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS recipes (
        recipe_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        image_name TEXT,
        ingredients TEXT NOT NULL,
        preparation TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT (datetime('now')),
        updated_at TIMESTAMP DEFAULT (datetime('now'))
    );
    """)

    # Trigger for recipes updated_at
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_recipes_updated_at
    AFTER UPDATE ON recipes
    FOR EACH ROW
    BEGIN
        UPDATE recipes SET updated_at = datetime('now') WHERE recipe_id = OLD.recipe_id;
    END;
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

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_user_list_updated_at
    AFTER UPDATE ON user_list
    FOR EACH ROW
    BEGIN
        UPDATE user_list SET updated_at = datetime('now') WHERE list_id = OLD.list_id;
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_user_list_item_updated_at
    AFTER UPDATE ON user_list_item
    FOR EACH ROW
    BEGIN
        UPDATE user_list_item SET updated_at = datetime('now') WHERE list_item_id = OLD.list_item_id;
    END;
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("Creating database and tables...")
    create_tables()
    print("Database and tables created successfully (if they didn't exist).")