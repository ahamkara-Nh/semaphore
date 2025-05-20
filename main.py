import os
import hmac
import hashlib
import json
import logging
import sqlite3
from urllib.parse import unquote, parse_qs
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, status
from typing import Optional, Dict, Any
from database import get_db_connection, create_tables # Updated import
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables() # Use new function from database.py
    yield
    # No specific cleanup needed for sqlite3 connections here as they are managed per request

app = FastAPI(lifespan=lifespan)

BOT_TOKEN = os.getenv("BOT_TOKEN")

class UserPreferencesUpdate(BaseModel):
    allergy_nuts: Optional[bool] = None
    allergy_peanut: Optional[bool] = None
    allergy_gluten: Optional[bool] = None
    allergy_eggs: Optional[bool] = None
    allergy_fish: Optional[bool] = None
    allergy_soy: Optional[bool] = None
    # daily_reminders and update_notifications are not updated via this endpoint directly

class TelegramInitData(BaseModel):
    initData: str

def validate_telegram_data(init_data_str: str, bot_token: str) -> bool:
    try:
        try:
            parsed_data = parse_qs(init_data_str)
        except Exception:
            parsed_data = parse_qs(unquote(init_data_str))

        if 'hash' not in parsed_data or not parsed_data['hash']:
            return False

        hash_received = parsed_data['hash'][0]
        data_check_string_parts = []

        for key, value in sorted(parsed_data.items()):
            if key != 'hash':
                data_check_string_parts.append(f"{key}={value[0]}")

        data_check_string = "\n".join(data_check_string_parts)

        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        return calculated_hash == hash_received
    except Exception as e:
        logger.error(f"Error validating Telegram data: {e}")
        return False

# Helper to convert sqlite3.Row to dict
def row_to_dict(row: sqlite3.Row) -> Optional[Dict[str, Any]]:
    if row:
        return dict(row)
    return None

@app.get("/")
async def root():
    return {"message": "Hello World - Backend is running!"}

@app.post("/auth/telegram")
async def auth_telegram(payload: TelegramInitData):
    logger.debug(f"Received payload: {payload}")
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not configured on the server.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="BOT_TOKEN is not configured on the server."
        )

    is_valid = validate_telegram_data(payload.initData, BOT_TOKEN)

    if not is_valid:
        logger.warning(f"Invalid Telegram initialization data received: {payload.initData[:100]}...") # Log part of invalid data for debugging
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Telegram initialization data."
        )
    
    conn = None
    try:
        init_data_params = parse_qs(unquote(payload.initData))
        user_info_json = init_data_params.get('user', [None])[0]
        if not user_info_json:
            logger.error("User data not found in Telegram initialization data.")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User data not found in Telegram initialization data."
            )
        user_info = json.loads(user_info_json)
        telegram_id = str(user_info.get("id"))

        if not telegram_id:
            logger.error("Telegram user ID not found in user data.")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Telegram user ID not found in user data."
            )
        logger.info(f"Attempting authentication for telegram_id: {telegram_id}")
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        db_user_row = cursor.fetchone()

        if not db_user_row:
            logger.info(f"New user. Attempting to insert user with telegram_id {telegram_id}.")
            cursor.execute(
                "INSERT INTO users (telegram_id) VALUES (?)",
                (telegram_id,)
            )
            logger.info(f"Executed insert for new user with telegram_id {telegram_id}.")
            
            cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
            new_user_data = cursor.fetchone()
            if not new_user_data:
                conn.rollback()
                logger.error(f"Failed to retrieve user with telegram_id {telegram_id} immediately after insertion.")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to create and retrieve new user."
                )
            user_id = new_user_data['id']
            logger.info(f"Retrieved new user ID: {user_id} for telegram_id {telegram_id}.")

            logger.info(f"Attempting to insert default preferences for user_id {user_id}.")
            cursor.execute(
                """INSERT INTO user_preferences (
                       user_id, allergy_nuts, allergy_peanut, allergy_gluten,
                       allergy_eggs, allergy_fish, allergy_soy,
                       daily_reminders, update_notifications
                   ) VALUES (?, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE)""",
                (user_id,)
            )
            logger.info(f"Default preferences insertion executed for user_id {user_id}.")
            conn.commit()
            logger.info(f"Successfully committed new user {user_id} and default preferences.")

            cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
            prefs_row = cursor.fetchone()
            if prefs_row:
                fetched_prefs = row_to_dict(prefs_row)
                logger.info(f"Successfully fetched preferences for new user {user_id}: {fetched_prefs}")
                return JSONResponse(content={
                    "message": "Authentication successful, new user and default preferences created.",
                    "user_id": user_id,
                    "telegram_id": telegram_id,
                    "preferences": fetched_prefs
                })
            else:
                logger.error(f"Failed to fetch preferences for new user {user_id} immediately after creation.")
                return JSONResponse(content={
                    "message": "Authentication successful, new user created but failed to retrieve preferences.",
                    "user_id": user_id,
                    "telegram_id": telegram_id,
                    "preference_retrieval_status": "failed_after_creation"
                }, status_code=status.HTTP_200_OK)
        else:
            db_user = row_to_dict(db_user_row)
            user_id = db_user['id']
            logger.info(f"User {telegram_id} (ID: {user_id}) already exists. Fetching preferences.")

            cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
            prefs_row = cursor.fetchone()
            if prefs_row:
                fetched_prefs = row_to_dict(prefs_row)
                logger.info(f"Successfully fetched preferences for existing user {user_id}: {fetched_prefs}")
                return JSONResponse(content={
                    "message": "Authentication successful, user exists.",
                    "user_id": user_id,
                    "telegram_id": db_user['telegram_id'],
                    "preferences": fetched_prefs
                })
            else:
                logger.warning(f"Preferences not found for existing user {user_id}. This might indicate a data inconsistency or an issue during initial preference creation.")
                return JSONResponse(content={
                    "message": "Authentication successful, user exists but preferences not found.",
                    "user_id": user_id,
                    "telegram_id": db_user['telegram_id'],
                    "preferences_status": "not_found_for_existing_user"
                }, status_code=status.HTTP_200_OK)

    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError in auth_telegram for initData '{payload.initData[:100]}...': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON in user data."
        )
    except sqlite3.Error as e:
        logger.error(f"SQLite error in auth_telegram: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"General error in auth_telegram: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing user data: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/complete_onboarding")
async def complete_onboarding(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        db_user_row = cursor.fetchone()

        if not db_user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        
        db_user = row_to_dict(db_user_row)
        if db_user['onboarding_completed']:
            return JSONResponse(content={"message": f"User {telegram_id} onboarding already completed.", "user_id": db_user['id'], "telegram_id": db_user['telegram_id']})

        cursor.execute("UPDATE users SET onboarding_completed = TRUE WHERE telegram_id = ?", (telegram_id,))
        conn.commit()
        
        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        updated_user_row = cursor.fetchone()
        updated_user = row_to_dict(updated_user_row)

        return JSONResponse(content={"message": f"User {telegram_id} onboarding completed successfully.", "user_id": updated_user['id'], "telegram_id": updated_user['telegram_id'], "onboarding_completed": updated_user['onboarding_completed']})
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error completing onboarding: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error completing onboarding: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/preferences")
async def update_user_preferences(telegram_id: str, preferences_data: UserPreferencesUpdate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info(f"update_user_preferences: Received telegram_id='{telegram_id}', type={type(telegram_id)}")
        cleaned_telegram_id = telegram_id.strip()
        logger.info(f"update_user_preferences: Querying with cleaned_telegram_id='{cleaned_telegram_id}'")
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (cleaned_telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            logger.warning(f"User with telegram_id '{cleaned_telegram_id}' not found in database.") # Log the specific ID not found
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id '{cleaned_telegram_id}' not found. Query was made with this ID."
            )
        user_id = user_row['id']

        cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
        user_preferences_row = cursor.fetchone()

        update_fields = preferences_data.model_dump(exclude_unset=True)
        
        if not user_preferences_row: # Should not happen if auth creates default prefs
            # This case is less likely now that auth creates default preferences
            # but kept for robustness or if preferences could be deleted elsewhere.
            sql = "INSERT INTO user_preferences (user_id, {}) VALUES (?, {})"
            columns = ', '.join(update_fields.keys())
            placeholders = ', '.join(['?'] * len(update_fields))
            values = [user_id] + list(update_fields.values())
            cursor.execute(sql.format(columns, placeholders), tuple(values))
            message = "User preferences created successfully."
        else:
            if not update_fields:
                 # Fetch current preferences to return if no update data is provided
                cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
                current_prefs_row = cursor.fetchone()
                return JSONResponse(content={"message": "No preference data provided for update.", "user_id": user_id, "preferences": row_to_dict(current_prefs_row)})

            set_clauses = [f"{key} = ?" for key in update_fields.keys()]
            sql = f"UPDATE user_preferences SET {', '.join(set_clauses)} WHERE user_id = ?"
            values = list(update_fields.values()) + [user_id]
            cursor.execute(sql, tuple(values))
            message = "User preferences updated successfully."
        
        conn.commit()

        cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
        updated_preferences_row = cursor.fetchone()
        return JSONResponse(content={"message": message, "user_id": user_id, "preferences": row_to_dict(updated_preferences_row)})
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error updating preferences: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating preferences: {e}"
        )
    finally:
        if conn:
            conn.close()