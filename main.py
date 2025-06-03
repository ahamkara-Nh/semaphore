import os
import hmac
import hashlib
import json
import logging
import sqlite3
from urllib.parse import unquote, parse_qs
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, status, Depends
from typing import Optional, Dict, Any, List
from database import get_db_connection, create_tables # Updated import
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import pytz

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
    allergy_nuts: Optional[bool] = False
    allergy_peanut: Optional[bool] = False
    allergy_gluten: Optional[bool] = False
    allergy_eggs: Optional[bool] = False
    allergy_fish: Optional[bool] = False
    allergy_soy: Optional[bool] = False
    fructose_filter_level: Optional[int] = 0
    lactose_filter_level: Optional[int] = 0
    fructan_filter_level: Optional[int] = 0
    mannitol_filter_level: Optional[int] = 0
    sorbitol_filter_level: Optional[int] = 0
    gos_filter_level: Optional[int] = 0
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

@app.get("/categories")
async def get_categories():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, image_name FROM product_category ORDER BY name")
        categories = [{"name": row['name'], "image_name": row['image_name']} for row in cursor.fetchall()]
        
        return {"categories": categories}
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    finally:
        if conn:
            conn.close()



@app.get("/users/{telegram_id}/onboarding_status")
async def get_onboarding_status(telegram_id: str):
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
        user_id = db_user['id']

        # Get phase tracking information
        cursor.execute("SELECT current_phase FROM phase_tracking WHERE user_id = ?", (user_id,))
        phase_row = cursor.fetchone()
        current_phase = phase_row['current_phase'] if phase_row else None

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": db_user['telegram_id'],
            "onboarding_completed": db_user['onboarding_completed'],
            "current_phase": current_phase
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error getting onboarding status: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting onboarding status: {e}"
        )
    finally:
        if conn:
            conn.close()

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

            # Create default preferences
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

            # Create the four required lists for the new user
            list_types = ['favourites', 'phase1', 'phase2', 'phase3', 'user_created']
            for list_type in list_types:
                logger.info(f"Creating {list_type} list for user_id {user_id}")
                cursor.execute(
                    "INSERT INTO user_list (user_id, list_type) VALUES (?, ?)",
                    (user_id, list_type)
                )

            conn.commit()
            logger.info(f"Successfully committed new user {user_id}, default preferences, and user lists.")

            # Fetch all created data
            cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
            prefs_row = cursor.fetchone()
            
            cursor.execute("SELECT * FROM user_list WHERE user_id = ?", (user_id,))
            lists_rows = cursor.fetchall()
            user_lists = [row_to_dict(row) for row in lists_rows]

            if prefs_row:
                fetched_prefs = row_to_dict(prefs_row)
                logger.info(f"Successfully fetched preferences for new user {user_id}: {fetched_prefs}")
                return JSONResponse(content={
                    "message": "Authentication successful, new user and default preferences created.",
                    "user_id": user_id,
                    "telegram_id": telegram_id,
                    "preferences": fetched_prefs,
                    "lists": user_lists
                })
            else:
                logger.error(f"Failed to fetch preferences for new user {user_id} immediately after creation.")
                return JSONResponse(content={
                    "message": "Authentication successful, new user created but failed to retrieve preferences.",
                    "user_id": user_id,
                    "telegram_id": telegram_id,
                    "preference_retrieval_status": "failed_after_creation",
                    "lists": user_lists
                }, status_code=status.HTTP_200_OK)
        else:
            db_user = row_to_dict(db_user_row)
            user_id = db_user['id']
            logger.info(f"User {telegram_id} (ID: {user_id}) already exists. Fetching preferences and lists.")

            cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
            prefs_row = cursor.fetchone()
            
            cursor.execute("SELECT * FROM user_list WHERE user_id = ?", (user_id,))
            lists_rows = cursor.fetchall()
            user_lists = [row_to_dict(row) for row in lists_rows]

            if prefs_row:
                fetched_prefs = row_to_dict(prefs_row)
                logger.info(f"Successfully fetched preferences for existing user {user_id}: {fetched_prefs}")
                return JSONResponse(content={
                    "message": "Authentication successful, user exists.",
                    "user_id": user_id,
                    "telegram_id": db_user['telegram_id'],
                    "preferences": fetched_prefs,
                    "lists": user_lists
                })
            else:
                logger.warning(f"Preferences not found for existing user {user_id}. This might indicate a data inconsistency or an issue during initial preference creation.")
                return JSONResponse(content={
                    "message": "Authentication successful, user exists but preferences not found.",
                    "user_id": user_id,
                    "telegram_id": db_user['telegram_id'],
                    "preferences_status": "not_found_for_existing_user",
                    "lists": user_lists
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




@app.get("/users/{telegram_id}/preferences")
async def get_user_preferences(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get all preferences
        cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
        preferences_row = cursor.fetchone()
        if not preferences_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Preferences not found for user with telegram_id {telegram_id}"
            )

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "preferences": row_to_dict(preferences_row)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error getting preferences: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting preferences: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/preferences/fodmap")
async def update_user_fodmap_preferences(telegram_id: str, preferences_data: UserPreferencesUpdate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Extract only FODMAP-related fields
        fodmap_fields = {
            key: value for key, value in preferences_data.model_dump(exclude_unset=True).items()
            if key.endswith('_filter_level')
        }

        if not fodmap_fields:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No FODMAP filter levels provided for update"
            )

        # Update FODMAP filter levels
        set_clauses = [f"{key} = ?" for key in fodmap_fields.keys()]
        sql = f"UPDATE user_preferences SET {', '.join(set_clauses)} WHERE user_id = ?"
        values = list(fodmap_fields.values()) + [user_id]
        cursor.execute(sql, tuple(values))
        conn.commit()

        # Get updated preferences
        cursor.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
        updated_prefs = cursor.fetchone()

        return JSONResponse(content={
            "message": "FODMAP filter levels updated successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "preferences": row_to_dict(updated_prefs)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error updating FODMAP preferences: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating FODMAP preferences: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/users/{telegram_id}/preferences/created-at")
async def get_user_preferences_created_at(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get created_at from user_preferences
        cursor.execute("SELECT created_at FROM user_preferences WHERE user_id = ?", (user_id,))
        preferences_row = cursor.fetchone()
        if not preferences_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Preferences not found for user with telegram_id {telegram_id}"
            )

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "created_at": preferences_row['created_at']
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error getting preferences created_at: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting preferences created_at: {e}"
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


class PhaseTrackingCreate(BaseModel):
    current_phase: int

class PhaseTrackingUpdate(BaseModel):
    current_phase: Optional[int] = None
    phase1_streak_days: Optional[int] = None
    phase2_reintroduction_days: Optional[int] = None
    phase2_break_days: Optional[int] = None
    phase2_current_fodmap_group_id: Optional[int] = None

class PhaseTrackingResponse(BaseModel):
    phase_tracking_id: int
    user_id: int
    current_phase: int
    phase1_streak_days: int
    phase2_reintroduction_days: int
    phase2_break_days: int
    phase2_current_fodmap_group_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime

@app.get("/users/{telegram_id}/phase-tracking", response_model=PhaseTrackingResponse)
async def get_user_phase_tracking(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get phase tracking information
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        phase_tracking_row = cursor.fetchone()
        if not phase_tracking_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase tracking not found for user with telegram_id {telegram_id}"
            )

        phase_tracking_dict = row_to_dict(phase_tracking_row)
        return PhaseTrackingResponse(**phase_tracking_dict)

    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_user_phase_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"General error in get_user_phase_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting phase tracking: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.post("/users/{telegram_id}/phase-tracking", response_model=PhaseTrackingResponse, status_code=status.HTTP_201_CREATED)
async def create_user_phase_tracking(telegram_id: str, phase_data: PhaseTrackingCreate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cleaned_telegram_id = telegram_id.strip()
        logger.info(f"Attempting to create phase tracking for telegram_id: {cleaned_telegram_id}")

        # 1. Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (cleaned_telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            logger.warning(f"User with telegram_id '{cleaned_telegram_id}' not found.")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id '{cleaned_telegram_id}' not found."
            )
        user_id = user_row['id']
        logger.info(f"User found: user_id {user_id} for telegram_id {cleaned_telegram_id}.")

        # 2. Check if phase_tracking record already exists for this user_id
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        existing_phase_tracking_row = cursor.fetchone()
        if existing_phase_tracking_row:
            logger.warning(f"Phase tracking record already exists for user_id {user_id}.")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Phase tracking record already exists for user_id {user_id}."
            )

        # 3. Insert new phase_tracking record with provided current_phase
        logger.info(f"Inserting new phase tracking record for user_id {user_id} with current_phase {phase_data.current_phase}.")
        cursor.execute(
            "INSERT INTO phase_tracking (user_id, current_phase) VALUES (?, ?)",
            (user_id, phase_data.current_phase)
        )
        conn.commit()
        logger.info(f"Successfully inserted and committed phase tracking for user_id {user_id}.")

        # 4. Fetch the newly created record to return it
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        new_phase_tracking_row = cursor.fetchone()

        if not new_phase_tracking_row:
            logger.error(f"Failed to retrieve phase tracking record for user_id {user_id} after insertion.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve phase tracking record after creation."
            )
        
        created_record_dict = row_to_dict(new_phase_tracking_row)
        logger.info(f"Successfully created and retrieved phase tracking for user_id {user_id}: {created_record_dict}")
        
        return PhaseTrackingResponse(**created_record_dict)

    except sqlite3.Error as e:
        logger.error(f"SQLite error in create_user_phase_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException: 
        raise
    except Exception as e:
        logger.error(f"General error in create_user_phase_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating phase tracking: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/phase-tracking", response_model=PhaseTrackingResponse)
async def update_phase_tracking(telegram_id: str, update_data: PhaseTrackingUpdate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if phase_tracking exists for this user
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        phase_tracking_row = cursor.fetchone()
        if not phase_tracking_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase tracking not found for user with telegram_id {telegram_id}"
            )

        # Prepare update data
        update_fields = update_data.model_dump(exclude_unset=True)
        if not update_fields:
            # If no update fields provided, just return current data
            return PhaseTrackingResponse(**row_to_dict(phase_tracking_row))

        # Build and execute update query
        set_clauses = [f"{key} = ?" for key in update_fields.keys()]
        sql = f"UPDATE phase_tracking SET {', '.join(set_clauses)} WHERE user_id = ?"
        values = list(update_fields.values()) + [user_id]
        cursor.execute(sql, tuple(values))
        conn.commit()

        # Get updated phase tracking
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        updated_phase_tracking_row = cursor.fetchone()
        
        return PhaseTrackingResponse(**row_to_dict(updated_phase_tracking_row))

    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_phase_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase tracking: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/categories/{category_id}/products/{telegram_id}")
async def get_filtered_products_by_category(category_id: int, telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # First, verify the category exists
        cursor.execute("SELECT category_id FROM product_category WHERE category_id = ?", (category_id,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Category with id {category_id} not found"
            )

        # Get user preferences
        cursor.execute("""
            SELECT up.* 
            FROM user_preferences up
            JOIN users u ON u.id = up.user_id
            WHERE u.telegram_id = ?
        """, (telegram_id,))
        user_prefs = cursor.fetchone()
        if not user_prefs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Preferences not found for user with telegram_id {telegram_id}"
            )

        # Build the WHERE clause based on allergies and FODMAP levels
        where_conditions = ["category_id = ?"]
        params = [category_id]

        # Add allergy conditions
        if user_prefs['allergy_nuts']:
            where_conditions.append("contains_nuts = FALSE")
        if user_prefs['allergy_peanut']:
            where_conditions.append("contains_peanut = FALSE")
        if user_prefs['allergy_gluten']:
            where_conditions.append("contains_gluten = FALSE")
        if user_prefs['allergy_eggs']:
            where_conditions.append("contains_eggs = FALSE")
        if user_prefs['allergy_fish']:
            where_conditions.append("contains_fish = FALSE")
        if user_prefs['allergy_soy']:
            where_conditions.append("contains_soy = FALSE")

        # Add FODMAP level conditions only if the filter level is greater than 0
        fodmap_conditions = []
        if user_prefs['fructose_filter_level'] > 0:
            fodmap_conditions.append(f"fructose_level = {user_prefs['fructose_filter_level']}")
        if user_prefs['lactose_filter_level'] > 0:
            fodmap_conditions.append(f"lactose_level = {user_prefs['lactose_filter_level']}")
        if user_prefs['fructan_filter_level'] > 0:
            fodmap_conditions.append(f"fructan_level = {user_prefs['fructan_filter_level']}")
        if user_prefs['mannitol_filter_level'] > 0:
            fodmap_conditions.append(f"mannitol_level = {user_prefs['mannitol_filter_level']}")
        if user_prefs['sorbitol_filter_level'] > 0:
            fodmap_conditions.append(f"sorbitol_level = {user_prefs['sorbitol_filter_level']}")
        if user_prefs['gos_filter_level'] > 0:
            fodmap_conditions.append(f"gos_level = {user_prefs['gos_filter_level']}")

        # Add FODMAP conditions to WHERE clause if any exist
        if fodmap_conditions:
            where_conditions.extend(fodmap_conditions)

        # Build and execute the query using a subquery that selects the product with the highest
        # serving_amount_grams for each unique product name
        query = f"""
            WITH RankedProducts AS (
                SELECT 
                    product_id,
                    name,
                    fructose_level,
                    lactose_level,
                    fructan_level,
                    mannitol_level,
                    sorbitol_level,
                    gos_level,
                    serving_title,
                    serving_amount_grams,
                    contains_nuts,
                    contains_peanut,
                    contains_gluten,
                    contains_eggs,
                    contains_fish,
                    contains_soy,
                    replacement_name,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY serving_amount_grams DESC) as rn
                FROM product 
                WHERE {' AND '.join(where_conditions)}
            )
            SELECT 
                product_id,
                name,
                fructose_level,
                lactose_level,
                fructan_level,
                mannitol_level,
                sorbitol_level,
                gos_level,
                serving_title,
                serving_amount_grams,
                contains_nuts,
                contains_peanut,
                contains_gluten,
                contains_eggs,
                contains_fish,
                contains_soy,
                replacement_name
            FROM RankedProducts 
            WHERE rn = 1
            ORDER BY name
        """
        
        cursor.execute(query, tuple(params))
        products = [row_to_dict(row) for row in cursor.fetchall()]

        return {
            "category_id": category_id,
            "products": products,
            "filters_applied": {
                "allergies": {
                    "nuts": user_prefs['allergy_nuts'],
                    "peanut": user_prefs['allergy_peanut'],
                    "gluten": user_prefs['allergy_gluten'],
                    "eggs": user_prefs['allergy_eggs'],
                    "fish": user_prefs['allergy_fish'],
                    "soy": user_prefs['allergy_soy']
                },
                "fodmap_levels": {
                    "fructose": user_prefs['fructose_filter_level'],
                    "lactose": user_prefs['lactose_filter_level'],
                    "fructan": user_prefs['fructan_filter_level'],
                    "mannitol": user_prefs['mannitol_filter_level'],
                    "sorbitol": user_prefs['sorbitol_filter_level'],
                    "gos": user_prefs['gos_filter_level']
                }
            }
        }

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error getting filtered products: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting filtered products: {e}"
        )
    finally:
        if conn:
            conn.close()

class ProductSearch(BaseModel):
    search_term: str

@app.post("/products/search/{telegram_id}")
async def search_products_by_name(telegram_id: str, search_data: ProductSearch):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user preferences
        cursor.execute("""
            SELECT up.* 
            FROM user_preferences up
            JOIN users u ON u.id = up.user_id
            WHERE u.telegram_id = ?
        """, (telegram_id,))
        user_prefs = cursor.fetchone()
        if not user_prefs:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Preferences not found for user with telegram_id {telegram_id}"
            )

        # Build the WHERE clause based on allergies
        where_conditions = ["name LIKE ?"]
        params = [f"%{search_data.search_term}%"]

        # Add allergy conditions
        if user_prefs['allergy_nuts']:
            where_conditions.append("contains_nuts = FALSE")
        if user_prefs['allergy_peanut']:
            where_conditions.append("contains_peanut = FALSE")
        if user_prefs['allergy_gluten']:
            where_conditions.append("contains_gluten = FALSE")
        if user_prefs['allergy_eggs']:
            where_conditions.append("contains_eggs = FALSE")
        if user_prefs['allergy_fish']:
            where_conditions.append("contains_fish = FALSE")
        if user_prefs['allergy_soy']:
            where_conditions.append("contains_soy = FALSE")
            
        # Add FODMAP level conditions only if the filter level is greater than 0
        fodmap_conditions = []
        if user_prefs['fructose_filter_level'] > 0:
            fodmap_conditions.append(f"fructose_level = {user_prefs['fructose_filter_level']}")
        if user_prefs['lactose_filter_level'] > 0:
            fodmap_conditions.append(f"lactose_level = {user_prefs['lactose_filter_level']}")
        if user_prefs['fructan_filter_level'] > 0:
            fodmap_conditions.append(f"fructan_level = {user_prefs['fructan_filter_level']}")
        if user_prefs['mannitol_filter_level'] > 0:
            fodmap_conditions.append(f"mannitol_level = {user_prefs['mannitol_filter_level']}")
        if user_prefs['sorbitol_filter_level'] > 0:
            fodmap_conditions.append(f"sorbitol_level = {user_prefs['sorbitol_filter_level']}")
        if user_prefs['gos_filter_level'] > 0:
            fodmap_conditions.append(f"gos_level = {user_prefs['gos_filter_level']}")

        # Add FODMAP conditions to WHERE clause if any exist
        if fodmap_conditions:
            where_conditions.extend(fodmap_conditions)

        # Build and execute the query
        query = f"""
            WITH RankedProducts AS (
                SELECT 
                    product_id,
                    name,
                    category_id,
                    fructose_level,
                    lactose_level,
                    fructan_level,
                    mannitol_level,
                    sorbitol_level,
                    gos_level,
                    serving_title,
                    serving_amount_grams,
                    contains_nuts,
                    contains_peanut,
                    contains_gluten,
                    contains_eggs,
                    contains_fish,
                    contains_soy,
                    replacement_name,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY serving_amount_grams DESC) as rn
                FROM product 
                WHERE {' AND '.join(where_conditions)}
            )
            SELECT 
                p.product_id,
                p.name,
                p.category_id,
                pc.name as category_name,
                p.fructose_level,
                p.lactose_level,
                p.fructan_level,
                p.mannitol_level,
                p.sorbitol_level,
                p.gos_level,
                p.serving_title,
                p.serving_amount_grams,
                p.contains_nuts,
                p.contains_peanut,
                p.contains_gluten,
                p.contains_eggs,
                p.contains_fish,
                p.contains_soy,
                p.replacement_name
            FROM RankedProducts p
            JOIN product_category pc ON p.category_id = pc.category_id
            WHERE rn = 1
            ORDER BY p.name
            LIMIT 10
        """
        
        cursor.execute(query, tuple(params))
        products = [row_to_dict(row) for row in cursor.fetchall()]

        return {
            "search_term": search_data.search_term,
            "products": products,
            "filters_applied": {
                "allergies": {
                    "nuts": user_prefs['allergy_nuts'],
                    "peanut": user_prefs['allergy_peanut'],
                    "gluten": user_prefs['allergy_gluten'],
                    "eggs": user_prefs['allergy_eggs'],
                    "fish": user_prefs['allergy_fish'],
                    "soy": user_prefs['allergy_soy']
                },
                "fodmap_levels": {
                    "fructose": user_prefs['fructose_filter_level'],
                    "lactose": user_prefs['lactose_filter_level'],
                    "fructan": user_prefs['fructan_filter_level'],
                    "mannitol": user_prefs['mannitol_filter_level'],
                    "sorbitol": user_prefs['sorbitol_filter_level'],
                    "gos": user_prefs['gos_filter_level']
                }
            }
        }

    except sqlite3.Error as e:
        logger.error(f"SQLite error in search_products_by_name: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error searching products: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching products: {e}"
        )
    finally:
        if conn:
            conn.close()

class ProductNameRequest(BaseModel):
    name: str

@app.post("/products/get-by-name")
async def get_products_by_exact_name(product_data: ProductNameRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all product rows that match the exact name
        cursor.execute("""
            SELECT 
                product_id,
                name,
                category_id,
                fructose_level,
                lactose_level,
                fructan_level,
                mannitol_level,
                sorbitol_level,
                gos_level,
                serving_title,
                serving_amount_grams,
                contains_nuts,
                contains_peanut,
                contains_gluten,
                contains_eggs,
                contains_fish,
                contains_soy,
                replacement_name
            FROM product 
            WHERE name = ?
            ORDER BY serving_amount_grams DESC
        """, (product_data.name,))
        
        products = [row_to_dict(row) for row in cursor.fetchall()]
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No products found with name: {product_data.name}"
            )
            
        return {
            "name": product_data.name,
            "products": products,
            "count": len(products)
        }
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_products_by_exact_name: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting products by name: {e}")
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting products by name: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/users/{telegram_id}/lists/{list_type}/items")
async def get_user_list_items(telegram_id: str, list_type: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get list_id for the specified list_type
        cursor.execute("""
            SELECT list_id 
            FROM user_list 
            WHERE user_id = ? AND list_type = ?
        """, (user_id, list_type))
        list_row = cursor.fetchone()
        if not list_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"List of type {list_type} not found for user with telegram_id {telegram_id}"
            )
        list_id = list_row['list_id']

        # Get all list items with product information
        cursor.execute("""
            SELECT 
                uli.list_item_id,
                uli.created_at as added_at,
                p.product_id,
                p.name,
                p.category_id,
                pc.name as category_name,
                p.fructose_level,
                p.lactose_level,
                p.fructan_level,
                p.mannitol_level,
                p.sorbitol_level,
                p.gos_level,
                p.serving_title,
                p.serving_amount_grams,
                p.contains_nuts,
                p.contains_peanut,
                p.contains_gluten,
                p.contains_eggs,
                p.contains_fish,
                p.contains_soy,
                p.replacement_name
            FROM user_list_item uli
            JOIN product p ON uli.food_id = p.product_id
            JOIN product_category pc ON p.category_id = pc.category_id
            WHERE uli.list_id = ?
            ORDER BY uli.created_at DESC
        """, (list_id,))
        
        list_items = [row_to_dict(row) for row in cursor.fetchall()]

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "list_type": list_type,
            "list_id": list_id,
            "items": list_items
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_user_list_items: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user list items: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting user list items: {e}"
        )
    finally:
        if conn:
            conn.close()

class AddProductToListRequest(BaseModel):
    product_id: int
    list_type: str

@app.post("/users/{telegram_id}/lists/add-product")
async def add_product_to_list(telegram_id: str, request: AddProductToListRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get list_id for the specified list_type
        cursor.execute("""
            SELECT list_id 
            FROM user_list 
            WHERE user_id = ? AND list_type = ?
        """, (user_id, request.list_type))
        list_row = cursor.fetchone()
        if not list_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"List of type {request.list_type} not found for user with telegram_id {telegram_id}"
            )
        list_id = list_row['list_id']

        # Verify product exists
        cursor.execute("SELECT product_id FROM product WHERE product_id = ?", (request.product_id,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with id {request.product_id} not found"
            )

        # Check if product already exists in the list
        cursor.execute("""
            SELECT list_item_id 
            FROM user_list_item 
            WHERE list_id = ? AND food_id = ?
        """, (list_id, request.product_id))
        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Product {request.product_id} already exists in list {request.list_type}"
            )

        # Add product to list
        cursor.execute("""
            INSERT INTO user_list_item (list_id, food_id)
            VALUES (?, ?)
        """, (list_id, request.product_id))
        
        conn.commit()

        return JSONResponse(content={
            "message": "Product added successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "list_type": request.list_type,
            "product_id": request.product_id
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in add_product_to_list: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding product to list: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error adding product to list: {e}"
        )
    finally:
        if conn:
            conn.close()

class ProductCheckRequest(BaseModel):
    product_id: int

@app.post("/users/{telegram_id}/lists/check-product")
async def check_product_in_lists(telegram_id: str, request: ProductCheckRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Verify product exists
        cursor.execute("SELECT product_id FROM product WHERE product_id = ?", (request.product_id,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with id {request.product_id} not found"
            )

        # Check if product exists in any of the user's lists
        cursor.execute("""
            SELECT ul.list_type
            FROM user_list ul
            JOIN user_list_item uli ON ul.list_id = uli.list_id
            WHERE ul.user_id = ? AND uli.food_id = ?
        """, (user_id, request.product_id))
        
        lists_containing_product = [row['list_type'] for row in cursor.fetchall()]

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "product_id": request.product_id,
            "exists_in_lists": lists_containing_product,
            "is_in_any_list": len(lists_containing_product) > 0
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in check_product_in_lists: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking product in lists: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking product in lists: {e}"
        )
    finally:
        if conn:
            conn.close()

class RemoveProductFromListRequest(BaseModel):
    product_id: int
    list_type: str

@app.delete("/users/{telegram_id}/lists/remove-product")
async def remove_product_from_list(telegram_id: str, request: RemoveProductFromListRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get list_id for the specified list_type
        cursor.execute("""
            SELECT list_id 
            FROM user_list 
            WHERE user_id = ? AND list_type = ?
        """, (user_id, request.list_type))
        list_row = cursor.fetchone()
        if not list_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"List of type {request.list_type} not found for user with telegram_id {telegram_id}"
            )
        list_id = list_row['list_id']

        # Verify product exists
        cursor.execute("SELECT product_id FROM product WHERE product_id = ?", (request.product_id,))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with id {request.product_id} not found"
            )

        # Check if product exists in the list before attempting to remove
        cursor.execute("""
            SELECT list_item_id 
            FROM user_list_item 
            WHERE list_id = ? AND food_id = ?
        """, (list_id, request.product_id))
        if not cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {request.product_id} not found in list {request.list_type}"
            )

        # Remove product from list
        cursor.execute("""
            DELETE FROM user_list_item 
            WHERE list_id = ? AND food_id = ?
        """, (list_id, request.product_id))
        
        conn.commit()

        return JSONResponse(content={
            "message": "Product removed successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "list_type": request.list_type,
            "product_id": request.product_id
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in remove_product_from_list: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing product from list: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error removing product from list: {e}"
        )
    finally:
        if conn:
            conn.close()

class CreateUserProductRequest(BaseModel):
    name: str
    fructose_level: int
    lactose_level: int
    fructan_level: int
    mannitol_level: int
    sorbitol_level: int
    gos_level: int
    serving_title: str

@app.post("/users/{telegram_id}/products", status_code=status.HTTP_201_CREATED)
async def create_user_product(telegram_id: str, product_data: CreateUserProductRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if a product with this name already exists for this user
        cursor.execute("""
            SELECT user_product_id 
            FROM user_products 
            WHERE creator_id = ? AND name = ?
        """, (user_id, product_data.name))
        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A product with name '{product_data.name}' already exists for this user"
            )

        # Insert the new user product
        cursor.execute("""
            INSERT INTO user_products (
                creator_id, name, fructose_level, lactose_level,
                fructan_level, mannitol_level, sorbitol_level, gos_level,
                serving_title
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            product_data.name,
            product_data.fructose_level,
            product_data.lactose_level,
            product_data.fructan_level,
            product_data.mannitol_level,
            product_data.sorbitol_level,
            product_data.gos_level,
            product_data.serving_title
        ))
        
        conn.commit()

        # Get the newly created product
        cursor.execute("""
            SELECT * FROM user_products 
            WHERE user_product_id = last_insert_rowid()
        """)
        new_product = cursor.fetchone()

        return JSONResponse(content={
            "message": "User product created successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "product": row_to_dict(new_product)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in create_user_product: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user product: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating user product: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/users/{telegram_id}/products")
async def get_user_products(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get all products created by this user
        cursor.execute("""
            SELECT 
                user_product_id,
                name,
                fructose_level,
                lactose_level,
                fructan_level,
                mannitol_level,
                sorbitol_level,
                gos_level,
                serving_title,
                created_at,
                updated_at
            FROM user_products 
            WHERE creator_id = ?
            ORDER BY created_at DESC
        """, (user_id,))
        
        products = [row_to_dict(row) for row in cursor.fetchall()]

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "products": products,
            "total_count": len(products)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_user_products: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user products: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting user products: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.delete("/users/{telegram_id}/products/{product_name}")
async def delete_user_product(telegram_id: str, product_name: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if the product exists and belongs to the user
        cursor.execute("""
            SELECT user_product_id, name
            FROM user_products 
            WHERE name = ? AND creator_id = ?
        """, (product_name, user_id))
        product = cursor.fetchone()
        
        if not product:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with name '{product_name}' not found or doesn't belong to this user"
            )

        # Delete the product
        cursor.execute("""
            DELETE FROM user_products 
            WHERE name = ? AND creator_id = ?
        """, (product_name, user_id))
        
        conn.commit()

        return JSONResponse(content={
            "message": f"Product '{product_name}' deleted successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "product_name": product_name
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in delete_user_product: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting user product: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting user product: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/recipes")
async def get_all_recipes():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all recipes ordered by creation date (newest first)
        cursor.execute("""
            SELECT 
                recipe_id,
                name,
                image_name,
                ingredients,
                preparation,
                created_at,
                updated_at
            FROM recipes 
            ORDER BY created_at DESC
        """)
        
        recipes = []
        for row in cursor.fetchall():
            recipe = row_to_dict(row)
            # Convert ingredients and preparation from newline-separated strings to lists
            recipe['ingredients'] = recipe['ingredients'].split('\n') if recipe['ingredients'] else []
            recipe['preparation'] = recipe['preparation'].split('\n') if recipe['preparation'] else []
            recipes.append(recipe)

        return JSONResponse(content={
            "recipes": recipes,
            "total_count": len(recipes)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_all_recipes: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error getting recipes: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting recipes: {e}"
        )
    finally:
        if conn:
            conn.close()

class SymptomsDiaryCreate(BaseModel):
    wind_level: int = Field(..., ge=0, le=10, description="Wind level from 0-10")
    bloat_level: int = Field(..., ge=0, le=10, description="Bloating level from 0-10")
    pain_level: int = Field(..., ge=0, le=10, description="Pain level from 0-10")
    stool_level: int = Field(..., ge=0, le=10, description="Stool consistency level from 0-10")
    notes: Optional[str] = Field(None, description="Optional notes about symptoms")

@app.post("/users/{telegram_id}/symptoms-diary", status_code=status.HTTP_201_CREATED)
async def create_symptoms_diary_entry(telegram_id: str, diary_data: SymptomsDiaryCreate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Insert the new diary entry
        cursor.execute("""
            INSERT INTO symptoms_diary (
                user_id, wind_level, bloat_level,
                pain_level, stool_level, notes
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            diary_data.wind_level,
            diary_data.bloat_level,
            diary_data.pain_level,
            diary_data.stool_level,
            diary_data.notes
        ))
        
        conn.commit()

        # Get the newly created diary entry
        cursor.execute("""
            SELECT * FROM symptoms_diary 
            WHERE diary_id = last_insert_rowid()
        """)
        new_entry = cursor.fetchone()

        return JSONResponse(content={
            "message": "Symptoms diary entry created successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "diary_entry": row_to_dict(new_entry)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in create_symptoms_diary_entry: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating symptoms diary entry: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating symptoms diary entry: {e}"
        )
    finally:
        if conn:
            conn.close()


class FoodItem(BaseModel):
    id: int
    is_user_product: bool = False

class CreateFoodNoteRequest(BaseModel):
    memo: str
    foods: List[FoodItem]

@app.post("/users/{telegram_id}/food-notes", status_code=status.HTTP_201_CREATED)
async def create_food_note(telegram_id: str, note_data: CreateFoodNoteRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Create a new food note list
        list_type = f"food_note_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cursor.execute(
            "INSERT INTO user_list (user_id, list_type) VALUES (?, ?)",
            (user_id, list_type)
        )
        
        # Get the newly created list ID
        cursor.execute("SELECT last_insert_rowid()")
        list_id = cursor.fetchone()[0]
        
        # Add each food item to the list
        for food in note_data.foods:
            if food.is_user_product:
                # Verify user product exists
                cursor.execute(
                    "SELECT user_product_id FROM user_products WHERE user_product_id = ? AND creator_id = ?",
                    (food.id, user_id)
                )
                if not cursor.fetchone():
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"User product with id {food.id} not found"
                    )
                
                # For user-created products, we need to store differently
                # Modifying this to match the schema - user_created_id references users(id)
                cursor.execute(
                    """
                    INSERT INTO user_list_item (list_id, food_id, user_created_id) 
                    SELECT ?, up.user_product_id, up.creator_id
                    FROM user_products up
                    WHERE up.user_product_id = ?
                    """,
                    (list_id, food.id)
                )
            else:
                # Verify product exists
                cursor.execute("SELECT product_id FROM product WHERE product_id = ?", (food.id,))
                if not cursor.fetchone():
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Product with id {food.id} not found"
                    )
                
                # Add product to list
                cursor.execute(
                    "INSERT INTO user_list_item (list_id, food_id) VALUES (?, ?)",
                    (list_id, food.id)
                )
        
        # Create food note
        cursor.execute(
            "INSERT INTO food_notes (user_id, food_list_id, memo) VALUES (?, ?, ?)",
            (user_id, list_id, note_data.memo)
        )
        
        conn.commit()
        
        # Get the newly created note
        cursor.execute("SELECT * FROM food_notes WHERE note_id = last_insert_rowid()")
        new_note = cursor.fetchone()
        
        # Get the food items in the note
        cursor.execute("""
            SELECT 
                uli.list_item_id,
                uli.food_id,
                uli.user_created_id,
                uli.created_at,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT name FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT name FROM product WHERE product_id = uli.food_id)
                END as name,
                CASE
                    WHEN uli.user_created_id IS NOT NULL THEN TRUE
                    ELSE FALSE
                END as is_user_product,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT fructose_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT fructose_level FROM product WHERE product_id = uli.food_id)
                END as fructose_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT lactose_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT lactose_level FROM product WHERE product_id = uli.food_id)
                END as lactose_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT fructan_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT fructan_level FROM product WHERE product_id = uli.food_id)
                END as fructan_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT mannitol_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT mannitol_level FROM product WHERE product_id = uli.food_id)
                END as mannitol_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT sorbitol_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT sorbitol_level FROM product WHERE product_id = uli.food_id)
                END as sorbitol_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT gos_level FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT gos_level FROM product WHERE product_id = uli.food_id)
                END as gos_level,
                CASE 
                    WHEN uli.user_created_id IS NOT NULL THEN 
                        (SELECT serving_title FROM user_products WHERE user_product_id = uli.food_id)
                    ELSE 
                        (SELECT serving_title FROM product WHERE product_id = uli.food_id)
                END as serving_title
            FROM user_list_item uli
            WHERE uli.list_id = ?
        """, (list_id,))
        
        food_items = []
        for item in cursor.fetchall():
            item_dict = row_to_dict(item)
            # For user-created products, we need to map FODMAP levels differently
            # User products: 0 = high (3), 1 = medium (2), 2 = low (1)
            # Standard products: values are already correct
            if item_dict['is_user_product']:
                fodmap_fields = ['fructose_level', 'lactose_level', 'fructan_level', 
                                'mannitol_level', 'sorbitol_level', 'gos_level']
                for field in fodmap_fields:
                    # Map user product values: 0->3 (high), 1->2 (medium), 2->1 (low)
                    if item_dict[field] == 0:
                        item_dict[field] = 3  # high
                    elif item_dict[field] == 1:
                        item_dict[field] = 2  # medium
                    elif item_dict[field] == 2:
                        item_dict[field] = 1  # low
            
            food_items.append(item_dict)
        
        note = row_to_dict(new_note)
        note['food_items'] = food_items
        note['entry_type'] = 'food_note'
        note['entry_id'] = note['note_id']

        return JSONResponse(content={
            "message": "Food note created successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "note": note,
            "food_items": food_items
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in create_food_note: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating food note: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating food note: {e}"
        )
    finally:
        if conn:
            conn.close()

class DiaryHistoryPage(BaseModel):
    page: int = Field(1, description="Page number to retrieve, starting from 1")
    items_per_page: int = Field(3, description="Number of items per page")

@app.get("/users/{telegram_id}/diary-history")
async def get_user_diary_history(telegram_id: str, page_params: DiaryHistoryPage = Depends()):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Calculate pagination parameters
        page = max(1, page_params.page)  # Ensure page is at least 1
        items_per_page = page_params.items_per_page
        offset = (page - 1) * items_per_page

        # Get symptoms diary entries
        cursor.execute("""
            SELECT 
                'symptoms_diary' as entry_type,
                diary_id as entry_id,
                created_at,
                wind_level,
                bloat_level,
                pain_level,
                stool_level,
                notes,
                NULL as memo,
                NULL as food_list_id
            FROM symptoms_diary 
            WHERE user_id = ?
        """, (user_id,))
        symptoms_entries = [row_to_dict(row) for row in cursor.fetchall()]

        # Get food notes with their items
        cursor.execute("""
            SELECT 
                fn.note_id,
                fn.created_at,
                fn.memo,
                fn.food_list_id
            FROM food_notes fn
            WHERE fn.user_id = ?
        """, (user_id,))
        food_notes = [row_to_dict(row) for row in cursor.fetchall()]

        # For each food note, get its items
        for note in food_notes:
            cursor.execute("""
                SELECT 
                    uli.list_item_id,
                    uli.food_id,
                    uli.user_created_id,
                    uli.created_at,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT name FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT name FROM product WHERE product_id = uli.food_id)
                    END as name,
                    CASE
                        WHEN uli.user_created_id IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END as is_user_product,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT fructose_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT fructose_level FROM product WHERE product_id = uli.food_id)
                    END as fructose_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT lactose_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT lactose_level FROM product WHERE product_id = uli.food_id)
                    END as lactose_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT fructan_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT fructan_level FROM product WHERE product_id = uli.food_id)
                    END as fructan_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT mannitol_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT mannitol_level FROM product WHERE product_id = uli.food_id)
                    END as mannitol_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT sorbitol_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT sorbitol_level FROM product WHERE product_id = uli.food_id)
                    END as sorbitol_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT gos_level FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT gos_level FROM product WHERE product_id = uli.food_id)
                    END as gos_level,
                    CASE 
                        WHEN uli.user_created_id IS NOT NULL THEN 
                            (SELECT serving_title FROM user_products WHERE user_product_id = uli.food_id)
                        ELSE 
                            (SELECT serving_title FROM product WHERE product_id = uli.food_id)
                    END as serving_title
                FROM user_list_item uli
                WHERE uli.list_id = ?
            """, (note['food_list_id'],))
            food_items = []
            for item in cursor.fetchall():
                item_dict = row_to_dict(item)
                # For user-created products, we need to map FODMAP levels differently
                # User products: 0 = high (3), 1 = medium (2), 2 = low (1)
                # Standard products: values are already correct
                if item_dict['is_user_product']:
                    fodmap_fields = ['fructose_level', 'lactose_level', 'fructan_level', 
                                    'mannitol_level', 'sorbitol_level', 'gos_level']
                    for field in fodmap_fields:
                        # Map user product values: 0->3 (high), 1->2 (medium), 2->1 (low)
                        if item_dict[field] == 0:
                            item_dict[field] = 3  # high
                        elif item_dict[field] == 1:
                            item_dict[field] = 2  # medium
                        elif item_dict[field] == 2:
                            item_dict[field] = 1  # low
                
                food_items.append(item_dict)
            
            note['food_items'] = food_items
            note['entry_type'] = 'food_note'
            note['entry_id'] = note['note_id']

        # Combine the two types of entries
        all_entries = symptoms_entries + food_notes

        # Sort by created_at in descending order
        all_entries.sort(key=lambda x: x['created_at'], reverse=True)

        # Apply pagination
        total_entries = len(all_entries)
        total_pages = (total_entries + items_per_page - 1) // items_per_page  # Ceiling division
        paginated_entries = all_entries[offset:offset + items_per_page]

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "page": page,
            "items_per_page": items_per_page,
            "total_entries": total_entries,
            "total_pages": total_pages,
            "entries": paginated_entries
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_user_diary_history: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user diary history: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting user diary history: {e}"
        )
    finally:
        if conn:
            conn.close()

class UpdatePhaseTrackingRequest(BaseModel):
    timezone: str = Field(..., description="User's timezone in IANA format, e.g. 'Europe/London'")

@app.put("/users/{telegram_id}/phase-tracking/update-streak")
async def update_phase1_streak_days(telegram_id: str, request: UpdatePhaseTrackingRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if phase_tracking exists for this user
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        phase_tracking_row = cursor.fetchone()
        if not phase_tracking_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase tracking not found for user with telegram_id {telegram_id}"
            )

        # Get the user's timezone
        try:
            user_timezone = pytz.timezone(request.timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timezone: {request.timezone}"
            )

        # Get current time in user's timezone
        now = datetime.now(user_timezone)
        today_date = now.date()
        
        # Check if there's a phase1_date in phases_timings
        cursor.execute("""
            SELECT phase1_date 
            FROM phases_timings 
            WHERE user_id = ?
        """, (user_id,))
        phase_timing_row = cursor.fetchone()
        phase1_date = None
        
        if phase_timing_row and phase_timing_row['phase1_date']:
            # Convert phase1_date to user's timezone
            phase1_date_str = phase_timing_row['phase1_date']
            phase1_dt = datetime.fromisoformat(phase1_date_str.replace('Z', '+00:00'))
            phase1_dt = phase1_dt.astimezone(user_timezone)
            phase1_date = phase1_dt.date()
            logger.info(f"Found phase1_date: {phase1_date} for user_id {user_id}")
        
        # Get symptoms diary entries ordered by date (descending)
        if phase1_date:
            # If we have a phase1_date, only consider entries from that date onwards
            phase1_date_iso = phase1_date.isoformat()
            cursor.execute("""
                SELECT 
                    diary_id,
                    wind_level,
                    bloat_level,
                    pain_level,
                    stool_level,
                    created_at
                FROM symptoms_diary 
                WHERE user_id = ? AND date(created_at) >= date(?)
                ORDER BY created_at DESC
            """, (user_id, phase1_date_iso))
            logger.info(f"Filtering diary entries from phase1_date: {phase1_date_iso}")
        else:
            # Otherwise, get all entries
            cursor.execute("""
                SELECT 
                    diary_id,
                    wind_level,
                    bloat_level,
                    pain_level,
                    stool_level,
                    created_at
                FROM symptoms_diary 
                WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
            logger.info("No phase1_date found, using all diary entries")
        
        diary_entries = [row_to_dict(row) for row in cursor.fetchall()]
        
        # Group entries by date in user's timezone
        entries_by_date = {}
        for entry in diary_entries:
            # Convert entry timestamp to user's timezone
            entry_dt = datetime.fromisoformat(entry['created_at'].replace('Z', '+00:00'))
            entry_dt = entry_dt.astimezone(user_timezone)
            entry_date = entry_dt.date()
            
            if entry_date not in entries_by_date:
                entries_by_date[entry_date] = []
            entries_by_date[entry_date].append(entry)
        
        # Calculate streak days
        streak_days = 0
        current_date = today_date
        days_without_entries = 0
        
        while True:
            # If we have a phase1_date and we've gone before it, break
            if phase1_date and current_date < phase1_date:
                break
                
            # Check if we have entries for this date
            if current_date in entries_by_date:
                day_entries = entries_by_date[current_date]
                days_without_entries = 0  # Reset counter when we find entries
                
                # Check if all symptoms are low (1 or 2) for all entries on this day
                all_symptoms_low = True
                for entry in day_entries:
                    if (entry['wind_level'] > 2 or 
                        entry['bloat_level'] > 2 or 
                        entry['pain_level'] > 2 or 
                        entry['stool_level'] > 2):
                        all_symptoms_low = False
                        break
                
                if all_symptoms_low:
                    streak_days += 1
                    current_date -= timedelta(days=1)
                else:
                    # If any symptom is high, streak ends
                    break
            else:
                # Count days without entries
                days_without_entries += 1
                
                # If no entries for two consecutive days, streak ends
                if days_without_entries >= 2:
                    break
                    
                # Otherwise continue checking the next day
                current_date -= timedelta(days=1)
        
        # Update phase_tracking with new streak days
        cursor.execute("""
            UPDATE phase_tracking 
            SET phase1_streak_days = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (streak_days, user_id))
        
        conn.commit()
        
        # Get updated phase tracking
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        updated_phase_tracking = cursor.fetchone()

        return JSONResponse(content={
            "message": "Phase 1 streak days updated successfully",
            "user_id": user_id,
            "telegram_id": telegram_id,
            "phase_tracking": row_to_dict(updated_phase_tracking),
            "current_streak_days": streak_days,
            "phase1_date_used": phase1_date.isoformat() if phase1_date else None
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_phase1_streak_days: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase 1 streak days: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase 1 streak days: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/users/{telegram_id}/phases-timings")
async def get_user_phases_timings(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get phases_timings for the user
        cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
        phases_timings_row = cursor.fetchone()
        if not phases_timings_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phases timings not found for user with telegram_id {telegram_id}"
            )

        return JSONResponse(content={
            "user_id": user_id,
            "telegram_id": telegram_id,
            "phases_timings": row_to_dict(phases_timings_row)
        })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_user_phases_timings: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting phases timings: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting phases timings: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/phases-timings/update-phase1-date")
async def update_phase1_date(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if phases_timings record exists for this user
        cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
        phases_timings_row = cursor.fetchone()
        
        current_time = datetime.now().isoformat()
        
        if not phases_timings_row:
            # Create new phases_timings record with current phase1_date
            logger.info(f"Creating new phases_timings record for user_id {user_id}")
            cursor.execute(
                "INSERT INTO phases_timings (user_id, phase1_date) VALUES (?, ?)",
                (user_id, current_time)
            )
            conn.commit()
            
            # Get the newly created record
            cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
            updated_phases_timings = cursor.fetchone()
            
            return JSONResponse(content={
                "message": "Phases timings created with phase1_date set to current time",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phases_timings": row_to_dict(updated_phases_timings)
            })
        else:
            # Update existing phases_timings record with current phase1_date
            logger.info(f"Updating phase1_date for user_id {user_id}")
            cursor.execute(
                "UPDATE phases_timings SET phase1_date = ? WHERE user_id = ?",
                (current_time, user_id)
            )
            conn.commit()
            
            # Get the updated record
            cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
            updated_phases_timings = cursor.fetchone()
            
            return JSONResponse(content={
                "message": "Phase1_date updated successfully",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phases_timings": row_to_dict(updated_phases_timings)
            })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_phase1_date: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase1_date: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase1_date: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.put("/users/{telegram_id}/phases-timings/update-phase2-date")
async def update_phase2_date(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if phases_timings record exists for this user
        cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
        phases_timings_row = cursor.fetchone()
        
        current_time = datetime.now().isoformat()
        
        if not phases_timings_row:
            # Create new phases_timings record with current phase2_date
            logger.info(f"Creating new phases_timings record for user_id {user_id}")
            cursor.execute(
                "INSERT INTO phases_timings (user_id, phase2_date) VALUES (?, ?)",
                (user_id, current_time)
            )
            conn.commit()
            
            # Get the newly created record
            cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
            updated_phases_timings = cursor.fetchone()
            
            return JSONResponse(content={
                "message": "Phases timings created with phase2_date set to current time",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phases_timings": row_to_dict(updated_phases_timings)
            })
        else:
            # Update existing phases_timings record with current phase2_date
            logger.info(f"Updating phase2_date for user_id {user_id}")
            cursor.execute(
                "UPDATE phases_timings SET phase2_date = ? WHERE user_id = ?",
                (current_time, user_id)
            )
            conn.commit()
            
            # Get the updated record
            cursor.execute("SELECT * FROM phases_timings WHERE user_id = ?", (user_id,))
            updated_phases_timings = cursor.fetchone()
            
            return JSONResponse(content={
                "message": "Phase2_date updated successfully",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phases_timings": row_to_dict(updated_phases_timings)
            })
    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_phase2_date: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase2_date: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase2_date: {e}"
        )
    finally:
        if conn:
            conn.close()

class Phase2TrackingUpdate(BaseModel):
    fructose: Optional[int] = Field(None, ge=0, description="Fructose tracking status")
    lactose: Optional[int] = Field(None, ge=0, description="Lactose tracking status")
    mannitol: Optional[int] = Field(None, ge=0, description="Mannitol tracking status")
    sorbitol: Optional[int] = Field(None, ge=0, description="Sorbitol tracking status")
    gos: Optional[int] = Field(None, ge=0, description="GOS tracking status")
    fructan: Optional[int] = Field(None, ge=0, description="Fructan tracking status")
    current_group: Optional[str] = Field(None, description="Current FODMAP group being tested")

class Phase2TrackingResponse(BaseModel):
    phase2_tracking_id: int
    user_id: int
    fructose: int
    lactose: int
    mannitol: int
    sorbitol: int
    gos: int
    fructan: int
    current_group: Optional[str]
    created_at: datetime
    updated_at: datetime

@app.put("/users/{telegram_id}/phase2-tracking", response_model=Phase2TrackingResponse)
async def update_or_create_phase2_tracking(telegram_id: str, update_data: Phase2TrackingUpdate):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Check if phase2_tracking exists for this user
        cursor.execute("SELECT * FROM phase2_tracking WHERE user_id = ?", (user_id,))
        phase2_tracking_row = cursor.fetchone()
        
        update_fields = update_data.model_dump(exclude_unset=True)
        
        if not phase2_tracking_row:
            # Create new phase2_tracking record
            logger.info(f"Creating new phase2_tracking record for user_id {user_id}")
            
            # Prepare columns and values for INSERT
            columns = ["user_id"]
            values = [user_id]
            placeholders = ["?"]
            
            for key, value in update_fields.items():
                columns.append(key)
                values.append(value)
                placeholders.append("?")
            
            sql = f"INSERT INTO phase2_tracking ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            # Get the newly created record
            cursor.execute("SELECT * FROM phase2_tracking WHERE user_id = ?", (user_id,))
            new_phase2_tracking_row = cursor.fetchone()
            
            if not new_phase2_tracking_row:
                logger.error(f"Failed to retrieve phase2_tracking record for user_id {user_id} after insertion.")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to retrieve phase2_tracking record after creation."
                )
            
            created_record_dict = row_to_dict(new_phase2_tracking_row)
            logger.info(f"Successfully created phase2_tracking for user_id {user_id}: {created_record_dict}")
            
            return Phase2TrackingResponse(**created_record_dict)
        else:
            # Update existing phase2_tracking record
            if not update_fields:
                # If no update fields provided, just return current data
                return Phase2TrackingResponse(**row_to_dict(phase2_tracking_row))
            
            # Build and execute update query
            set_clauses = [f"{key} = ?" for key in update_fields.keys()]
            sql = f"UPDATE phase2_tracking SET {', '.join(set_clauses)} WHERE user_id = ?"
            values = list(update_fields.values()) + [user_id]
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            # Get updated phase2_tracking
            cursor.execute("SELECT * FROM phase2_tracking WHERE user_id = ?", (user_id,))
            updated_phase2_tracking_row = cursor.fetchone()
            
            updated_record_dict = row_to_dict(updated_phase2_tracking_row)
            logger.info(f"Successfully updated phase2_tracking for user_id {user_id}: {updated_record_dict}")
            
            return Phase2TrackingResponse(**updated_record_dict)
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_or_create_phase2_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase2_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase2_tracking: {e}"
        )
    finally:
        if conn:
            conn.close()

@app.get("/users/{telegram_id}/phase2-tracking", response_model=Phase2TrackingResponse)
async def get_phase2_tracking(telegram_id: str):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get phase2_tracking information
        cursor.execute("SELECT * FROM phase2_tracking WHERE user_id = ?", (user_id,))
        phase2_tracking_row = cursor.fetchone()
        if not phase2_tracking_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase2 tracking not found for user with telegram_id {telegram_id}"
            )

        phase2_tracking_dict = row_to_dict(phase2_tracking_row)
        return Phase2TrackingResponse(**phase2_tracking_dict)

    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_phase2_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting phase2_tracking: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting phase2_tracking: {e}"
        )
    finally:
        if conn:
            conn.close()

class UpdatePhase2StreakRequest(BaseModel):
    timezone: str = Field(..., description="User's timezone in IANA format, e.g. 'Europe/London'")

@app.put("/users/{telegram_id}/phase-tracking/update-phase2-streak")
async def update_phase2_streak_days(telegram_id: str, request: UpdatePhase2StreakRequest):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get user_id from telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with telegram_id {telegram_id} not found"
            )
        user_id = user_row['id']

        # Get the user's timezone
        try:
            user_timezone = pytz.timezone(request.timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid timezone: {request.timezone}"
            )

        # Get current date in user's timezone
        now = datetime.now(user_timezone)
        current_date = now.date()
        
        # Get phase2_date as our absolute starting point
        cursor.execute("SELECT phase2_date FROM phases_timings WHERE user_id = ?", (user_id,))
        phase_timing_row = cursor.fetchone()
        
        if not phase_timing_row or not phase_timing_row['phase2_date']:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase2 date not found for user with telegram_id {telegram_id}. Please set phase2_date first."
            )
            
        # Convert phase2_date to user's timezone
        phase2_date_str = phase_timing_row['phase2_date']
        phase2_dt = datetime.fromisoformat(phase2_date_str.replace('Z', '+00:00'))
        phase2_dt = phase2_dt.astimezone(user_timezone)
        phase2_date = phase2_dt.date()
        
        # Calculate total days since phase2 started
        total_days_since_phase2 = (current_date - phase2_date).days
        if total_days_since_phase2 < 0:
            total_days_since_phase2 = 0
        
        # Get phase_tracking data
        cursor.execute("SELECT * FROM phase_tracking WHERE user_id = ?", (user_id,))
        phase_tracking_row = cursor.fetchone()
        if not phase_tracking_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Phase tracking not found for user with telegram_id {telegram_id}"
            )
        
        phase_tracking = row_to_dict(phase_tracking_row)
        current_phase2_reintroduction_days = phase_tracking['phase2_reintroduction_days'] or 0
        phase2_break_days = phase_tracking['phase2_break_days'] or 0
        
        # Calculate new reintroduction days (maximum 3)
        new_reintroduction_days = min(3, total_days_since_phase2)
        
        # 2. Check if phase2_reintroduction_days should be 3 or more
        if new_reintroduction_days >= 3:
            # If already 3, set to 3
            if current_phase2_reintroduction_days != 3:
                cursor.execute("""
                    UPDATE phase_tracking 
                    SET phase2_reintroduction_days = 3, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """, (user_id,))
                conn.commit()
            
            # NEW STEP: Check symptoms during the reintroduction period
            # Get phase2_tracking data to check current_group and last update
            cursor.execute("SELECT * FROM phase2_tracking WHERE user_id = ?", (user_id,))
            phase2_tracking_row = cursor.fetchone()
            
            if phase2_tracking_row:
                phase2_tracking_data = row_to_dict(phase2_tracking_row)
                current_group = phase2_tracking_data.get('current_group')
                
                # Only proceed if we have a current_group being tested
                if current_group:
                    # Get the updated_at time from phase2_tracking
                    updated_at_str = phase2_tracking_data['updated_at']
                    updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
                    updated_at = updated_at.astimezone(user_timezone)
                    reintro_start_date = updated_at.date()
                    
                    # Calculate end date for 3-day reintroduction period
                    reintro_end_date = reintro_start_date + timedelta(days=3)
                    
                    # Get symptoms diary entries during the reintroduction period
                    reintro_start_iso = reintro_start_date.isoformat()
                    reintro_end_iso = reintro_end_date.isoformat()
                    
                    cursor.execute("""
                        SELECT 
                            diary_id,
                            wind_level,
                            bloat_level,
                            pain_level,
                            stool_level,
                            created_at
                        FROM symptoms_diary 
                        WHERE user_id = ? 
                        AND date(created_at) >= date(?) 
                        AND date(created_at) <= date(?)
                    """, (user_id, reintro_start_iso, reintro_end_iso))
                    
                    symptom_entries = [row_to_dict(row) for row in cursor.fetchall()]
                    
                    # Check if any symptom is greater than 2
                    high_symptoms_found = False
                    for entry in symptom_entries:
                        if (entry['wind_level'] > 2 or 
                            entry['bloat_level'] > 2 or 
                            entry['pain_level'] > 2 or 
                            entry['stool_level'] > 2):
                            high_symptoms_found = True
                            break
                    
                    # Update the current FODMAP group in phase2_tracking
                    fodmap_value = 3 if high_symptoms_found else 2
                    
                    # Build the SQL update based on which group is being tested
                    if current_group in ['fructose', 'lactose', 'mannitol', 'sorbitol', 'gos', 'fructan']:
                        cursor.execute(f"""
                            UPDATE phase2_tracking 
                            SET {current_group} = ?, current_group = NULL, updated_at = CURRENT_TIMESTAMP
                            WHERE user_id = ?
                        """, (fodmap_value, user_id))
                        conn.commit()
                        
                        logger.info(f"Updated phase2_tracking for {current_group} with value {fodmap_value} based on symptoms check and cleared current_group")
            
            # 3.2 Check phase2_break_days
            if phase2_break_days == 3:
                # Break is already complete, no need to update
                return JSONResponse(content={
                    "message": "Phase 2 break already complete (3 days)",
                    "user_id": user_id,
                    "telegram_id": telegram_id,
                    "phase2_reintroduction_days": 3,
                    "phase2_break_days": phase2_break_days,
                    "days_since_phase2": total_days_since_phase2
                })
            
            # For break days, get the last day we updated phase2_tracking
            cursor.execute("SELECT updated_at FROM phase2_tracking WHERE user_id = ?", (user_id,))
            phase2_tracking_row = cursor.fetchone()
            
            if not phase2_tracking_row:
                # If no phase2_tracking record, create one
                cursor.execute("""
                    INSERT INTO phase2_tracking (user_id)
                    VALUES (?)
                """, (user_id,))
                conn.commit()
                
                # Set starting_date to phase2_date
                starting_date = phase2_date
            else:
                # Convert updated_at to user's timezone for food note checks
                updated_at_str = phase2_tracking_row['updated_at']
                updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
                updated_at = updated_at.astimezone(user_timezone)
                starting_date = updated_at.date()
            
            # Check food notes since starting point
            starting_date_iso = starting_date.isoformat()
            cursor.execute("""
                SELECT fn.note_id, fn.created_at, fn.food_list_id
                FROM food_notes fn
                WHERE fn.user_id = ? AND date(fn.created_at) >= date(?)
                ORDER BY fn.created_at ASC
            """, (user_id, starting_date_iso))
            
            food_notes = [row_to_dict(row) for row in cursor.fetchall()]
            
            # Group notes by date
            notes_by_date = {}
            for note in food_notes:
                note_dt = datetime.fromisoformat(note['created_at'].replace('Z', '+00:00'))
                note_dt = note_dt.astimezone(user_timezone)
                note_date = note_dt.date()
                
                if note_date not in notes_by_date:
                    notes_by_date[note_date] = []
                notes_by_date[note_date].append(note)
            
            # Check consecutive days with only low FODMAP foods
            consecutive_days = 0
            max_consecutive_days = 0
            date_to_check = starting_date
            
            while date_to_check <= current_date:
                if date_to_check in notes_by_date:
                    day_notes = notes_by_date[date_to_check]
                    high_fodmap_found = False
                    
                    # Check all food items in all notes for this day
                    for note in day_notes:
                        cursor.execute("""
                            SELECT 
                                uli.food_id,
                                uli.user_created_id,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT fructose_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT fructose_level FROM product WHERE product_id = uli.food_id)
                                END as fructose_level,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT lactose_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT lactose_level FROM product WHERE product_id = uli.food_id)
                                END as lactose_level,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT fructan_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT fructan_level FROM product WHERE product_id = uli.food_id)
                                END as fructan_level,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT mannitol_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT mannitol_level FROM product WHERE product_id = uli.food_id)
                                END as mannitol_level,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT sorbitol_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT sorbitol_level FROM product WHERE product_id = uli.food_id)
                                END as sorbitol_level,
                                CASE 
                                    WHEN uli.user_created_id IS NOT NULL THEN 
                                        (SELECT gos_level FROM user_products WHERE user_product_id = uli.food_id)
                                    ELSE 
                                        (SELECT gos_level FROM product WHERE product_id = uli.food_id)
                                END as gos_level
                            FROM user_list_item uli
                            WHERE uli.list_id = ?
                        """, (note['food_list_id'],))
                        
                        food_items = cursor.fetchall()
                        
                        for item in food_items:
                            is_user_created = item['user_created_id'] is not None
                            
                            # For each food item, check if it has high FODMAP levels
                            # For standard products: level > 1 means high FODMAP
                            # For user products: level < 2 means high FODMAP (0=high, 1=medium, 2=low)
                            if is_user_created:
                                if (item['fructose_level'] < 2 or 
                                    item['lactose_level'] < 2 or
                                    item['fructan_level'] < 2 or
                                    item['mannitol_level'] < 2 or
                                    item['sorbitol_level'] < 2 or
                                    item['gos_level'] < 2):
                                    high_fodmap_found = True
                                    break
                            else:
                                if (item['fructose_level'] > 1 or 
                                    item['lactose_level'] > 1 or
                                    item['fructan_level'] > 1 or
                                    item['mannitol_level'] > 1 or
                                    item['sorbitol_level'] > 1 or
                                    item['gos_level'] > 1):
                                    high_fodmap_found = True
                                    break
                        
                        if high_fodmap_found:
                            break
                    
                    if high_fodmap_found:
                        # Reset streak if high FODMAP food found
                        consecutive_days = 0
                    else:
                        # Increment streak for day with only low FODMAP foods
                        consecutive_days += 1
                        max_consecutive_days = max(max_consecutive_days, consecutive_days)
                else:
                    # No food notes for this day, can't determine, reset streak
                    consecutive_days = 0
                
                date_to_check += timedelta(days=1)
            
            # Update phase2_break_days in phase_tracking
            new_break_days = min(3, max_consecutive_days)
            
            if new_break_days != phase2_break_days:
                cursor.execute("""
                    UPDATE phase_tracking 
                    SET phase2_break_days = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """, (new_break_days, user_id))
                conn.commit()
            
            # Update phase2_tracking updated_at to current time
            cursor.execute("""
                UPDATE phase2_tracking
                SET updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (user_id,))
            conn.commit()
            
            return JSONResponse(content={
                "message": f"Phase 2 break days updated to {new_break_days}",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phase2_reintroduction_days": 3,
                "phase2_break_days": new_break_days,
                "days_since_phase2": total_days_since_phase2,
                "max_consecutive_days": max_consecutive_days
            })
        else:
            # 3.1 Update phase2_reintroduction_days based on absolute days since phase2 started
            if new_reintroduction_days != current_phase2_reintroduction_days:
                cursor.execute("""
                    UPDATE phase_tracking 
                    SET phase2_reintroduction_days = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """, (new_reintroduction_days, user_id))
                conn.commit()
            
            # Update phase2_tracking updated_at to current time
            cursor.execute("""
                UPDATE phase2_tracking
                SET updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (user_id,))
            conn.commit()
            
            return JSONResponse(content={
                "message": f"Phase 2 reintroduction days updated to {new_reintroduction_days}",
                "user_id": user_id,
                "telegram_id": telegram_id,
                "phase2_reintroduction_days": new_reintroduction_days,
                "phase2_break_days": phase2_break_days,
                "days_since_phase2": total_days_since_phase2
            })
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error in update_phase2_streak_days: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating phase 2 streak days: {e}", exc_info=True)
        if conn: conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating phase 2 streak days: {e}"
        )
    finally:
        if conn:
            conn.close()