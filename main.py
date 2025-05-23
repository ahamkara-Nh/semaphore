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
from datetime import datetime
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