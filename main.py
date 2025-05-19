import os
import hmac
import hashlib
import json
from urllib.parse import unquote, parse_qs
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, status, Depends
from sqlalchemy.orm import Session
from database import SessionLocal, engine, User, create_db_and_tables, get_db
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the ML model
    create_db_and_tables()
    yield
    # Clean up the ML models and release the resources

app = FastAPI(lifespan=lifespan)

BOT_TOKEN = os.getenv("BOT_TOKEN")

class TelegramInitData(BaseModel):
    initData: str

def validate_telegram_data(init_data_str: str, bot_token: str) -> bool:
    try:
        # The initData is URL-encoded, so we need to decode it first
        # and then parse it as a query string.
        # Sometimes it might be already partially decoded by frameworks, so try both.
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
                # Values from parse_qs are lists, take the first element
                data_check_string_parts.append(f"{key}={value[0]}")

        data_check_string = "\n".join(data_check_string_parts)

        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        return calculated_hash == hash_received
    except Exception as e:
        print(f"Error validating Telegram data: {e}") # For debugging
        return False

@app.get("/")
async def root():
    return {"message": "Hello World - Backend is running!"}

@app.post("/auth/telegram")
async def auth_telegram(payload: TelegramInitData, db: Session = Depends(get_db)):
    if not BOT_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="BOT_TOKEN is not configured on the server."
        )

    is_valid = validate_telegram_data(payload.initData, BOT_TOKEN)

    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Telegram initialization data."
        )
    
    # If valid, you can parse user data from initData
    # For example, to get user info:
    # init_data_params = parse_qs(unquote(payload.initData))
    # user_info_json = init_data_params.get('user', [None])[0]
    # If valid, parse user data from initData
    try:
        init_data_params = parse_qs(unquote(payload.initData))
        user_info_json = init_data_params.get('user', [None])[0]
        if not user_info_json:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User data not found in Telegram initialization data."
            )
        user_info = json.loads(user_info_json)
        telegram_id = str(user_info.get("id"))

        if not telegram_id:
             raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Telegram user ID not found in user data."
            )

        # Check if user exists
        db_user = db.query(User).filter(User.telegram_id == telegram_id).first()

        if not db_user:
            # Create new user
            new_user = User(
                telegram_id=telegram_id,
                # onboarding_completed will default to False as per model
            )
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            return JSONResponse(content={"message": "Authentication successful, new user created.", "user_id": new_user.id, "telegram_id": new_user.telegram_id})
        else:
            # User exists, update updated_at (SQLAlchemy handles this if onupdate is set)
            # db.commit() # To trigger onupdate if there were other changes
            return JSONResponse(content={"message": "Authentication successful, user exists.", "user_id": db_user.id, "telegram_id": db_user.telegram_id})

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON in user data."
        )
    except Exception as e:
        # Log the exception e for debugging
        print(f"Error processing user data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing user data."
        )

@app.put("/users/{telegram_id}/complete_onboarding")
async def complete_onboarding(telegram_id: str, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.telegram_id == telegram_id).first()
    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with telegram_id {telegram_id} not found"
        )
    
    if db_user.onboarding_completed:
        return JSONResponse(content={"message": f"User {telegram_id} onboarding already completed.", "user_id": db_user.id, "telegram_id": db_user.telegram_id})

    db_user.onboarding_completed = True
    # updated_at should be handled by SQLAlchemy's onupdate
    db.commit()
    db.refresh(db_user)
    return JSONResponse(content={"message": f"User {telegram_id} onboarding completed successfully.", "user_id": db_user.id, "telegram_id": db_user.telegram_id, "onboarding_completed": db_user.onboarding_completed})