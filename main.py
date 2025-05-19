import os
import hmac
import hashlib
import json
from urllib.parse import unquote, parse_qs

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

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
async def auth_telegram(payload: TelegramInitData):
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
    # if user_info_json:
    #     user_info = json.loads(user_info_json)
    #     # Process user_info, e.g., create or update user in DB
    #     return JSONResponse(content={"message": "Authentication successful", "user": user_info})

    return JSONResponse(content={"message": "Authentication successful"})