import os
import logging
import sqlite3
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from database import get_db_connection

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Get bot token from environment variable
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not found in environment variables")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a welcome message when the command /start is issued."""
    keyboard = [
        [InlineKeyboardButton("Настройки уведомлений", callback_data="notification_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Добро пожаловать в приложение для отслеживания low-fodmap диеты!",
        reply_markup=reply_markup
    )

async def get_user_preferences(telegram_id):
    """Get user preferences from database."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First check if user exists, create if not
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user = cursor.fetchone()
        
        if not user:
            # Create user
            cursor.execute("INSERT INTO users (telegram_id) VALUES (?)", (telegram_id,))
            conn.commit()
            user_id = cursor.lastrowid
            
            # Create default preferences
            cursor.execute("""
                INSERT INTO user_preferences (user_id, daily_reminders, update_notifications) 
                VALUES (?, TRUE, TRUE)
            """, (user_id,))
            conn.commit()
            
            return {"daily_reminders": True, "update_notifications": True}
        
        # Get preferences
        user_id = user['id']
        cursor.execute("""
            SELECT daily_reminders, update_notifications 
            FROM user_preferences 
            WHERE user_id = ?
        """, (user_id,))
        
        prefs = cursor.fetchone()
        
        if not prefs:
            # Create default preferences if they don't exist
            cursor.execute("""
                INSERT INTO user_preferences (user_id, daily_reminders, update_notifications) 
                VALUES (?, TRUE, TRUE)
            """, (user_id,))
            conn.commit()
            return {"daily_reminders": True, "update_notifications": True}
        
        return {
            "daily_reminders": bool(prefs['daily_reminders']),
            "update_notifications": bool(prefs['update_notifications'])
        }
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        # Return default values if database error
        return {"daily_reminders": True, "update_notifications": True}
    finally:
        if conn:
            conn.close()

async def update_user_preference(telegram_id, preference_name, value):
    """Update user preference in database."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get user id
        cursor.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        user = cursor.fetchone()
        
        if not user:
            return False
        
        user_id = user['id']
        
        # Update preference
        cursor.execute(f"""
            UPDATE user_preferences 
            SET {preference_name} = ? 
            WHERE user_id = ?
        """, (value, user_id))
        
        conn.commit()
        return True
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        if conn:
            conn.close()

async def show_notification_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show notification settings menu."""
    query = update.callback_query
    telegram_id = str(update.effective_user.id)
    
    # Get current preferences
    prefs = await get_user_preferences(telegram_id)
    
    # Create message text
    update_status = "включены" if prefs["update_notifications"] else "отключены"
    reminders_status = "включены" if prefs["daily_reminders"] else "отключены"
    
    message = f"Уведомления об обновлениях {update_status}\nНапоминания о записях в дневник {reminders_status}"
    
    # Create buttons
    update_text = "Выключить" if prefs["update_notifications"] else "Включить"
    reminders_text = "Выключить" if prefs["daily_reminders"] else "Включить"
    
    keyboard = [
        [InlineKeyboardButton(f"{update_text} уведомления об обновлениях", 
                              callback_data=f"toggle_updates_{not prefs['update_notifications']}")],
        [InlineKeyboardButton(f"{reminders_text} напоминания о записях в дневнике", 
                              callback_data=f"toggle_reminders_{not prefs['daily_reminders']}")],
        [InlineKeyboardButton("Назад", callback_data="back_to_start")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text=message, reply_markup=reply_markup)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    telegram_id = str(update.effective_user.id)
    await query.answer()
    
    if query.data == "notification_settings":
        await show_notification_settings(update, context)
    
    elif query.data == "back_to_start":
        # Go back to start screen
        keyboard = [
            [InlineKeyboardButton("Настройки уведомлений", callback_data="notification_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text="Добро пожаловать в приложение для отслеживания low-fodmap диеты!",
            reply_markup=reply_markup
        )
    
    elif query.data.startswith("toggle_updates_"):
        # Toggle update notifications
        new_value = query.data.split("_")[-1] == "True"
        success = await update_user_preference(telegram_id, "update_notifications", new_value)
        
        if success:
            await show_notification_settings(update, context)
        else:
            await query.edit_message_text(
                text="Произошла ошибка при обновлении настроек. Попробуйте позже."
            )
    
    elif query.data.startswith("toggle_reminders_"):
        # Toggle daily reminders
        new_value = query.data.split("_")[-1] == "True"
        success = await update_user_preference(telegram_id, "daily_reminders", new_value)
        
        if success:
            await show_notification_settings(update, context)
        else:
            await query.edit_message_text(
                text="Произошла ошибка при обновлении настроек. Попробуйте позже."
            )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all non-command messages."""
    await update.message.reply_text(
        "Не знаю такой команды :( Весь функционал смотрите в приложении и по команде /start"
    )

def main() -> None:
    """Start the bot."""
    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Add message handler for all other messages
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Run the bot until the user presses Ctrl-C
    logger.info("Starting bot...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
