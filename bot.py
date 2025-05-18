import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
MINI_APP_URL = os.getenv('MINI_APP_URL') # Replace with your actual mini app URL or set as env var

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a message with a button that opens the mini app."""
    keyboard = [
        [InlineKeyboardButton("Open Diet Tracker", web_app={'url': MINI_APP_URL})]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Hello! Welcome to the Low-FODMAP Diet Tracker Bot. Click the button below to open the app!",
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a placeholder help message."""
    await update.message.reply_text("This is a placeholder help message. I will edit this later.")

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Replies to unknown commands."""
    await update.message.reply_text(
        "Sorry, I didn't understand that command. Available commands are /start and /help."
    )

def main() -> None:
    """Start the bot."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return

    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))

    # on non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_command))

    # Run the bot until the user presses Ctrl-C
    logger.info("Bot started...")
    application.run_polling()

if __name__ == "__main__":
    main()