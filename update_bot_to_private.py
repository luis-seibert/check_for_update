import json
from os import environ

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN: str = environ.get("BOT_TOKEN", "")
USER_IDS: str = environ.get("MY_USER_ID", "")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the bot and check if the user is authorized."""
    # Check if the user ID matches

    user_ids = json.loads(USER_IDS)
    if (
        update.message
        and update.message.from_user
        and update.message.from_user.id in user_ids
    ):
        await update.message.reply_text("Hello, this bot is only for you!")
    else:
        if update.message:
            await update.message.reply_text(
                "Sorry, you are not authorized to use this bot."
            )


def main():
    """Start the bot and set up the command handler."""
    # Create the application and pass the bot token
    application = Application.builder().token(BOT_TOKEN).build()

    # Add the command handler for the '/start' command
    application.add_handler(CommandHandler("start", start))

    # Start the bot
    application.run_polling()


if __name__ == "__main__":
    main()
