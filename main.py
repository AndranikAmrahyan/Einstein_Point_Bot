import logging
import sqlite3
import threading
import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from flask import Flask
from telegram import Update, Bot
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters
)

# Загрузка переменных окружения
load_dotenv()

# Настройка логгера
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Конфигурация
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    RENDER_APP_URL = os.getenv("RENDER_APP_URL")
    DB_NAME = "points_bot.db"
    ALLOWED_CHAT_IDS = [-1002157100033, -1002439723121]
    ALLOWED_CHAT_USERNAME = "Family_Worlds"
    CREATOR = "tg://openmessage?user_id=5553779390"

# Инициализация Flask
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Telegram Bot is running!"

@app_flask.route("/ping")
def ping():
    return "pong", 200

def run_web_server():
    app_flask.run(host="0.0.0.0", port=8080)

# Инициализация БД
def init_db():
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER, 
                  chat_id INTEGER, 
                  points INTEGER, 
                  username TEXT,
                  UNIQUE(user_id, chat_id))''')  # Добавлен уникальный индекс
    conn.commit()
    conn.close()

init_db()

# Функции работы с БД
def get_user_points(user_id: int, chat_id: int) -> int:
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute("SELECT points FROM users WHERE user_id=? AND chat_id=?", (user_id, chat_id))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 0

def update_user_points(user_id: int, chat_id: int, delta: int, username: str):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    
    # Обновляем существующую запись или создаем новую
    c.execute('''INSERT OR REPLACE INTO users 
                 (user_id, chat_id, points, username)
                 VALUES (?, ?, 
                     COALESCE((SELECT points FROM users WHERE user_id=? AND chat_id=?), 0) + ?, 
                     ?)''',
                 (user_id, chat_id, user_id, chat_id, delta, username))
    
    conn.commit()
    conn.close()

def get_top_users(chat_id: int, limit: int = 10):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('''SELECT username, points 
                 FROM users 
                 WHERE chat_id=?
                 GROUP BY user_id  -- Группируем по пользователю
                 ORDER BY points DESC 
                 LIMIT ?''', (chat_id, limit))
    result = c.fetchall()
    conn.close()
    return result

# Проверка прав модератора
async def is_moderator(user_id: int, chat_id: int, bot: Bot) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking moderator status: {e}")
        return False

# Общая функция для изменения баллов
async def modify_points(update: Update, context: ContextTypes.DEFAULT_TYPE, operation: str):
    if not await is_moderator(update.effective_user.id, 
                             update.effective_chat.id, 
                             context.bot):
        await update.message.reply_text("❌ Только модераторы могут использовать эту команду")
        return

    try:
        if update.message.reply_to_message:
            target_user = update.message.reply_to_message.from_user
            points = int(context.args[0])
            
            # Добавляем пользователя в базу при ответе на сообщение
            update_user_points(
                user_id=target_user.id,
                chat_id=update.effective_chat.id,
                delta=0,
                username=target_user.username or target_user.full_name
            )
        else:
            if len(context.args) < 2:
                raise ValueError("Недостаточно аргументов")
                
            points = int(context.args[0])
            mention = context.args[1]
            
            if not mention.startswith('@'):
                raise ValueError("Некорректное упоминание")
            
            username = mention.lstrip('@')
            target_user = await resolve_mention(
                context.bot,
                chat_id=update.effective_chat.id,
                username=username
            )

        # Применяем изменения
        final_points = points if operation == 'add' else -points
        update_user_points(
            user_id=target_user.id,
            chat_id=update.effective_chat.id,
            delta=final_points,
            username=target_user.username or target_user.full_name
        )
        
        await update.message.reply_text(
            f"✅ Пользователю @{target_user.username or target_user.full_name} "
            f"{'добавлено' if operation == 'add' else 'снято'} {abs(points)} баллов"
        )

    except ValueError as e:
        if "User not found" in str(e):
            await update.message.reply_text("❌ Пользователь не найден. Ответьте на его сообщение, чтобы добавить в систему")
        elif "Недостаточно аргументов" in str(e):
            await update.message.reply_text("❌ Формат: /команда [число] @юзер")
        elif "Некорректное упоминание" in str(e):
            await update.message.reply_text("❌ Используйте правильный формат @username")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await update.message.reply_text("❌ Произошла ошибка при выполнении команды")

# Обработка упоминаний
async def resolve_mention(bot: Bot, chat_id: int, username: str):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE username=?", (username,))
    result = c.fetchone()
    conn.close()
    
    if not result:
        raise ValueError("User not found")
    
    user_id = result[0]
    try:
        chat_member = await bot.get_chat_member(chat_id, user_id)
        return chat_member.user
    except BadRequest as e:
        logger.error(f"Error getting chat member: {e}")
        raise ValueError("User not found")

async def add_points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await modify_points(update, context, 'add')

async def remove_points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await modify_points(update, context, 'remove')

async def my_points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    points = get_user_points(
        update.effective_user.id,
        update.effective_chat.id
    )
    await update.message.reply_text(
        f"🏆 Ваши баллы: {points}"
    )
    
async def check_points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        target_user = None
        
        # Проверяем ответ на сообщение
        if update.message.reply_to_message:
            target_user = update.message.reply_to_message.from_user
        # Проверяем упоминание в аргументах
        elif context.args and context.args[0].startswith('@'):
            username = context.args[0].lstrip('@')
            target_user = await resolve_mention(
                context.bot,
                chat_id=update.effective_chat.id,
                username=username
            )
        
        if not target_user:
            await update.message.reply_text("❌ Ответьте на сообщение пользователя или укажите @username")
            return

        # Проверяем наличие пользователя в базе
        conn = sqlite3.connect(Config.DB_NAME)
        c = conn.cursor()
        c.execute("SELECT 1 FROM users WHERE user_id=? AND chat_id=?", 
                 (target_user.id, update.effective_chat.id))
        exists = c.fetchone()
        conn.close()
        
        if not exists:
            await update.message.reply_text("ℹ️ Пользователь еще не имеет баллов в этом чате")
            return

        # Получаем и отображаем баллы
        points = get_user_points(target_user.id, update.effective_chat.id)
        await update.message.reply_text(
            f"🏆 Пользователь {target_user.mention_markdown()} имеет *{points}* баллов",
            parse_mode="Markdown"
        )

    except ValueError as e:
        if "User not found" in str(e):
            await update.message.reply_text("ℹ️ Пользователь еще не имеет баллов в этом чате")  # "❌ Пользователь не найден в системе"
        else:
            await update.message.reply_text(
                "❌ Неверный формат команды.\n"
                "Доступные варианты:\n"
                "1. Ответьте на сообщение пользователя и отправьте /points\n"
                "2. Или отправьте: /points @username"
            )
    except Exception as e:
        logger.error(f"Error in check_points: {e}")
        await update.message.reply_text("❌ Ошибка при проверке баллов")

async def top_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(context.args[0]) if context.args else 10
        limit = min(limit, 50)
    except ValueError:
        limit = 10
    
    top = get_top_users(update.effective_chat.id, limit)
    if not top:
        await update.message.reply_text("📊 Рейтинг пока пуст")
        return
        
    response = f"🏆 Топ {limit} пользователей:\n" + "\n".join(
        [f"{i+1}. {user[0]} - {user[1]} баллов" for i, user in enumerate(top)]
    )
    await update.message.reply_text(response)
    
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "📚 *Доступные команды:*\n\n"
        "➕ Добавить баллы (модераторы):\n"
        "`/add [кол-во] @юзер` или ответ на сообщение\n\n"
        "➖ Снять баллы (модераторы):\n"
        "`/remove [кол-во] @юзер` или ответ на сообщение\n\n"
        "🏆 Мои баллы:\n"
        "`/my`\n\n"
        "🔍 Проверить баллы другого:\n"
        "`/points @юзер` или ответ на сообщение\n\n"
        "🏅 Топ пользователей:\n"
        "`/top [число]` (по умолчанию 10)\n\n"
        "🆘 Помощь:\n"
        "`/help`\n\n"
        f"⚡️ Бот создан специально для чата @{Config.ALLOWED_CHAT_USERNAME}\n"
        f"👨💻 Создатель бота: [Слоняра]({Config.CREATOR})"
    )
    
    await update.message.reply_text(
        help_text,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

# Самопингование
async def self_ping():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{Config.RENDER_APP_URL}/ping") as resp:
                    logger.info(f"Self-ping status: {resp.status}")
        except Exception as e:
            logger.error(f"Self-ping error: {str(e)}")
        await asyncio.sleep(300)

# Добавляем обработчик для новых чатов
async def handle_new_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.bot.id in [user.id for user in update.message.new_chat_members]:
        chat_id = update.effective_chat.id
        if chat_id not in Config.ALLOWED_CHAT_IDS:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🚫 Бот создан специально для чата @{Config.ALLOWED_CHAT_USERNAME}\n"
                    f"Бота создал: {Config.CREATOR}"
                )
            )
            await context.bot.leave_chat(chat_id)

def main():
    # Запуск Flask в отдельном потоке
    threading.Thread(target=run_web_server, daemon=True).start()

    # Создание и настройка бота
    application = ApplicationBuilder().token(Config.BOT_TOKEN).build()
    
    # Фильтр для разрешенных чатов (используем встроенный фильтр)
    allowed_chat = filters.Chat(chat_id=Config.ALLOWED_CHAT_IDS)
    
    # Обработчик для новых чатов
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat)
    )

    # Регистрация обработчиков с применением фильтра
    application.add_handler(CommandHandler("help", help_command, filters=allowed_chat))
    application.add_handler(CommandHandler("add", add_points, filters=allowed_chat))
    application.add_handler(CommandHandler("remove", remove_points, filters=allowed_chat))
    application.add_handler(CommandHandler("my", my_points, filters=allowed_chat))
    application.add_handler(CommandHandler("points", check_points, filters=allowed_chat))
    application.add_handler(CommandHandler("top", top_users, filters=allowed_chat))

    # Запуск самопингования
    application.create_task(self_ping())

    # Запуск бота
    application.run_polling()

if __name__ == "__main__":
    main()
