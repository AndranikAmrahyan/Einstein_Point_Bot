# НЕ ОЧИСТИТЬ КЭШ В RENDER - ПОТЕРЯЕШЬ ДАННЫЕ!
# Если ошибка о конфликте, нужно создать новый токен бота: Телеграм @BotFather - команда /revoke
# ТОЛЬКО для Render прямо в коде дать BOT_TOKEN

import logging
import sqlite3
import datetime
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
from telegram.helpers import escape_markdown
import signal
import sys
from functools import partial

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
    BOT_TOKEN = "7487925725:AAHzJyVWRG2fklT0hQvaXrq-Cawq9vzomEQ"  # os.getenv("BOT_TOKEN")
    RENDER_APP_URL = "https://einstein-point-bot.onrender.com"  # os.getenv("RENDER_APP_URL")
    DB_NAME = "points_bot.db"
    BACKUP_CHAT_ID = -1002571801416  # ID чата для бэкапов(сохранении данных) https://t.me/+Axwm80ZCBSc3NjQy
    ALLOWED_CHAT_IDS = [BACKUP_CHAT_ID, -1002157100033, -1002439723121]
    MAIN_ALLOWED_CHAT = "@Family_Worlds"  # -1002157100033
    CREATOR = "tg://openmessage?user_id=5553779390"
    ALLOWED_ADMINS = {
        # Формат: {chat_id: [user_id1, user_id2, ...]}
        -1002157100033: [5553779390, 1057267401, 2085350493, 1911958747, 5518327998],  # https://t.me/Family_Worlds
        -1002439723121: [5553779390]  # https://t.me/Einstein_bot_test_2
    }

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
                  full_name TEXT,
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

def update_user_points(user_id: int, chat_id: int, delta: int, username: str, full_name: str):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    
    c.execute('''INSERT OR REPLACE INTO users 
                 (user_id, chat_id, points, username, full_name)
                 VALUES (?, ?, 
                     COALESCE((SELECT points FROM users WHERE user_id=? AND chat_id=?), 0) + ?, 
                     ?, ?)''',
                 (user_id, chat_id, user_id, chat_id, delta, username, full_name))
    
    conn.commit()
    conn.close()

def get_top_users(chat_id: int, limit: int = 10):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, full_name, points 
                 FROM users 
                 WHERE chat_id=?
                 GROUP BY user_id
                 ORDER BY points DESC 
                 LIMIT ?''', (chat_id, limit))
    result = c.fetchall()
    conn.close()
    return result

async def export_chat_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт данных чата в виде сообщения"""
    if not await is_moderator(update.effective_user.id, 
                             update.effective_chat.id, 
                             context.bot):
        await update.message.reply_text("❌ Только модераторы могут использовать эту команду")
        return

    try:
        chat_id = update.effective_chat.id
        
        # Получаем данные только для текущего чата
        conn = sqlite3.connect(Config.DB_NAME)
        c = conn.cursor()
        c.execute('''SELECT user_id, username, full_name, points 
                     FROM users WHERE chat_id=?''', (chat_id,))
        users_data = c.fetchall()
        conn.close()
        
        if not users_data:
            await update.message.reply_text("❌ В этом чате нет данных для экспорта")
            return

        # Формируем данные для экспорта
        export_lines = []
        for user_id, username, full_name, points in users_data:
            # Экранируем специальные символы
            safe_username = (username or "").replace(':', '\\:')
            safe_full_name = (full_name or "").replace(':', '\\:')
            export_lines.append(f"{user_id}:{safe_username}:{safe_full_name}:{points}")
        
        export_text = "\n".join(export_lines)
        
        # Первое сообщение с инструкцией
        await update.message.reply_text(
            f"💾 Сохраните следующее сообщение для восстановления данных.\n"
            "Для восстановления просто отправьте сообщение в чат."
        )
        
        # Второе сообщение с данными
        await update.message.reply_text(
            f"/restore_data\n{export_text}"
        )
        
        # Отправляем копию в BACKUP_CHAT_ID
        try:
            chat_title = update.effective_chat.title or f"Чат {chat_id}"
            await context.bot.send_message(
                chat_id=Config.BACKUP_CHAT_ID,
                text=f"💾 Бэкап данных из чата '{chat_title}' ({chat_id})\n\n"
                     f"/restore_data\n{export_text}",
                disable_notification=True
            )
            logger.info(f"Данные чата {chat_id} отправлены в BACKUP_CHAT_ID")
        except Exception as backup_error:
            logger.error(f"Ошибка при отправке в BACKUP_CHAT_ID: {backup_error}")
            # Не прерываем выполнение, только логируем ошибку
        
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        await update.message.reply_text("❌ Ошибка при экспорте данных")

async def import_chat_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Импорт данных из сообщения"""
    if not await is_moderator(update.effective_user.id, 
                             update.effective_chat.id, 
                             context.bot):
        await update.message.reply_text("❌ Только модераторы могут использовать эту команду")
        return

    try:
        if not update.message.text or not update.message.text.startswith('/restore_data'):
            await update.message.reply_text("❌ Неверный формат данных для восстановления")
            return

        # Извлекаем данные из текущего сообщения
        lines = update.message.text.split('\n')
        if len(lines) < 2:
            await update.message.reply_text("❌ В сообщении нет данных для восстановления")
            return

        chat_id = update.effective_chat.id
        conn = sqlite3.connect(Config.DB_NAME)
        c = conn.cursor()
        
        # Удаляем старые данные для этого чата
        c.execute("DELETE FROM users WHERE chat_id=?", (chat_id,))
        
        # Восстанавливаем данные
        success_count = 0
        error_count = 0
        
        for line in lines[1:]:  # Пропускаем первую строку с командой
            if not line.strip():
                continue
                
            try:
                parts = line.split(':')
                if len(parts) != 4:
                    error_count += 1
                    continue
                
                user_id = int(parts[0])
                username = parts[1].replace('\\:', ':') if parts[1] else ""
                full_name = parts[2].replace('\\:', ':') if parts[2] else ""
                points = int(parts[3])
                
                # Вставляем данные
                c.execute('''INSERT OR REPLACE INTO users 
                             (user_id, chat_id, points, username, full_name)
                             VALUES (?, ?, ?, ?, ?)''',
                         (user_id, chat_id, points, username, full_name))
                success_count += 1
                
            except (ValueError, IndexError) as e:
                error_count += 1
                logger.warning(f"Error parsing line: {line}, error: {e}")
        
        conn.commit()
        conn.close()
        
        await update.message.reply_text(
            f"✅ Данные успешно восстановлены!\n"
            f"• Успешно импортировано: {success_count} записей\n"
            f"• Ошибок при импорте: {error_count}"
        )
        
    except Exception as e:
        logger.error(f"Error importing data: {e}")
        await update.message.reply_text("❌ Ошибка при восстановлении данных")

# Проверка прав модератора
async def is_moderator(user_id: int, chat_id: int, bot: Bot) -> bool:
    try:
        # Сначала проверяем белый список админов
        if chat_id in Config.ALLOWED_ADMINS:
            return user_id in Config.ALLOWED_ADMINS[chat_id]
        # Если чата нет в списке - проверяем статус в чате
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
                username=target_user.username or "",
                full_name=target_user.full_name
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
            username=target_user.username or "",
            full_name=target_user.full_name
        )
        
        user_link = target_user.mention_markdown()
        await update.message.reply_text(
            f"✅ Пользователю {user_link} "
            f"{'добавлено' if operation == 'add' else 'снято'} {abs(points)} баллов",
            parse_mode="Markdown"
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
        
    response = f"🏆 Топ {limit} пользователей:\n"
    lines = []
    
    for index, (user_id, username, full_name, points) in enumerate(top):
        try:
            # Получаем объект пользователя
            user = await context.bot.get_chat_member(update.effective_chat.id, user_id)
            # mention = user.user.mention_markdown()
            name = escape_markdown(user.user.full_name, version=2)  # version=1
            mention = f"[{name}](tg://openmessage?user_id={user.user.id})"  # tg://user?id=
        except BadRequest:
            # Если не удалось получить пользователя, используем сохраненное имя
            name_to_show = full_name or username or f"Пользователь {user_id}"
            mention = f"[{escape_markdown(name_to_show, version=2)}](tg://openmessage?user_id={user_id})"  # tg://user?id=  # version=1 # (tg://openmessage?user_id={user_id})
        except Exception as e:
            logger.error(f"Ошибка при получении пользователя в top_users: {e}")
            mention = f"[Пользователь {user_id}](tg://openmessage?user_id={user_id})"  # tg://user?id=
        
        # Экранируем все спецсимволы
        
        escaped_points = escape_markdown(str(points), version=2)
        
        # lines.append(f"{index + 1}. {mention} - *{points}* баллов")
        line = (
            f"{index + 1}\\. "  # Экранированная точка
            f"{mention} \\- "   # Экранированный дефис
            f"*{escaped_points}* баллов"  # Экранированные баллы
        )
        lines.append(line)

    response += "\n".join(lines)
    await update.message.reply_text(response, parse_mode="MarkdownV2")  # Markdown
    
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Экранируем все специальные символы
    creator_link = escape_markdown(Config.CREATOR, version=2)
    chat_mention = escape_markdown(Config.MAIN_ALLOWED_CHAT, version=2)
    
    help_text = (
        "📚 *Доступные команды:*\n\n"
        "➕ Добавить баллы \\(модераторы\\):\n"
        "`/add [кол\\-во] @юзер` или ответ на сообщение\n\n"
        "➖ Снять баллы \\(модераторы\\):\n"
        "`/remove [кол\\-во] @юзер` или ответ на сообщение\n\n"
        "🏆 Мои баллы:\n"
        "`/my`\n\n"
        "🔍 Проверить баллы другого:\n"
        "`/points @юзер` или ответ на сообщение\n\n"
        "🏅 Топ пользователей:\n"
        "`/top [число]` \\(по умолчанию 10\\)\n\n"
        "🆘 Помощь:\n"
        "`/help`\n\n"
        f"⚡️ Бот создан специально для чата {chat_mention}\n"
        f"👨💻 Создатель бота: [Слоняра]({creator_link})"
    )
    
    await update.message.reply_text(
        help_text,
        parse_mode="MarkdownV2",
        disable_web_page_preview=True
    )

# Добавляем обработчик для новых чатов
async def handle_new_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.bot.id in [user.id for user in update.message.new_chat_members]:
        chat_id = update.effective_chat.id
        if chat_id not in Config.ALLOWED_CHAT_IDS:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🚫 Бот создан специально для чата {Config.MAIN_ALLOWED_CHAT}\n"
                    f"Бота создал: {Config.CREATOR}"
                )
            )
            await context.bot.leave_chat(chat_id)
        else:
            await help_command(update, context)

# Самопингование
async def self_ping(context):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{Config.RENDER_APP_URL}/ping") as resp:
                logger.info(f"Self-ping status: {resp.status}")
    except Exception as e:
        logger.error(f"Self-ping error: {str(e)}")

# Обработчик остановки
async def shutdown(application):
    logger.info("Starting graceful shutdown...")
    application.job_queue.stop()  # Останавливаем все задачи
    await application.stop()
    await application.shutdown()
    logger.info("Application stopped successfully")

def handle_signal(application, loop, signal_name):
    logger.info(f"Received {signal_name} signal")
    loop.create_task(shutdown(application))

async def send_db_backup(context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info("Starting daily DB backup...")
        db_path = Config.DB_NAME
        
        if not os.path.exists(db_path):
            logger.error("DB file not found!")
            return

        with open(db_path, 'rb') as db_file:
            await context.bot.send_document(
                chat_id=Config.BACKUP_CHAT_ID,
                document=db_file,
                caption=f"🛟 Ежедневный бэкап БД ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} UTC)",
                disable_notification=True
            )
            logger.info("DB backup successfully sent")
            
    except Exception as e:
        logger.error(f"Error in DB backup: {str(e)}")

async def post_init(application):
    # Регистрируем периодический самопинг через job_queue
    application.job_queue.run_repeating(
        self_ping,
        interval=180,  # 3 минут
        first=10  # Первый пинг через 10 сек после старта
    )
    
    # Ежедневный бэкап(сохранение данных) в 21:00 UTC
    application.job_queue.run_daily(
        send_db_backup,
        time=datetime.time(hour=21, minute=0, tzinfo=datetime.timezone.utc),
        days=(0, 1, 2, 3, 4, 5, 6),
        name="daily_db_backup"
    )
    
    # Настраиваем обработчик сигналов только для UNIX-систем
    if sys.platform != 'win32':
        loop = asyncio.get_running_loop()
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signame),
                partial(handle_signal, application, loop, signame)
            )

def main():
    # Запуск Flask в отдельном потоке
    threading.Thread(target=run_web_server, daemon=True).start()

    # Создание и настройка бота
    application = ApplicationBuilder()\
        .token(Config.BOT_TOKEN)\
        .post_init(post_init)\
        .build()
    
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
    application.add_handler(CommandHandler("export_data", export_chat_data, filters=allowed_chat))
    application.add_handler(CommandHandler("restore_data", import_chat_data, filters=allowed_chat))

    # Запуск бота
    try:
        application.run_polling()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application stopped by user")
    finally:
        if application.running:
            application.stop()

if __name__ == "__main__":
    main()