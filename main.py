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
from telegram import (
    Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup, 
    KeyboardButton, KeyboardButtonRequestChat, KeyboardButtonRequestUsers, 
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters
)
# from telegram.helpers import escape_markdown
from html import escape as escape_html
import signal
import sys
from functools import partial
import json
import random
import re

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
    BOT_TOKEN = "7661688763:AAEk911JxnrHRQ_l9UgAp3AhHl9YyDVxiWU"  # os.getenv("BOT_TOKEN")
    RENDER_APP_URL = "https://einstein-point-bot-7k8m.onrender.com"  # os.getenv("RENDER_APP_URL")
    DB_NAME = "points_bot.db"
    BACKUP_CHAT_ID = -1002571801416  # ID чата для бэкапов(сохранении данных) https://t.me/+Axwm80ZCBSc3NjQy
    ALLOWED_CHAT_IDS = [BACKUP_CHAT_ID, -1002157100033, -1002439723121]
    CREATOR = "@andranik_amrahyan"
    ALLOWED_ADMINS = {
        # Формат: {chat_id: [user_id1, user_id2, ...]}
        -1002157100033: [5553779390, 1057267401, 2085350493, 1911958747, 5518327998],  # https://t.me/Family_Worlds
        -1002439723121: [5553779390]  # https://t.me/Einstein_bot_test_2
    }

# Глобальная переменная для управления состоянием приложения
application = None

# In-memory store for in-progress giveaways (per-organizer)
giveaways_in_progress = {}

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

    # Таблица розыгрышей
    c.execute('''CREATE TABLE IF NOT EXISTS giveaways
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  chat_id INTEGER,
                  message_id INTEGER,
                  organizer_id INTEGER,
                  organizers_text TEXT,
                  prize_total INTEGER,
                  winners_count INTEGER,
                  button_text TEXT,
                  condition_chats TEXT,
                  participants_count INTEGER DEFAULT 0,
                  finished INTEGER DEFAULT 0)''')

    # Таблица участников для каждого розыгрыша
    c.execute('''CREATE TABLE IF NOT EXISTS giveaway_participants
                 (giveaway_id INTEGER,
                  user_id INTEGER,
                  username TEXT,
                  full_name TEXT,
                  UNIQUE(giveaway_id, user_id))''')

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

# --------- NEW: Helper functions for giveaways ---------

def save_giveaway_to_db(giveaway_data: dict) -> int:
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    # condition_chats сохраняется как JSON строка. Теперь это список словарей.
    c.execute('''INSERT INTO giveaways
                 (chat_id, organizer_id, organizers_text, prize_total, winners_count, button_text, condition_chats)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                     giveaway_data['chat_id'],
                     giveaway_data['organizer_id'],
                     giveaway_data.get('organizers_text',''),
                     giveaway_data['prize_total'],
                     giveaway_data['winners_count'],
                     giveaway_data['button_text'],
                     json.dumps(giveaway_data.get('condition_chats', []))
                 ))
    gid = c.lastrowid
    conn.commit()
    conn.close()
    return gid

def set_giveaway_message_id(giveaway_id: int, message_id: int):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE giveaways SET message_id=? WHERE id=?', (message_id, giveaway_id))
    conn.commit()
    conn.close()

def add_participant(giveaway_id: int, user_id: int, username: str, full_name: str) -> bool:
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO giveaway_participants (giveaway_id, user_id, username, full_name)
                     VALUES (?, ?, ?, ?)''', (giveaway_id, user_id, username or '', full_name or ''))
        c.execute('UPDATE giveaways SET participants_count = participants_count + 1 WHERE id=?', (giveaway_id,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Уже есть
        return False
    finally:
        conn.close()

def get_giveaway(giveaway_id: int) -> dict:
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('SELECT id, chat_id, message_id, organizer_id, organizers_text, prize_total, winners_count, button_text, condition_chats, participants_count, finished FROM giveaways WHERE id=?', (giveaway_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        'id': row[0],
        'chat_id': row[1],
        'message_id': row[2],
        'organizer_id': row[3],
        'organizers_text': row[4],
        'prize_total': row[5],
        'winners_count': row[6],
        'button_text': row[7],
        'condition_chats': json.loads(row[8] or '[]'),
        'participants_count': row[9],
        'finished': bool(row[10])
    }

def get_participants(giveaway_id: int):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('SELECT user_id, username, full_name FROM giveaway_participants WHERE giveaway_id=?', (giveaway_id,))
    rows = c.fetchall()
    conn.close()
    return [{'user_id': r[0], 'username': r[1], 'full_name': r[2]} for r in rows]

def mark_giveaway_finished(giveaway_id: int):
    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE giveaways SET finished=1 WHERE id=?', (giveaway_id,))
    conn.commit()
    conn.close()

# ------------------------------------------------------

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
        
        # Используем HTML-упоминание
        try:
            user_link = target_user.mention_html()
        except Exception:
            # fallback to simple escaped name
            user_link = escape_html(target_user.full_name or target_user.username or str(target_user.id))

        await update.message.reply_text(
            f"✅ Пользователю {user_link} "
            f"{'добавлено' if operation == 'add' else 'снято'} {abs(points)} баллов",
            parse_mode="HTML"
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
        try:
            user_mention = target_user.mention_html()
        except Exception:
            user_mention = escape_html(target_user.full_name or target_user.username or str(target_user.id))

        await update.message.reply_text(
            f"🏆 Пользователь {user_mention} имеет <b>{points}</b> баллов",
            parse_mode="HTML"
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
            name = escape_html(user.user.full_name or user.user.username or str(user.user.id))
            mention = f"<a href=\"tg://openmessage?user_id={user.user.id}\">{name}</a>"
        except BadRequest:
            # Если не удалось получить пользователя, используем сохраненное имя
            name_to_show = full_name or username or f"Пользователь {user_id}"
            name_escaped = escape_html(name_to_show)
            mention = f"<a href=\"tg://openmessage?user_id={user_id}\">{name_escaped}</a>"
        except Exception as e:
            logger.error(f"Ошибка при получении пользователя в top_users: {e}")
            mention = f"<a href=\"tg://openmessage?user_id={user_id}\">Пользователь {user_id}</a>"
        
        # Экранируем все спецсимволы
        escaped_points = escape_html(str(points))
        
        line = (
            f"{index + 1}. {mention} - <b>{escaped_points}</b> баллов"
        )
        lines.append(line)

    response += "\n".join(lines)
    await update.message.reply_text(response, parse_mode="HTML")  # HTML

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Экранируем все специальные символы
    creator_username = Config.CREATOR.lstrip('@')
    creator_link_html = f'<a href="https://t.me/{creator_username}">Эйнштейн</a>'
    
    help_text = (
        "<b>📚 Доступные команды:</b>\n"
        "────────────────────\n"
        "➕ <b>Добавить баллы (модераторы)</b>:\n"
        "<code>/add [кол-во] @юзер</code> или ответ на сообщение\n\n"
        "➖ <b>Снять баллы (модераторы)</b>:\n"
        "<code>/remove [кол-во] @юзер</code> или ответ на сообщение\n\n"
        "🏆 <b>Мои баллы:</b>\n"
        "<code>/my</code>\n\n"
        "🔍 <b>Проверить баллы другого:</b>\n"
        "<code>/points @юзер</code> или ответ на сообщение\n\n"
        "🏅 <b>Топ пользователей:</b>\n"
        "<code>/top [число]</code> (по умолчанию 10)\n\n"
        "💾 <b>Экспорт данных (модераторы):</b>\n"
        "<code>/export_data</code> - экспорт данных чата в сообщение\n\n"
        "🔄 <b>Восстановление данных (модераторы):</b>\n"
        "<code>/restore_data</code> - восстановление данных из сообщения\n\n"
        "🎁 <b>Розыгрыши (личные сообщения с ботом):</b>\n"
        "<code>/create_giveaway</code> - создать новый розыгрыш (лично боту)\n"
        "<code>/cancel_giveaway</code> - отменить процесс создания розыгрыша (лично боту)\n"
        "<code>/end_giveaway</code> - завершить розыгрыш (отправьте в чате-ответом на сообщение розыгрыша)\n\n"
        "🆘 <b>Помощь:</b>\n"
        "<code>/help</code>\n"
        "────────────────────\n"
        f"👨💻 Создатель бота: {creator_link_html}"
    )
    
    await update.message.reply_text(
        help_text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

# Добавляем обработчик для новых чатов
async def handle_new_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.bot.id in [user.id for user in update.message.new_chat_members]:
        chat_id = update.effective_chat.id
        await help_command(update, context)
        if chat_id not in Config.ALLOWED_CHAT_IDS:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🚫 Бот доступен только для разрешенных чатов.\n"
                    f"Для получения бота свяжитесь с {Config.CREATOR}"
                )
            )

# Обработка неавторизованных команд в других чатах
async def reject_unauthorized_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяет, если команда отправлена в неразрешенном чате, и она есть в списке команд бота.
    Если да - отправляет отказ.
    """
    msg_text = update.message.text or update.message.caption or ""
    
    if not msg_text.startswith('/'):
        return

    # Извлекаем "чистую" команду (без /, аргументов и @username)
    # Пример: "/start@botname arg" -> "start"
    command_part = msg_text.split()[0][1:].split('@')[0].lower()
    
    # Если это команда help — ничего не делаем и выходим (она должна работать везде)
    if command_part == "help":
        return
    
    known_commands = context.bot_data.get('bot_commands', set())
    
    # Если эта команда есть в списке команд бота (полученном через getMyCommands)
    if command_part in known_commands:
        await update.message.reply_text(
            f"🚫 Бот доступен только для разрешенных чатов.\n"
            f"Для получения бота свяжитесь с {Config.CREATOR}",
            reply_to_message_id=update.message.message_id
        )

# --------- GIVEAWAY: Conversation & Interaction ---------

async def create_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Команда должна приходить в личном чате
    if update.effective_chat.type != 'private':
        await update.message.reply_text(
            "❗️ Для создания розыгрыша используйте команду в личных сообщениях с ботом.",
            reply_to_message_id=update.message.message_id
        )
        return

    user_id = update.effective_user.id
    giveaways_in_progress[user_id] = {'step': 'choose_chat', 'organizer_id': user_id}

    # Использование Reply клавиатуры с запросом чата (Chat Selection)
    # request_id=1 - идентификатор запроса
    # chat_is_channel=False - нам нужны группы, не каналы
    request_btn = KeyboardButton(
        text="📢 Выбрать чат из списка",
        request_chat=KeyboardButtonRequestChat(request_id=1, chat_is_channel=False)
    )
    cancel_btn = KeyboardButton(text="Отменить")

    markup = ReplyKeyboardMarkup([[request_btn], [cancel_btn]], resize_keyboard=True, one_time_keyboard=True)

    await update.message.reply_text(
        "🎉 Вы начали создание розыгрыша.\n"
        "Нажмите кнопку ниже, чтобы выбрать группу, в которой будет проводиться розыгрыш.\n\n"
        "<i>Примечание: Выберите один из разрешенных чатов.</i>",
        reply_markup=markup,
        parse_mode="HTML"
    )

async def handle_chat_shared(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик получения общей информации о чате (после нажатия кнопки request_chat)"""
    user_id = update.effective_user.id
    state = giveaways_in_progress.get(user_id)
    
    if not state:
        return

    shared_chat_id = update.message.chat_shared.chat_id
    step = state.get('step')

    # ========================== ШАГ 1: ВЫБОР ОСНОВНОГО ЧАТА ==========================
    if step == 'choose_chat':
        # 1. Проверяем, разрешен ли этот чат
        if shared_chat_id not in Config.ALLOWED_CHAT_IDS:
            await update.message.reply_text(
                "🚫 Этот чат не входит в список разрешенных чатов бота.\n"
                "Пожалуйста, выберите корректный чат или нажмите 'Отменить'.\n"
                f"Или для получения бота свяжитесь с {Config.CREATOR}"
            )
            return

        # 2. Проверяем права модератора
        if not await is_moderator(user_id, shared_chat_id, context.bot):
            await update.message.reply_text(
                "❌ У вас нет прав модератора в выбранном чате. Выберите другой чат или отмените."
            )
            return

        # 3. Проверяем наличие бота в чате
        try:
            await context.bot.get_chat_member(shared_chat_id, context.bot.id)
        except Exception:
            await update.message.reply_text(
                "❌ Бот не находится в выбранном чате. Добавьте бота и попробуйте снова."
            )
            return

        # Сохраняем чат и переходим к выбору организаторов
        giveaways_in_progress[user_id].update({'chat_id': shared_chat_id, 'step': 'ask_organizers'})

        # Создаем клавиатуру для выбора пользователей (User Selection)
        request_users_btn = KeyboardButton(
            text="👤 Выбрать организаторов",
            request_users=KeyboardButtonRequestUsers(
                request_id=2, 
                user_is_bot=False, 
                user_is_premium=None, 
                max_quantity=10
            )
        )
        me_btn = KeyboardButton(text="Только я")
        cancel_btn = KeyboardButton(text="Отменить")
        
        markup = ReplyKeyboardMarkup([[request_users_btn], [me_btn, cancel_btn]], resize_keyboard=True, one_time_keyboard=True)

        await update.message.reply_text(
            "✅ Чат выбран.\n\n"
            "Теперь укажите организаторов.\n"
            "Нажмите **«Выбрать организаторов»**, чтобы выбрать людей из списка контактов или чатов,\n"
            "или нажмите **«Только я»**, чтобы организатором были только вы.",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        return

    # ========================== ШАГ 3: ВЫБОР УСЛОВИЙ (CONDITION CHATS) ==========================
    elif step == 'ask_conditions':
        # 2) Проверка: Если выбранный чат совпадает с чатом розыгрыша - игнорируем
        if shared_chat_id == state.get('chat_id'):
            await update.message.reply_text(
                "ℹ️ Этот чат уже является местом проведения розыгрыша и добавлен в условия автоматически.",
                reply_markup=get_conditions_keyboard()
            )
            return

        # При получении чата для условий проверяем, состоит ли в нем бот
        try:
            # Пытаемся получить инфо о чате и статусе бота
            member = await context.bot.get_chat_member(shared_chat_id, context.bot.id)
            if member.status in ['left', 'kicked']:
                raise BadRequest("Bot not member")
            
            # Получаем объект чата
            chat_obj = await context.bot.get_chat(shared_chat_id)
            title = chat_obj.title or str(shared_chat_id)
            username = chat_obj.username

        except Exception as e:
            await update.message.reply_text(
                f"❌ Бот не является участником чата (ID: {shared_chat_id}).\n"
                "Сначала добавьте бота в этот канал/группу, а затем попробуйте снова.",
                reply_markup=get_conditions_keyboard()
            )
            return

        # 5) Проверка частных каналов (без username)
        if not username:
            # Сохраняем временные данные и просим ссылку
            state['temp_condition_id'] = shared_chat_id
            state['temp_condition_title'] = title
            state['step'] = 'waiting_condition_link'
            
            await update.message.reply_text(
                f"ℹ️ Вы выбрали частный чат <b>{escape_html(title)}</b> (без публичного имени).\n"
                "Пожалуйста, отправьте ссылку-приглашение в этот чат, чтобы добавить его в условия.",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove() # Убираем кнопки пока ждем ссылку
            )
            return

        # Если есть username, формируем ссылку и сохраняем
        link = f"https://t.me/{username}"
        add_condition_chat(state, shared_chat_id, title, link)
        
        # Формируем сообщение со списком
        msg = build_conditions_message(state, added_title=title)
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_conditions_keyboard())
        return

def add_condition_chat(state, chat_id, title, link):
    current_conditions = state.get('condition_chats', [])
    # Проверяем, нет ли уже такого ID
    for c in current_conditions:
        if c['id'] == chat_id:
            return False # Уже есть
    
    # Добавляем объект с данными
    current_conditions.append({
        'id': chat_id,
        'title': title,
        'link': link
    })
    state['condition_chats'] = current_conditions
    return True

def build_conditions_message(state, added_title=None):
    # 3) Добавляем "Текущий список условий" с обязательным пунктом
    msg = ""
    if added_title:
        msg += f"✅ Добавлен чат: <b>{escape_html(added_title)}</b>\n\n"
    
    msg += "<b>Текущий список условий:</b>\n"
    msg += "• Состоять в чате розыгрыша\n"
    
    conditions = state.get('condition_chats', [])
    for c in conditions:
        title = c.get('title', str(c.get('id')))
        msg += f"• {escape_html(title)}\n"
        
    msg += "\nДобавьте ещё чаты или нажмите «Готово»."
    return msg

def get_conditions_keyboard():
    # Клавиатура для добавления условий
    # request_id=3 -> Группы, request_id=4 -> Каналы
    btn_channel = KeyboardButton(
        text="➕ Добавить канал",
        request_chat=KeyboardButtonRequestChat(request_id=4, chat_is_channel=True)
    )
    btn_group = KeyboardButton(
        text="➕ Добавить группу",
        request_chat=KeyboardButtonRequestChat(request_id=3, chat_is_channel=False)
    )
    btn_done = KeyboardButton(text="✅ Готово / Далее")
    btn_cancel = KeyboardButton(text="Отменить")
    return ReplyKeyboardMarkup([[btn_channel, btn_group], [btn_done, btn_cancel]], resize_keyboard=True)

async def handle_users_shared(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик получения пользователей (после нажатия кнопки request_users)"""
    user_id = update.effective_user.id
    state = giveaways_in_progress.get(user_id)
    
    if not state or state.get('step') != 'ask_organizers':
        return

    shared_users = update.message.users_shared.users
    
    valid_names = []
    failed_users = []
    
    # 4) Обработка ошибок при получении пользователей
    for shared_user in shared_users:
        uid = shared_user.user_id
        try:
            # Пытаемся получить информацию
            chat_info = await context.bot.get_chat(uid)
            full_name = chat_info.full_name or chat_info.title or f"User {uid}"
            # 1) Оборачиваем имя в <code>
            valid_names.append(f"<code>{escape_html(full_name)}</code>")
        except Exception as e:
            logger.warning(f"Could not fetch info for user {uid}: {e}")
            failed_users.append(uid)

    # Если есть ошибки - включаем режим ручного ввода/исправления
    if failed_users:
        # Сохраняем уже полученные имена во временный список
        state['temp_organizers'] = valid_names
        state['step'] = 'manual_organizer_entry'
        
        await update.message.reply_text(
            f"⚠️ Не удалось получить данные для {len(failed_users)} пользователей.\n"
            "Возможно, они не запускали этого бота.\n"
            "Пусть они отправят команду /start боту и вы попробуете снова, либо вы можете ввести их имена вручную далее."
        )

        current_orgs_str = ", ".join(valid_names) if valid_names else "(пока нет)"
        
        kb = ReplyKeyboardMarkup([
            [KeyboardButton("✅ Готово / Далее")],
            [KeyboardButton("Отменить")]
        ], resize_keyboard=True)

        await update.message.reply_text(
            f"Выбраны организаторы: {current_orgs_str}\n\n"
            "Введите имена недостающих организаторов вручную (одним сообщением через запятую, например: <code>Иван</code>, <code>Петр</code>) или нажмите «Готово», если список завершен.",
            parse_mode='HTML',
            reply_markup=kb
        )
        return

    # Если ошибок нет и есть валидные имена - переходим сразу к условиям (как раньше)
    if valid_names:
        organizers_text = ", ".join(valid_names)
        state['organizers_text'] = organizers_text
        state['step'] = 'ask_conditions'
        state['condition_chats'] = [] # Список объектов {id, title, link}
        
        msg_text = (
            f"Выбраны организаторы: {organizers_text}\n\n"
            "Теперь укажите чаты/каналы, в которых должен состоять участник.\n"
            "Используйте кнопки ниже для добавления.\n\n"
            "<b>Текущий список условий:</b>\n"
            "• Состоять в чате розыгрыша"
        )

        await update.message.reply_text(
            msg_text,
            parse_mode="HTML",
            reply_markup=get_conditions_keyboard()
        )
        return

    # Если никого не удалось добавить (все failed)
    if not valid_names and not failed_users:
         # Странный кейс, но на всякий случай
         await update.message.reply_text("Не выбрано ни одного пользователя.")

async def cancel_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # отмена процесса создания (личный чат)
    if update.effective_chat.type != 'private':
        await update.message.reply_text("❗️ Команда отмены доступна только в личных сообщениях с ботом.")
        return
    user_id = update.effective_user.id
    if user_id in giveaways_in_progress:
        giveaways_in_progress.pop(user_id, None)
        await update.message.reply_text("✅ Процесс создания розыгрыша отменён.", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("ℹ️ Нет активного процесса создания розыгрыша.", reply_markup=ReplyKeyboardRemove())

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    # Abort
    if data == 'give_abort':
        giveaways_in_progress.pop(user_id, None)
        try:
            await query.edit_message_text('❌ Создание розыгрыша отменено.')
        except Exception:
            pass
        await query.answer(text='Отменено', show_alert=False)
        return

    # Start finalization
    if data and data.startswith('give_start_'):
        owner_id = int(data.split('_')[-1])
        if owner_id != user_id:
            await query.answer('Только создатель розыгрыша может начать его.', show_alert=False)
            return
        state = giveaways_in_progress.get(user_id)
        if not state:
            try:
                await query.edit_message_text('ℹ️ Нет данных для запуска розыгрыша.')
            except Exception:
                pass
            await query.answer('Нет данных', show_alert=False)
            return

        # Сохраняем в БД
        giveaway_payload = {
            'chat_id': state['chat_id'],
            'organizer_id': user_id,
            'organizers_text': state.get('organizers_text',''),
            'prize_total': int(state.get('prize_total',0)),
            'winners_count': int(state.get('winners_count',1)),
            'button_text': state.get('button_text','Участвовать 🎯'),
            'condition_chats': state.get('condition_chats', []) # Это список словарей
        }
        gid = save_giveaway_to_db(giveaway_payload)

        # Формируем пост
        chat_obj = await context.bot.get_chat(state['chat_id'])
        chat_title = chat_obj.title or str(state['chat_id'])
        chat_link = None
        if getattr(chat_obj, 'username', None):
            chat_link = f"https://t.me/{chat_obj.username}"

        organizers_display = giveaway_payload['organizers_text'] 
        prize_total = giveaway_payload['prize_total']
        winners_count = giveaway_payload['winners_count']
        per_winner = round(prize_total / winners_count) if winners_count > 0 else prize_total

        conditions_lines = []
        conditions_lines.append(f"⠀• Состоять в чате розыгрыша")

        # Проходим по списку словарей
        for c_data in giveaway_payload['condition_chats']:
            # c_data = {'id': ..., 'title': ..., 'link': ...}
            title = c_data.get('title', 'Unknown')
            link = c_data.get('link')
            
            if link:
                conditions_lines.append(f"⠀• Состоять в <a href=\"{link}\">{escape_html(title)}</a>")
            else:
                conditions_lines.append(f"⠀• Состоять в {escape_html(title)}")

        prize_text = f"🏆 Призовой фонд: <code>{prize_total} баллов</code>\n🏅 Победителей: <code>{winners_count}</code>\n💸 Каждому: <code>{per_winner} баллов</code>"

        if chat_link:
            chat_md = f"<a href=\"{chat_link}\">{escape_html(chat_title)}</a>"
        else:
            chat_md = f"<code>{escape_html(chat_title)}</code>"

        full_text = (
            "🎉 <b>Розыгрыш!</b> 💸\n\n"
            f"👑 <b>Организатор(ы):</b> {organizers_display}\n"
            f"📢 Чат: {chat_md}\n\n"
            f"{prize_text}\n\n"
            "<b>✅ Условия участия:</b>\n"
        )

        for l in conditions_lines:
            full_text += l + "\n"

        full_text += "⠀• Нажать на кнопку под постом\n\n"
        full_text += f"👥 Участников: <code>0 чел.</code>\n"

        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(text=giveaway_payload['button_text'], callback_data=f'enter_give_{gid}')]])
        sent = await context.bot.send_message(
            chat_id=state['chat_id'],
            text=full_text,
            parse_mode='HTML',
            reply_markup=keyboard,
            disable_web_page_preview=True
        )

        set_giveaway_message_id(gid, sent.message_id)
        giveaways_in_progress.pop(user_id, None)

        try:
            await query.edit_message_text('✅ Розыгрыш успешно создан и отправлен в указанный чат.')
        except Exception:
            pass
        await query.answer(text='Розыгрыш запущен', show_alert=False)
        return

    # Participation handler
    if data and data.startswith('enter_give_'):
        gid = int(data.split('_')[-1])
        give = get_giveaway(gid)
        if not give or give.get('finished'):
            await query.answer('ℹ️ Этот розыгрыш завершён или не найден.', show_alert=False)
            return

        user = query.from_user
        
        # Собираем ID чатов для проверки
        # give['condition_chats'] - это список словарей [{'id':...}, ...]
        condition_chats_ids = [c['id'] for c in give.get('condition_chats', [])]
        required_chats = [give['chat_id']] + condition_chats_ids
        
        missing = []
        for cid in required_chats:
            try:
                member = await context.bot.get_chat_member(cid, user.id)
                if member.status in ['left','kicked']:
                    missing.append(cid)
            except BadRequest:
                missing.append(cid)
            except Exception as e:
                logger.warning(f"Error checking membership for user {user.id} in chat {cid}: {e}")
                missing.append(cid)

        if missing:
            await query.answer('❌ Вы не состоите во всех требуемых чатах.', show_alert=False)
            return

        added = add_participant(gid, user.id, user.username or '', user.full_name or '')
        if not added:
            await query.answer('ℹ️ Вы уже участвуете в этом розыгрыше.', show_alert=False)
            return

        # Обновляем сообщение с числом участников
        participants = get_participants(gid)
        participants_count = len(participants)
        
        try:
            chat_obj = await context.bot.get_chat(give['chat_id'])
            chat_title = chat_obj.title or str(give['chat_id'])
            chat_link = f"https://t.me/{chat_obj.username}" if getattr(chat_obj, 'username', None) else None

            organizers_display = give['organizers_text']
            prize_total = give['prize_total']
            winners_count = give['winners_count']
            per_winner = round(prize_total / winners_count) if winners_count > 0 else prize_total

            conditions_lines = []
            conditions_lines.append(f"⠀• Состоять в чате розыгрыша")

            for c_data in give['condition_chats']:
                title = c_data.get('title', 'Unknown')
                link = c_data.get('link')
                if link:
                    conditions_lines.append(f"⠀• Состоять в <a href=\"{link}\">{escape_html(title)}</a>")
                else:
                    conditions_lines.append(f"⠀• Состоять в {escape_html(title)}")

            prize_text = f"🏆 Призовой фонд: <code>{prize_total} баллов</code>\n🏅 Победителей: <code>{winners_count}</code>\n💸 Каждому: <code>{per_winner} баллов</code>"

            if chat_link:
                chat_md = f"<a href=\"{chat_link}\">{escape_html(chat_title)}</a>"
            else:
                chat_md = f"<code>{escape_html(chat_title)}</code>"

            full_text = (
                "🎉 <b>Розыгрыш!</b> 💸\n\n"
                f"👑 <b>Организатор(ы):</b> {organizers_display}\n"
                f"📢 Чат: {chat_md}\n\n"
                f"{prize_text}\n\n"
                "<b>✅ Условия участия:</b>\n"
            )

            for l in conditions_lines:
                full_text += l + "\n"

            full_text += "⠀• Нажать на кнопку под постом\n\n"
            full_text += f"👥 Участников: <code>{participants_count} чел.</code>\n"

            await context.bot.edit_message_text(
                chat_id=give['chat_id'],
                message_id=give['message_id'],
                text=full_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text=give['button_text'], callback_data=f'enter_give_{gid}')]]),
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"Не удалось обновить сообщение розыгрыша: {e}")

        await query.answer('✅ Вы успешно участвуете в розыгрыше!', show_alert=False)
        return

async def handle_giveaway_text_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Обработка текстовых ответов в личных сообщениях при создании розыгрыша
    if update.effective_chat.type != 'private':
        return
    user_id = update.effective_user.id
    text = (update.message.text or '').strip()

    # Обработка кнопок отмены/навигации из Reply клавиатуры
    if text == "Отменить":
        await cancel_giveaway(update, context)
        return

    state = giveaways_in_progress.get(user_id)
    if not state:
        # Если пришел текст, но состояния нет - возможно нужно удалить клавиатуру если она зависла
        if text in ["Отменить", "Только я", "✅ Готово / Далее"]:
             await update.message.reply_text("Нет активного действия.", reply_markup=ReplyKeyboardRemove())
        return

    step = state.get('step')

    # Шаг выбора организатора (если пользователь выбрал "Только я" или ввел текст вручную вместо кнопки)
    if step == 'ask_organizers':
        if text == "Только я" or text == ".":
            # Используем данные самого пользователя
            name = f"<code>{escape_html(update.effective_user.full_name)}</code>"
            state['organizers_text'] = name
        else:
            # Ручной ввод - тоже оборачиваем в <code>
            # Предполагаем, что пользователь может ввести "Name1, Name2"
            # Разбиваем и форматируем каждый
            raw_names = [n.strip() for n in text.split(',')]
            formatted_names = [f"<code>{escape_html(n)}</code>" for n in raw_names if n]
            state['organizers_text'] = ", ".join(formatted_names)
        
        state['step'] = 'ask_conditions'
        state['condition_chats'] = []
        
        msg = (
            "Теперь укажите чаты/каналы, в которых должен состоять участник.\n"
            "Используйте кнопки ниже для добавления.\n\n"
            "<b>Текущий список условий:</b>\n"
            "• Состоять в чате розыгрыша"
        )
        await update.message.reply_text(
            msg,
            parse_mode="HTML",
            reply_markup=get_conditions_keyboard()
        )
        return

    # Добавление организаторов вручную после ошибки получения данных
    if step == 'manual_organizer_entry':
        if text == "✅ Готово / Далее":
            # Собираем итоговый список
            org_list = state.get('temp_organizers', [])
            if not org_list:
                await update.message.reply_text("Список организаторов пуст. Введите хотя бы одно имя или нажмите Отменить.")
                return
            
            organizers_text = ", ".join(org_list)
            state['organizers_text'] = organizers_text
            
            # Переход к условиям
            state['step'] = 'ask_conditions'
            state['condition_chats'] = []
            
            msg_text = (
                f"Выбраны организаторы: {organizers_text}\n\n"
                "Теперь укажите чаты/каналы, в которых должен состоять участник.\n"
                "Используйте кнопки ниже для добавления.\n\n"
                "<b>Текущий список условий:</b>\n"
                "• Состоять в чате розыгрыша"
            )

            await update.message.reply_text(
                msg_text,
                parse_mode="HTML",
                reply_markup=get_conditions_keyboard()
            )
        else:
            # Ручной ввод имен
            raw_names = [n.strip() for n in text.split(',')]
            formatted_names = [f"<code>{escape_html(n)}</code>" for n in raw_names if n]
            
            current_list = state.get('temp_organizers', [])
            current_list.extend(formatted_names)
            state['temp_organizers'] = current_list
            
            # Показываем обновленный список
            current_orgs_str = ", ".join(current_list)
            await update.message.reply_text(
                f"Добавлено. Текущий список: {current_orgs_str}\n\n"
                "Введите еще имена или нажмите «Готово / Далее».",
                parse_mode='HTML'
            )
        return

    # 5) Обработка ссылки для частного канала
    if step == 'waiting_condition_link':
        # Простая проверка ссылки
        if not (text.startswith('http') or text.startswith('t.me')):
            await update.message.reply_text("❌ Это не похоже на ссылку. Пожалуйста, отправьте корректную ссылку приглашения (например https://t.me/+AbCdE...).")
            return
        
        # Добавляем с ссылкой
        chat_id = state.get('temp_condition_id')
        title = state.get('temp_condition_title')
        
        add_condition_chat(state, chat_id, title, text)
        
        # Возвращаемся в меню условий
        state['step'] = 'ask_conditions'
        # Очищаем темп данные
        state.pop('temp_condition_id', None)
        state.pop('temp_condition_title', None)
        
        msg = build_conditions_message(state, added_title=title)
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_conditions_keyboard())
        return

    if step == 'ask_conditions':
        if text == "✅ Готово / Далее":
            # Переход к следующему шагу
            state['step'] = 'ask_prize'
            await update.message.reply_text(
                'Укажите общий призовой фонд в баллах (число). Например: 1000',
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            # Если пользователь пытается ввести текст вручную, напоминаем про кнопки
            await update.message.reply_text(
                'Пожалуйста, используйте кнопки меню для добавления чатов или завершения выбора.',
                reply_markup=get_conditions_keyboard()
            )
        return

    if step == 'ask_prize':
        try:
            val = int(text)
            if val <= 0:
                raise ValueError()
            state['prize_total'] = val
            state['step'] = 'ask_winners'
            await update.message.reply_text('Сколько победителей? Введите целое число, например: 3')
        except Exception:
            await update.message.reply_text('Неверный ввод. Укажите целое число > 0 для призового фонда.')
        return

    if step == 'ask_winners':
        try:
            val = int(text)
            if val <= 0:
                raise ValueError()
            state['winners_count'] = val
            state['step'] = 'ask_button'
            await update.message.reply_text('Укажите текст кнопки для участия, или отправьте точку "." чтобы выбрать значение по умолчанию (Участвовать 🎯).')
        except Exception:
            await update.message.reply_text('Неверный ввод. Укажите целое число > 0 для количества победителей.')
        return

    if step == 'ask_button':
        if text == '.':
            state['button_text'] = 'Участвовать 🎯'
        else:
            state['button_text'] = text
        # Переходим к предпросмотру
        state['step'] = 'preview'

        # Формируем предпросмотр
        try:
            chat = await context.bot.get_chat(state['chat_id'])
            chat_title = chat.title or str(state['chat_id'])
            # organizers_text (уже с <code> тегами)
            organizers = state.get('organizers_text') 
            prize_total = state.get('prize_total', 0)
            winners_count = state.get('winners_count', 1)
            per_winner = round(prize_total / winners_count) if winners_count>0 else prize_total

            conditions_text = ''
            # state['condition_chats'] список словарей
            for c_data in state.get('condition_chats', []):
                title = c_data.get('title', 'Unknown')
                link = c_data.get('link')
                if link:
                    conditions_text += f"⠀• Состоять в <a href=\"{link}\">{escape_html(title)}</a>\n"
                else:
                    conditions_text += f"⠀• Состоять в {escape_html(title)}\n"

            prize_text = f"🏆 Призовой фонд: <code>{prize_total} баллов</code>\n🏅 Победителей: <code>{winners_count}</code>\n💸 Каждому: <code>{per_winner} баллов</code>"

            chat_link = getattr(chat, 'username', None)
            if chat_link:
                chat_md = f"<a href=\"https://t.me/{chat.username}\">{escape_html(chat_title)}</a>"
            else:
                chat_md = f"<code>{escape_html(chat_title)}</code>"

            preview = (
                "🎉 <b>Розыгрыш!</b> 💸\n\n"
                f"👑 <b>Организатор(ы):</b> {organizers}\n"
                f"📢 Чат: {chat_md}\n\n"
                f"{prize_text}\n\n"
                "<b>✅ Условия участия:</b>\n"
                f"⠀• Состоять в чате розыгрыша\n"
                f"{conditions_text}"
                "⠀• Нажать на кнопку под постом\n\n"
                f"👥 Участников: <code>0 чел.</code>\n"
            )

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(text='Начать розыгрыш', callback_data=f'give_start_{user_id}')],
                [InlineKeyboardButton(text='Отменить', callback_data='give_abort')]
            ])
            await update.message.reply_text(preview, parse_mode='HTML', reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error forming preview: {e}")
            await update.message.reply_text('Ошибка при формировании предпросмотра. Попробуйте снова.')
        return

# Команда завершения розыгрыша (в чате розыгрыша, ответом на сообщение розыгрыша)
async def end_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text('❗️ Эту команду нужно отправить как ответ на сообщение розыгрыша.')
        return

    replied = update.message.reply_to_message
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    if not await is_moderator(user_id, chat_id, context.bot):
        await update.message.reply_text('❌ Только модераторы этого чата могут завершать розыгрыш.')
        return

    conn = sqlite3.connect(Config.DB_NAME)
    c = conn.cursor()
    c.execute('SELECT id FROM giveaways WHERE chat_id=? AND message_id=? AND finished=0', (chat_id, replied.message_id))
    row = c.fetchone()
    conn.close()
    if not row:
        await update.message.reply_text('❌ Этот ответ не связан с активным розыгрышем.')
        return

    gid = row[0]
    give = get_giveaway(gid)
    participants = get_participants(gid)
    if not participants:
        await update.message.reply_text('ℹ️ В розыгрыше нет участников.')
        mark_giveaway_finished(gid)
        return

    valid_users = []
    # Извлекаем ID из condition_chats (список словарей)
    condition_chats_ids = [c['id'] for c in give.get('condition_chats', [])]
    required_chats = [give['chat_id']] + condition_chats_ids
    
    for p in participants:
        uid = p['user_id']
        ok = True
        for cid in required_chats:
            try:
                member = await context.bot.get_chat_member(cid, uid)
                if member.status in ['left','kicked']:
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            valid_users.append(p)

    if not valid_users:
        await update.message.reply_text('ℹ️ Нет участников, соответствующих условиям на момент подведения итогов.')
        mark_giveaway_finished(gid)
        return

    winners_count = min(give['winners_count'], len(valid_users))
    winners = random.sample(valid_users, winners_count)

    per_winner = round(give['prize_total'] / give['winners_count']) if give['winners_count']>0 else give['prize_total']

    winners_mentions = []
    for w in winners:
        uid = w['user_id']
        try:
            member = await context.bot.get_chat_member(chat_id, uid)
            uname = member.user.username or ''
            fname = member.user.full_name or ''
        except Exception:
            uname = w.get('username','')
            fname = w.get('full_name','')

        update_user_points(uid, chat_id, per_winner, uname, fname)
        mention = f"<a href=\"tg://openmessage?user_id={uid}\">{escape_html(fname)}</a>"
        winners_mentions.append(mention)

    result_text = (
        "🎉 <b>Результаты розыгрыша!</b>\n\n"
        f"Победители ({winners_count}):\n"
        + "\n".join(winners_mentions)
        + f"\n\nКаждому начислено <b>{per_winner}</b> баллов."
    )

    await context.bot.send_message(
        chat_id=chat_id,
        text=result_text,
        reply_to_message_id=replied.message_id,
        parse_mode='HTML'
    )
    mark_giveaway_finished(gid)

# Самопингование
async def self_ping(context):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{Config.RENDER_APP_URL}/ping") as resp:
                logger.info(f"Self-ping status: {resp.status}")
    except Exception as e:
        logger.error(f"Self-ping error: {str(e)}")

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
    
    # Получаем и сохраняем список команд бота из @BotFather при старте
    try:
        logger.info("Fetching bot commands from BotFather...")
        commands = await application.bot.get_my_commands()
        # Сохраняем только имена команд в set для быстрого поиска
        application.bot_data['bot_commands'] = {c.command for c in commands}
        logger.info(f"Loaded {len(application.bot_data['bot_commands'])} commands: {application.bot_data['bot_commands']}")
    except Exception as e:
        logger.error(f"Failed to fetch bot commands: {e}")
        application.bot_data['bot_commands'] = set()

# ---- MODIFIED: graceful shutdown with soft timeout ----
async def shutdown():
    global application
    GRACE_SECONDS = 5

    if application:
        logger.info("Starting graceful shutdown...")
        try:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            logger.info("Application stopped successfully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:
            application = None

    logger.info(f"Waiting up to {GRACE_SECONDS} seconds for background tasks to finish before force exit...")
    try:
        await asyncio.sleep(GRACE_SECONDS)
    except Exception as e:
        logger.error(f"Sleep interrupted during shutdown wait: {e}")

    logger.info("Finalizing shutdown: exiting process.")
    os._exit(0)

def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(shutdown())
    except RuntimeError:
        logger.info("Event loop not running — running shutdown synchronously.")
        try:
            asyncio.run(shutdown())
        except Exception as e:
            logger.error(f"Fallback synchronous shutdown failed: {e}")
            os._exit(1)

def main():
    global application
    
    # Запуск Flask в отдельном потоке
    threading.Thread(target=run_web_server, daemon=True).start()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Создание и настройка бота
    application = ApplicationBuilder()\
        .token(Config.BOT_TOKEN)\
        .post_init(post_init)\
        .build()
    
    allowed_chat = filters.Chat(chat_id=Config.ALLOWED_CHAT_IDS)
    
    application.add_handler(
        MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, handle_new_chat)
    )

    # Регистрация обработчиков
    # Команда help доступна везде (нет фильтра allowed_chat)
    application.add_handler(CommandHandler("help", help_command)) 
    
    # Остальные команды доступны только в разрешенных чатах
    application.add_handler(CommandHandler("add", add_points, filters=allowed_chat))
    application.add_handler(CommandHandler("remove", remove_points, filters=allowed_chat))
    application.add_handler(CommandHandler("my", my_points, filters=allowed_chat))
    application.add_handler(CommandHandler("points", check_points, filters=allowed_chat))
    application.add_handler(CommandHandler("top", top_users, filters=allowed_chat))
    application.add_handler(CommandHandler("export_data", export_chat_data, filters=allowed_chat))
    application.add_handler(CommandHandler("restore_data", import_chat_data, filters=allowed_chat))

    # Giveaway handlers
    application.add_handler(CommandHandler("create_giveaway", create_giveaway))
    application.add_handler(CommandHandler("cancel_giveaway", cancel_giveaway, filters=filters.ChatType.PRIVATE))
    
    # Handlers for RequestChat and RequestUser (New Feature)
    application.add_handler(MessageHandler(filters.StatusUpdate.CHAT_SHARED, handle_chat_shared))
    application.add_handler(MessageHandler(filters.StatusUpdate.USERS_SHARED, handle_users_shared))

    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_giveaway_text_response))
    application.add_handler(CommandHandler("end_giveaway", end_giveaway, filters=allowed_chat))

    # Handler для отклонения команд в неавторизованных чатах
    # Фильтр: 
    # 1. Это команда (COMMAND)
    # 2. Чат НЕ в списке разрешенных (~filters.Chat)
    # 3. Команда НЕ является /help (в функции reject_unauthorized_command)
    unauthorized_filter = (
        filters.COMMAND & 
        ~allowed_chat
    )
    application.add_handler(MessageHandler(unauthorized_filter, reject_unauthorized_command))

    try:
        application.run_polling(
            drop_pending_updates=True,
            close_loop=False
        )
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        if application and application.running:
            try:
                asyncio.run(shutdown())
            except Exception as e:
                logger.error(f"Error running shutdown from finally: {e}")
                os._exit(1)

if __name__ == "__main__":
    main()
