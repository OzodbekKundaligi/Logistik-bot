"""
Telegram logistics bot (single-file project).

Features:
- User onboarding with /start (name, surname, phone, role)
- Driver profile form
- Cargo posting form for shippers
- Auto-post cargo to catalog chat + region chats (12 regions)
- Profile, profile analysis, market statistics
- Pro subscription management
- Admin panel with broadcast, stats, pro controls, channel mapping

Environment variables:
- BOT_TOKEN=...
- MONGODB_URI=mongodb://127.0.0.1:27017
- MONGODB_DB=logistik_bot
- ADMIN_IDS=123456789,987654321
- SUPPORT_CONTACT=@your_support
- NEWS_CHANNEL=https://t.me/your_channel
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Optional

from aiogram import Bot, Dispatcher, F
from aiogram import BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING, ReturnDocument


@dataclass
class Config:
    bot_token: str
    mongodb_uri: str
    mongodb_db: str
    admin_ids: set[int]
    support_contact: str
    news_channel: str


REGIONS = [
    "Andijon",
    "Buxoro",
    "Farg'ona",
    "Jizzax",
    "Namangan",
    "Navoiy",
    "Qashqadaryo",
    "Samarqand",
    "Sirdaryo",
    "Surxondaryo",
    "Toshkent",
    "Xorazm",
]

ROLE_DRIVER = "driver"
ROLE_SHIPPER = "shipper"
LANG_UZ = "uz"
LANG_RU = "ru"

LANG_SELECT_UZ = "🇺🇿 O'zbekcha"
LANG_SELECT_RU = "🇷🇺 Русский"
LANG_LABEL_TO_CODE = {
    LANG_SELECT_UZ: LANG_UZ,
    LANG_SELECT_RU: LANG_RU,
}
VALID_LANGS = {LANG_UZ, LANG_RU}

ROLE_LABELS = {
    ROLE_DRIVER: "🚛 Haydovchi",
    ROLE_SHIPPER: "📦 Yuk beruvchi",
}
ROLE_LABELS_RU = {
    ROLE_DRIVER: "🚛 Водитель",
    ROLE_SHIPPER: "📦 Грузоотправитель",
}

LABEL_TO_ROLE = {
    "🚛 Haydovchi": ROLE_DRIVER,
    "📦 Yuk beruvchi": ROLE_SHIPPER,
    "🚛 Водитель": ROLE_DRIVER,
    "📦 Грузоотправитель": ROLE_SHIPPER,
}

PAYMENT_OPTIONS = ["💵 Naqd", "💳 Karta", "🏦 O'tkazma"]
PAYMENT_OPTIONS_RU = ["💵 Наличные", "💳 Карта", "🏦 Перевод"]
PAYMENT_TO_CANON = {
    PAYMENT_OPTIONS[0]: PAYMENT_OPTIONS[0],
    PAYMENT_OPTIONS[1]: PAYMENT_OPTIONS[1],
    PAYMENT_OPTIONS[2]: PAYMENT_OPTIONS[2],
    PAYMENT_OPTIONS_RU[0]: PAYMENT_OPTIONS[0],
    PAYMENT_OPTIONS_RU[1]: PAYMENT_OPTIONS[1],
    PAYMENT_OPTIONS_RU[2]: PAYMENT_OPTIONS[2],
}

BTN_CANCEL = "❌ Bekor qilish"
BTN_BACK_MAIN = "⬅️ Asosiy menyu"
BTN_BACK_ADMIN = "🔙 Admin panel"
BTN_SKIP = "⏭ O'tkazib yuborish"

BTN_ADMIN_PANEL = "🛠 Admin panel"
BTN_BROADCAST = "📣 Habar yuborish"
BTN_ADMIN_STATS = "📊 Tizim statistikasi"
BTN_ADMIN_USERS = "📋 Foydalanuvchilar"
BTN_ADMIN_PRO = "💎 Pro boshqaruvi"
BTN_ADMIN_CHANNELS = "🌐 Kanal/Guruh sozlash"
BTN_ADMIN_GUIDE = "📘 Admin yo'riqnoma"
BTN_PRO_ADD = "➕ Pro qo'shish"
BTN_PRO_REMOVE = "➖ Pro o'chirish"
BTN_CH_SET_CATALOG = "📚 Katalog chat ID"
BTN_CH_SET_REGION = "🗺 Viloyat chat ID"
BTN_CH_LIST = "📋 Ulangan chatlar"
BTN_REQ_ADD = "➕ Majburiy kanal qo'shish"
BTN_REQ_REMOVE = "➖ Majburiy kanal o'chirish"
BTN_REQ_LIST = "📌 Majburiy kanallar"

BTN_BC_ALL = "👥 Barchaga"
BTN_BC_DRIVERS = "🚛 Haydovchilarga"
BTN_BC_SHIPPERS = "📦 Yuk beruvchilarga"
BTN_BC_PRO = "💎 Pro foydalanuvchilarga"

BTN_MENU_CARGO = "📦 Yuk joylash"
BTN_MENU_DRIVER = "🚛 Haydovchi anketasi"
BTN_MENU_PROFILE = "👤 Profilim"
BTN_MENU_ANALYSIS = "🧠 Profil tahlili"
BTN_MENU_STATS = "📊 Statistika"
BTN_MENU_PRO = "💎 Pro tarif"
BTN_MENU_NEWS = "📣 Yangiliklar"
BTN_MENU_CONTACT = "☎️ Bog'lanish"
BTN_MENU_SETTINGS = "⚙️ Sozlamalar"
BTN_SETTINGS_ROLE = "🔄 Rolni almashtirish"
BTN_SETTINGS_LANG = "🌐 Tilni almashtirish"

MENU_INTERRUPT_BUTTONS = (
    BTN_CANCEL,
    BTN_BACK_MAIN,
    BTN_BACK_ADMIN,
    BTN_MENU_CARGO,
    BTN_MENU_DRIVER,
    BTN_MENU_PROFILE,
    BTN_MENU_ANALYSIS,
    BTN_MENU_STATS,
    BTN_MENU_PRO,
    BTN_MENU_NEWS,
    BTN_MENU_CONTACT,
    BTN_MENU_SETTINGS,
    BTN_SETTINGS_ROLE,
    BTN_SETTINGS_LANG,
    BTN_ADMIN_PANEL,
    BTN_ADMIN_STATS,
    BTN_ADMIN_USERS,
    BTN_BROADCAST,
    BTN_ADMIN_PRO,
    BTN_PRO_ADD,
    BTN_PRO_REMOVE,
    BTN_ADMIN_CHANNELS,
    BTN_CH_SET_CATALOG,
    BTN_CH_SET_REGION,
    BTN_CH_LIST,
    BTN_REQ_ADD,
    BTN_REQ_REMOVE,
    BTN_REQ_LIST,
    BTN_ADMIN_GUIDE,
)
BTN_CARGO_CONFIRM = "✅ Guruhlarga yuborish"
BTN_CARGO_EDIT = "✏️ Tahrirlash"

RU_BUTTON_TEXTS = {
    BTN_CANCEL: "❌ Отмена",
    BTN_BACK_MAIN: "⬅️ Главное меню",
    BTN_BACK_ADMIN: "🔙 Админ панель",
    BTN_SKIP: "⏭ Пропустить",
    BTN_ADMIN_PANEL: "🛠 Админ панель",
    BTN_BROADCAST: "📣 Рассылка",
    BTN_ADMIN_STATS: "📊 Системная статистика",
    BTN_ADMIN_USERS: "📋 Пользователи",
    BTN_ADMIN_PRO: "💎 Управление Pro",
    BTN_ADMIN_CHANNELS: "🌐 Настройка каналов/групп",
    BTN_ADMIN_GUIDE: "📘 Инструкция админа",
    BTN_PRO_ADD: "➕ Добавить Pro",
    BTN_PRO_REMOVE: "➖ Удалить Pro",
    BTN_CH_SET_CATALOG: "📚 ID каталога",
    BTN_CH_SET_REGION: "🗺 ID по области",
    BTN_CH_LIST: "📋 Подключенные чаты",
    BTN_REQ_ADD: "➕ Добавить обязательный канал",
    BTN_REQ_REMOVE: "➖ Удалить обязательный канал",
    BTN_REQ_LIST: "📌 Обязательные каналы",
    BTN_BC_ALL: "👥 Всем",
    BTN_BC_DRIVERS: "🚛 Водителям",
    BTN_BC_SHIPPERS: "📦 Грузоотправителям",
    BTN_BC_PRO: "💎 Pro пользователям",
    BTN_MENU_CARGO: "📦 Разместить груз",
    BTN_MENU_DRIVER: "🚛 Анкета водителя",
    BTN_MENU_PROFILE: "👤 Мой профиль",
    BTN_MENU_ANALYSIS: "🧠 Анализ профиля",
    BTN_MENU_STATS: "📊 Статистика",
    BTN_MENU_PRO: "💎 Pro тариф",
    BTN_MENU_NEWS: "📣 Новости",
    BTN_MENU_CONTACT: "☎️ Связь",
    BTN_MENU_SETTINGS: "⚙️ Настройки",
    BTN_SETTINGS_ROLE: "🔄 Сменить роль",
    BTN_SETTINGS_LANG: "🌐 Сменить язык",
    BTN_CARGO_CONFIRM: "✅ Отправить в группы",
    BTN_CARGO_EDIT: "✏️ Изменить",
    "📲 Raqam yuborish": "📲 Отправить номер",
    ROLE_LABELS[ROLE_DRIVER]: ROLE_LABELS_RU[ROLE_DRIVER],
    ROLE_LABELS[ROLE_SHIPPER]: ROLE_LABELS_RU[ROLE_SHIPPER],
    PAYMENT_OPTIONS[0]: PAYMENT_OPTIONS_RU[0],
    PAYMENT_OPTIONS[1]: PAYMENT_OPTIONS_RU[1],
    PAYMENT_OPTIONS[2]: PAYMENT_OPTIONS_RU[2],
}

TEXT_CANON_MAP = {v: k for k, v in RU_BUTTON_TEXTS.items()}
TEXT_CANON_MAP.update(
    {
        LANG_SELECT_UZ: LANG_SELECT_UZ,
        LANG_SELECT_RU: LANG_SELECT_RU,
        ROLE_LABELS[ROLE_DRIVER]: ROLE_LABELS[ROLE_DRIVER],
        ROLE_LABELS[ROLE_SHIPPER]: ROLE_LABELS[ROLE_SHIPPER],
        ROLE_LABELS_RU[ROLE_DRIVER]: ROLE_LABELS[ROLE_DRIVER],
        ROLE_LABELS_RU[ROLE_SHIPPER]: ROLE_LABELS[ROLE_SHIPPER],
        PAYMENT_OPTIONS[0]: PAYMENT_OPTIONS[0],
        PAYMENT_OPTIONS[1]: PAYMENT_OPTIONS[1],
        PAYMENT_OPTIONS[2]: PAYMENT_OPTIONS[2],
        PAYMENT_OPTIONS_RU[0]: PAYMENT_OPTIONS[0],
        PAYMENT_OPTIONS_RU[1]: PAYMENT_OPTIONS[1],
        PAYMENT_OPTIONS_RU[2]: PAYMENT_OPTIONS[2],
    }
)


def canonicalize_user_text(text: Optional[str]) -> str:
    raw = (text or "").strip()
    return TEXT_CANON_MAP.get(raw, raw)


RU_TEXT_TRANSLATIONS = {
    "Asosiy menyu.": "Главное меню.",
    "Asosiy menyu": "Главное меню",
    "Kerakli bo'limni menyudan tanlang.": "Выберите нужный раздел в меню.",
    "Jarayon bekor qilindi.": "Действие отменено.",
    "Xush kelibsiz! Asosiy menyudan bo'limni tanlang.": "Добро пожаловать! Выберите раздел в главном меню.",
    "Profilingiz tugallanmagan. /start orqali davom eting.": "Ваш профиль не завершен. Продолжите через /start.",
    "Profilingiz tugallanmagan. /start ni bosing.": "Ваш профиль не завершен. Нажмите /start.",
    "Profilingiz tugallanmagan. Avval /start orqali to'ldiring.": "Профиль не завершен. Сначала заполните через /start.",
    "Buyruq topilmadi. Menyudan foydalaning yoki /start ni bosing.": "Команда не найдена. Используйте меню или нажмите /start.",
    "Yangi rolni tanlang:": "Выберите новую роль:",
    "Rolni tugmadan tanlang.": "Выберите роль кнопкой.",
    "Pastdagi tugmalardan birini tanlang.": "Выберите одну из кнопок ниже.",
    "Sizning rolingizni tanlang:": "Выберите вашу роль:",
    "Telefon formati noto'g'ri. Masalan: +998901234567": "Неверный формат телефона. Пример: +998901234567",
    "Ism kamida 2 ta harf bo'lsin. Qayta kiriting:": "Имя должно быть не короче 2 символов. Введите снова:",
    "Familiya kamida 2 ta harf bo'lsin. Qayta kiriting:": "Фамилия должна быть не короче 2 символов. Введите снова:",
    "Telefon raqamingizni yuboring:": "Отправьте номер телефона:",
    "Familiyangizni kiriting:": "Введите фамилию:",
    "Ismingizni kiriting:": "Введите имя:",
    "Assalomu alaykum!": "Здравствуйте!",
    "Logistik platformaga xush kelibsiz.": "Добро пожаловать в логистическую платформу.",
    "Ro'yxatdan o'tish tugadi. Endi yuk joylashingiz mumkin.": "Регистрация завершена. Теперь вы можете размещать груз.",
    "Haydovchi anketasi saqlandi.": "Анкета водителя сохранена.",
    "Yuk e'loningiz saqlandi va yuborildi.": "Ваше объявление сохранено и отправлено.",
    "Yuborilgan chatlar": "Отправлено чатов",
    "Hech bir chat ulanmagan. Admin paneldan katalog/viloyat chat ID larni kiriting.": "Ни один чат не подключен. Укажите ID каталога/областных чатов в админ панели.",
    "Yuborishda xatolar": "Ошибок отправки",
    "Sabab:": "Причина:",
    "Yuk qayerdan yuklanadi? Viloyatni tanlang:": "Откуда загружается груз? Выберите область:",
    "Yuk qayerga boradi? Viloyatni tanlang:": "Куда отправляется груз? Выберите область:",
    "Yuk turini kiriting (masalan: sement, mebel, oziq-ovqat):": "Введите тип груза (например: цемент, мебель, продукты):",
    "Og'irligini kiriting (tonna):": "Введите вес (тонна):",
    "Hajmini kiriting (m3):": "Введите объем (м3):",
    "Taklif narxini kiriting (so'm):": "Введите предлагаемую цену (сум):",
    "Yuklash sanasi (masalan: 25.02.2026 yoki bugun):": "Дата загрузки (например: 25.02.2026 или сегодня):",
    "To'lov turini tanlang:": "Выберите способ оплаты:",
    "Qo'shimcha izoh (ixtiyoriy):": "Дополнительный комментарий (необязательно):",
    "Tahrirlash boshlandi. Qayerdan yuklanadi?": "Редактирование начато. Откуда загружается груз?",
    "Viloyatni tugmadan tanlang.": "Выберите область кнопкой.",
    "Raqam kiriting. Masalan: 86": "Введите число. Например: 86",
    "Raqam kiriting. Masalan: 22": "Введите число. Например: 22",
    "Raqam kiriting. Masalan: 20": "Введите число. Например: 20",
    "Narxni raqamda kiriting. Masalan: 2500000": "Введите цену числом. Например: 2500000",
    "Yuklash sanasini kiriting.": "Введите дату загрузки.",
    "To'lov turini tugmadan tanlang.": "Выберите тип оплаты кнопкой.",
    "Yuk turini to'liqroq kiriting.": "Укажите тип груза подробнее.",
    "Mashina turi juda qisqa. Qayta kiriting:": "Тип машины слишком короткий. Введите снова:",
    "Qaysi yo'nalishlarda ishlaysiz? (masalan: Toshkent-Samarqand-Farg'ona)": "По каким маршрутам работаете? (например: Ташкент-Самарканд-Фергана)",
    "Yo'nalishni to'liqroq yozing.": "Укажите маршрут подробнее.",
    "1 km uchun narx (ixtiyoriy):": "Цена за 1 км (необязательно):",
    "Raqam kiriting yoki `⏭ O'tkazib yuborish` ni bosing.": "Введите число или нажмите `⏭ Пропустить`.",
    "Sozlamalar:": "Настройки:",
    "Bog'lanish:": "Связь:",
    "Yangiliklar bo'limi hali sozlanmagan.": "Раздел новостей пока не настроен.",
    "Yangiliklar kanali:": "Канал новостей:",
    "Tilni tanlang / Выберите язык:": "Выберите язык:",
    "Tilni tugma orqali tanlang / Выберите язык кнопкой.": "Выберите язык кнопкой.",
    "Til saqlandi. Asosiy menyu.": "Язык сохранен. Главное меню.",
    "Botdan foydalanish uchun majburiy obuna kerak": "Для использования бота требуется обязательная подписка",
    "Quyidagi kanal(lar)ga obuna bo'ling va `✅ Tekshirish` ni bosing:": "Подпишитесь на следующие каналы и нажмите `✅ Проверить`:",
    "Hali barcha kanallarga obuna bo'lmadingiz.": "Вы еще не подписались на все каналы.",
    "Obuna tasdiqlandi. Davom etishingiz mumkin.": "Подписка подтверждена. Можете продолжить.",
    "Obuna tasdiqlandi. Endi /start ni bosing.": "Подписка подтверждена. Теперь нажмите /start.",
    "Admin uchun obuna tekshiruvi shart emas.": "Для админа проверка подписки не требуется.",
    "PRO foydalanuvchi afzalliklari:": "Преимущества PRO пользователя:",
    "E'lonlar ajratib ko'rsatiladi": "Объявления выделяются",
    "Yuqoriroq ko'rinish imkoniyati": "Приоритетный показ",
    "Tezkor navbat": "Быстрая очередь",
    "Tariflar (misol):": "Тарифы (пример):",
    "Ulash uchun admin bilan bog'laning.": "Для подключения свяжитесь с админом.",
    "Rolingiz yuk beruvchi qilib yangilandi.": "Ваша роль обновлена на грузоотправителя.",
    "Rolingiz haydovchi qilib yangilandi.": "Ваша роль обновлена на водителя.",
    "Profil ma'lumotlari": "Данные профиля",
    "Ism:": "Имя:",
    "Familiya:": "Фамилия:",
    "Telefon:": "Телефон:",
    "Status:": "Статус:",
    "Rol:": "Роль:",
    "Haydovchi anketasi": "Анкета водителя",
    "Mashina ma'lumoti": "Информация о машине",
    "Turi:": "Тип:",
    "Sig'imi:": "Грузоподъемность:",
    "Hajmi:": "Объем:",
    "Yo'nalish:": "Маршрут:",
    "Izoh:": "Комментарий:",
    "Nomer ko'rish": "Показать номер",
    "Xabarga o'tish": "Перейти к сообщению",
    "Tekshirish": "Проверить",
    "🚛 Haydovchi": "🚛 Водитель",
    "📦 Yuk beruvchi": "📦 Грузоотправитель",
    "Belgilanmagan": "Не указано",
    "Oddiy": "Обычный",
    "Foydalanuvchi topilmadi.": "Пользователь не найден.",
    "Xatolik: foydalanuvchi topilmadi.": "Ошибка: пользователь не найден.",
    "Xatolik": "Ошибка",
    "Format": "Формат",
    "Masalan": "Например",
    "Noto'g'ri format. Raqam kiriting.": "Неверный формат. Введите число.",
    "Kun soni 0 dan katta bo'lishi kerak.": "Количество дней должно быть больше 0.",
    "Tugash sanasi": "Дата окончания",
    "Tugash": "Окончание",
    "Pro qo'shildi.": "Pro добавлен.",
    "Pro o'chirildi": "Pro удален",
    "Katalog chat saqlandi": "Чат каталога сохранен",
    "chati saqlandi": "чат сохранен",
    "chat saqlandi": "чат сохранен",
    "Tekshiruv": "Проверка",
    "Botni shu chatga admin/member qilib qo'shing va qayta tekshiring.": "Добавьте бота в этот чат как администратора/участника и проверьте снова.",
    "Viloyatni tanlang:": "Выберите область:",
    "Kanal/Guruh sozlash": "Настройка каналов/групп",
    "Majburiy kanal": "Обязательный канал",
    "Majburiy kanallar": "Обязательные каналы",
    "Katalog chat": "Чат каталога",
    "Yuborildi": "Отправлено",
    "Xato": "Ошибка",
    "Foydalanuvchilar topilmadi.": "Пользователи не найдены.",
    "Oxirgi foydalanuvchilar (20 ta)": "Последние пользователи (20)",
    "Qaysi auditoriyaga yuborilsin?": "Кому отправить рассылку?",
    "Yuboriladigan xabarni yuboring (text/photo/video ham bo'lishi mumkin).": "Отправьте сообщение для рассылки (можно текст/фото/видео).",
    "Broadcast yakunlandi.": "Рассылка завершена.",
    "Admin statistika": "Статистика администратора",
    "Foydalanuvchilar:": "Пользователи:",
    "Haydovchilar:": "Водители:",
    "Yuk beruvchilar:": "Грузоотправители:",
    "Jami yuk e'lonlari:": "Всего объявлений:",
    "Narx-navo": "Цены",
    "Ulangan viloyatlar:": "Подключенные области:",
    "Ulanmagan": "Не подключено",
    "Ulanmagan:": "Не подключено:",
    "Kanal/Guruh ulanishi": "Подключение каналов/групп",
    "Admin yo'riqnoma (to'liq)": "Инструкция админа (полная)",
    "Bog'lanish": "Связь",
    "Sozlamalar": "Настройки",
}
RU_TEXT_TRANSLATION_ITEMS = sorted(RU_TEXT_TRANSLATIONS.items(), key=lambda kv: len(kv[0]), reverse=True)


class RegistrationFSM(StatesGroup):
    first_name = State()
    last_name = State()
    phone = State()
    role = State()


class LanguageFSM(StatesGroup):
    select = State()


class DriverFSM(StatesGroup):
    car_type = State()
    capacity_ton = State()
    volume_m3 = State()
    routes = State()
    price_per_km = State()
    note = State()


class CargoFSM(StatesGroup):
    from_region = State()
    to_region = State()
    cargo_type = State()
    weight_ton = State()
    volume_m3 = State()
    price = State()
    load_date = State()
    payment_type = State()
    comment = State()
    confirm = State()


class SettingsFSM(StatesGroup):
    role = State()


class AdminBroadcastFSM(StatesGroup):
    audience = State()
    content = State()


class AdminProFSM(StatesGroup):
    add = State()
    remove = State()


class AdminChannelFSM(StatesGroup):
    catalog_chat = State()
    region_select = State()
    region_chat = State()
    required_add = State()
    required_remove = State()


CONFIG: Optional[Config] = None

mongo_client: Optional[AsyncIOMotorClient] = None
db = None
users_col = None
cargo_col = None
settings_col = None
region_channels_col = None
admins_col = None

dp = Dispatcher(storage=MemoryStorage())

BOT_USERNAME_CACHE: Optional[str] = None

# Private-only mode: all handlers work only in 1:1 chat with the bot.
dp.message.filter(F.chat.type == "private")
dp.callback_query.filter(F.message.chat.type == "private")


def is_valid_lang(value: Any) -> bool:
    return isinstance(value, str) and value in VALID_LANGS


def localize_button_text(text: str, lang: str) -> str:
    if lang != LANG_RU:
        return text
    return RU_BUTTON_TEXTS.get(text, text)


def translate_text(text: str, lang: str) -> str:
    if lang != LANG_RU:
        return text
    output = text
    for src, dst in RU_TEXT_TRANSLATION_ITEMS:
        output = output.replace(src, dst)
    return output


def translate_reply_markup(reply_markup: Any, lang: str) -> Any:
    if lang != LANG_RU or reply_markup is None:
        return reply_markup

    if isinstance(reply_markup, ReplyKeyboardMarkup):
        keyboard: list[list[KeyboardButton]] = []
        for row in reply_markup.keyboard:
            new_row: list[KeyboardButton] = []
            for btn in row:
                payload = btn.model_dump(exclude_none=True)
                if "text" in payload:
                    payload["text"] = localize_button_text(payload["text"], lang)
                new_row.append(KeyboardButton(**payload))
            keyboard.append(new_row)
        return ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=reply_markup.resize_keyboard,
            one_time_keyboard=reply_markup.one_time_keyboard,
            selective=reply_markup.selective,
            input_field_placeholder=reply_markup.input_field_placeholder,
            is_persistent=reply_markup.is_persistent,
        )

    if isinstance(reply_markup, InlineKeyboardMarkup):
        inline_keyboard: list[list[InlineKeyboardButton]] = []
        for row in reply_markup.inline_keyboard:
            new_row: list[InlineKeyboardButton] = []
            for btn in row:
                payload = btn.model_dump(exclude_none=True)
                if "text" in payload:
                    payload["text"] = localize_button_text(payload["text"], lang)
                    payload["text"] = translate_text(payload["text"], lang)
                new_row.append(InlineKeyboardButton(**payload))
            inline_keyboard.append(new_row)
        return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    return reply_markup


async def get_user_lang(user_id: int, default: str = LANG_UZ) -> str:
    if users_col is None:
        return default
    user = await users_col.find_one({"_id": user_id}, {"lang": 1})
    if user and is_valid_lang(user.get("lang")):
        return user["lang"]
    return default


async def set_user_lang(user_id: int, lang: str) -> None:
    if not is_valid_lang(lang):
        return
    await users_col.update_one({"_id": user_id}, {"$set": {"lang": lang, "updated_at": now_utc()}}, upsert=True)


def language_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=LANG_SELECT_UZ), KeyboardButton(text=LANG_SELECT_RU)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


class CanonicalizeInputMiddleware(BaseMiddleware):
    async def __call__(self, handler: Any, event: Message, data: dict[str, Any]) -> Any:
        if isinstance(event, Message) and event.text:
            mapped = canonicalize_user_text(event.text)
            if mapped != event.text:
                # aiogram Message modeli frozen; matnni model_copy orqali yangilaymiz.
                event = event.model_copy(update={"text": mapped})
        return await handler(event, data)


dp.message.outer_middleware(CanonicalizeInputMiddleware())


_ORIGINAL_MESSAGE_ANSWER = Message.answer


async def _localized_message_answer(self: Message, *args: Any, **kwargs: Any) -> Any:
    text_arg = None
    if args:
        text_arg = args[0]
    elif "text" in kwargs:
        text_arg = kwargs["text"]

    lang = LANG_UZ
    if self.from_user:
        lang = await get_user_lang(self.from_user.id, default=LANG_UZ)

    if isinstance(text_arg, str):
        translated = translate_text(text_arg, lang)
        if args:
            args = (translated, *args[1:])
        else:
            kwargs["text"] = translated

    if "reply_markup" in kwargs:
        kwargs["reply_markup"] = translate_reply_markup(kwargs["reply_markup"], lang)

    return await _ORIGINAL_MESSAGE_ANSWER(self, *args, **kwargs)


Message.answer = _localized_message_answer


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def normalize_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def parse_phone(value: str) -> Optional[str]:
    cleaned = re.sub(r"\s+", "", value.strip())
    if re.fullmatch(r"\+?\d{9,15}", cleaned):
        return cleaned
    return None


def parse_positive_number(value: str) -> Optional[float]:
    cleaned = value.replace(" ", "").replace(",", ".").strip()
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def parse_chat_id(value: str) -> Optional[int]:
    try:
        chat_id = int(value.strip())
    except ValueError:
        return None
    if chat_id == 0:
        return None
    return chat_id


CHAT_USERNAME_RE = re.compile(r"^@[A-Za-z0-9_]{5,}$")
CHAT_PUBLIC_LINK_RE = re.compile(
    r"^(?:https?://)?(?:t\.me|telegram\.me)/(?:s/)?([A-Za-z0-9_]{5,})(?:/\d+)?/?(?:\?.*)?$"
)
CHAT_INTERNAL_LINK_RE = re.compile(
    r"^(?:https?://)?(?:t\.me|telegram\.me)/c/(\d+)(?:/\d+)?/?(?:\?.*)?$"
)
CHAT_INVITE_LINK_RE = re.compile(r"^(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)[A-Za-z0-9_-]+/?$")


def parse_chat_reference(value: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    raw = value.strip()
    if not raw:
        return None, None, "Chat ma'lumoti kiritilmadi."

    numeric_id = parse_chat_id(raw)
    if numeric_id is not None:
        return numeric_id, None, None

    if CHAT_USERNAME_RE.fullmatch(raw):
        return None, raw, None

    internal_match = CHAT_INTERNAL_LINK_RE.fullmatch(raw)
    if internal_match:
        internal_id = internal_match.group(1)
        return int(f"-100{internal_id}"), None, None

    public_match = CHAT_PUBLIC_LINK_RE.fullmatch(raw)
    if public_match:
        return None, f"@{public_match.group(1)}", None

    if CHAT_INVITE_LINK_RE.fullmatch(raw):
        return None, None, (
            "❗ `+` yoki `joinchat` invite-link orqali chat ID avtomatik olinmaydi.\n"
            "Botni o'sha chatga qo'shing va:\n"
            "• chatdan forward xabar yuboring yoki\n"
            "• `@username` / `-100...` yuboring."
        )

    return None, None, (
        "Chat formati noto'g'ri.\n"
        "Qabul qilinadi: `-100...`, `@username`, `https://t.me/username`, `https://t.me/username/123`, `https://t.me/c/...`"
    )


def extract_chat_id_from_message(message: Message) -> Optional[int]:
    forwarded_chat = getattr(message, "forward_from_chat", None)
    if forwarded_chat and getattr(forwarded_chat, "id", None):
        return int(forwarded_chat.id)

    forward_origin = getattr(message, "forward_origin", None)
    if forward_origin is not None:
        origin_chat = getattr(forward_origin, "chat", None)
        if origin_chat and getattr(origin_chat, "id", None):
            return int(origin_chat.id)
        origin_sender_chat = getattr(forward_origin, "sender_chat", None)
        if origin_sender_chat and getattr(origin_sender_chat, "id", None):
            return int(origin_sender_chat.id)

    sender_chat = getattr(message, "sender_chat", None)
    if sender_chat and getattr(sender_chat, "id", None):
        return int(sender_chat.id)

    if message.text:
        return parse_chat_id(message.text)
    return None


async def resolve_chat_id_from_text(bot: Bot, value: str) -> tuple[Optional[int], Optional[str]]:
    numeric_id, username, parse_error = parse_chat_reference(value)
    if parse_error:
        return None, parse_error
    if numeric_id is not None:
        return numeric_id, None
    if not username:
        return None, "Chat topilmadi."

    try:
        chat = await bot.get_chat(username)
        return int(chat.id), None
    except Exception:  # noqa: BLE001
        return None, (
            f"`{username}` chatiga ulanib bo'lmadi.\n"
            "Bot shu chatga qo'shilganini va username to'g'ri ekanini tekshiring."
        )


async def resolve_chat_id_from_message(message: Message) -> tuple[Optional[int], Optional[str]]:
    direct_chat_id = extract_chat_id_from_message(message)
    if direct_chat_id is not None:
        return direct_chat_id, None

    if not message.text:
        return None, (
            "Chat ID topilmadi.\n"
            "ID (`-100...`), `@username`, `https://t.me/...` link yoki forward xabar yuboring."
        )

    return await resolve_chat_id_from_text(message.bot, message.text)


def normalize_send_error(exc: Exception) -> str:
    err = str(exc)
    lowered = err.lower()
    if "forbidden" in lowered or "not enough rights" in lowered:
        return "Botda yozish huquqi yo'q (admin emas yoki post/send ruxsati yo'q)."
    if "chat not found" in lowered:
        return "Chat topilmadi (ID/link noto'g'ri yoki bot chatga qo'shilmagan)."
    if "blocked" in lowered:
        return "Bot bloklangan yoki chatdan chiqarilgan."
    return err


async def check_chat_writable(bot: Bot, chat_id: int) -> tuple[bool, str]:
    try:
        me = await bot.get_me()
        chat = await bot.get_chat(chat_id)
        member = await bot.get_chat_member(chat_id, me.id)
    except Exception as exc:  # noqa: BLE001
        return False, normalize_send_error(exc)

    chat_type = getattr(chat.type, "value", str(chat.type))
    status = getattr(member.status, "value", str(member.status))

    if status in {"left", "kicked"}:
        return False, "Bot chatda yo'q. Botni chatga qo'shing."

    # Channelda post qilish uchun admin bo'lishi kerak.
    if chat_type == "channel":
        if status not in {"administrator", "creator"}:
            return False, "Bu kanal uchun bot admin bo'lishi shart."
        can_post = getattr(member, "can_post_messages", None)
        if can_post is False:
            return False, "Bot admin, lekin `Post messages` huquqi o'chirilgan."
        return True, "Kanalga yuborish huquqi bor."

    # Guruhlarda oddiy member ham yozishi mumkin (agar restricted bo'lmasa).
    can_send = getattr(member, "can_send_messages", None)
    if can_send is False:
        return False, "Bot bu guruhda yozishga cheklangan."
    return True, "Chatga yuborish huquqi bor."


def extract_start_payload(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    return payload or None


async def get_bot_username(bot: Bot) -> str:
    global BOT_USERNAME_CACHE
    if BOT_USERNAME_CACHE:
        return BOT_USERNAME_CACHE
    me = await bot.get_me()
    BOT_USERNAME_CACHE = me.username or ""
    return BOT_USERNAME_CACHE


def build_bot_start_link(bot_username: str, payload: str) -> str:
    return f"https://t.me/{bot_username}?start={payload}"


def build_cargo_inline_keyboard(bot_username: str, cargo_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="☎️ Nomer ko'rish", url=build_bot_start_link(bot_username, f"phone_{cargo_id}"))],
            [InlineKeyboardButton(text="✉️ Xabarga o'tish", url=build_bot_start_link(bot_username, f"cargo_{cargo_id}"))],
        ]
    )


async def get_mandatory_channels() -> list[dict[str, Any]]:
    doc = await settings_col.find_one({"_id": "mandatory_channels"})
    if not doc:
        return []
    channels = doc.get("channels") or []
    if not isinstance(channels, list):
        return []
    result: list[dict[str, Any]] = []
    for item in channels:
        if not isinstance(item, dict):
            continue
        chat_id = item.get("chat_id")
        if not isinstance(chat_id, int):
            continue
        result.append(item)
    return result


async def set_mandatory_channels(channels: list[dict[str, Any]]) -> None:
    await settings_col.update_one(
        {"_id": "mandatory_channels"},
        {"$set": {"channels": channels, "updated_at": now_utc()}},
        upsert=True,
    )


def channel_join_url(channel: dict[str, Any]) -> Optional[str]:
    url = channel.get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()
    username = channel.get("username")
    if isinstance(username, str) and username.strip():
        return f"https://t.me/{username.strip()}"
    return None


async def add_mandatory_channel(bot: Bot, chat_id: int) -> tuple[bool, str]:
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as exc:  # noqa: BLE001
        return False, normalize_send_error(exc)

    title = chat.title or chat.full_name or str(chat_id)
    username = chat.username
    entry = {
        "chat_id": int(chat.id),
        "title": title,
        "username": username,
        "url": f"https://t.me/{username}" if username else None,
    }

    channels = await get_mandatory_channels()
    channels = [c for c in channels if c.get("chat_id") != entry["chat_id"]]
    channels.append(entry)
    await set_mandatory_channels(channels)
    return True, f"✅ Majburiy kanal qo'shildi: <b>{safe(title)}</b> (<code>{entry['chat_id']}</code>)"


async def remove_mandatory_channel(chat_id: int) -> bool:
    channels = await get_mandatory_channels()
    updated = [c for c in channels if c.get("chat_id") != chat_id]
    if len(updated) == len(channels):
        return False
    await set_mandatory_channels(updated)
    return True


async def mandatory_channels_overview_text() -> str:
    channels = await get_mandatory_channels()
    lines = ["📌 <b>Majburiy obuna kanallari</b>"]
    if not channels:
        lines.append("Hozircha majburiy kanal yo'q.")
        return "\n".join(lines)

    for idx, item in enumerate(channels, start=1):
        title = item.get("title") or item.get("username") or item.get("chat_id")
        chat_id = item.get("chat_id")
        username = item.get("username")
        if username:
            lines.append(f"{idx}. {safe(title)} | <code>{chat_id}</code> | @{safe(username)}")
        else:
            lines.append(f"{idx}. {safe(title)} | <code>{chat_id}</code>")
    return "\n".join(lines)


async def get_missing_mandatory_channels(bot: Bot, user_id: int) -> list[dict[str, Any]]:
    channels = await get_mandatory_channels()
    missing: list[dict[str, Any]] = []
    if not channels:
        return missing

    for channel in channels:
        chat_id = channel.get("chat_id")
        if not isinstance(chat_id, int):
            continue
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            status = getattr(member.status, "value", str(member.status))
            if status in {"member", "administrator", "creator", "restricted"}:
                continue
            missing.append(channel)
        except Exception:  # noqa: BLE001
            missing.append(channel)
    return missing


def mandatory_subscribe_keyboard(channels: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in channels:
        title = str(item.get("title") or item.get("username") or item.get("chat_id"))
        url = channel_join_url(item)
        if url:
            rows.append([InlineKeyboardButton(text=f"📢 {title}", url=url)])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def mandatory_subscribe_text(channels: list[dict[str, Any]]) -> str:
    lines = [
        "🔒 <b>Botdan foydalanish uchun majburiy obuna kerak</b>",
        "Quyidagi kanal(lar)ga obuna bo'ling va `✅ Tekshirish` ni bosing:",
    ]
    for item in channels:
        title = item.get("title") or item.get("username") or item.get("chat_id")
        lines.append(f"• {safe(title)}")
    return "\n".join(lines)


async def ensure_mandatory_subscription_message(message: Message) -> bool:
    if not message.from_user:
        return False
    if await is_admin_user(message.from_user.id):
        return True

    missing = await get_missing_mandatory_channels(message.bot, message.from_user.id)
    if not missing:
        return True

    await message.answer(
        mandatory_subscribe_text(missing),
        reply_markup=mandatory_subscribe_keyboard(missing),
        disable_web_page_preview=True,
    )
    return False


async def handle_start_payload(message: Message, payload: str) -> None:
    if payload.startswith("phone_"):
        cargo_id = payload[len("phone_") :]
    elif payload.startswith("cargo_"):
        cargo_id = payload[len("cargo_") :]
    else:
        return

    try:
        cargo_oid = ObjectId(cargo_id)
    except Exception:  # noqa: BLE001
        await message.answer("E'lon identifikatori noto'g'ri.")
        return

    cargo = await cargo_col.find_one({"_id": cargo_oid})
    if not cargo:
        await message.answer("E'lon topilmadi yoki o'chirilgan.")
        return

    owner = await fetch_user(cargo.get("owner_id"))
    if not owner:
        await message.answer("Aloqa ma'lumoti topilmadi.")
        return

    owner_name = f"{owner.get('first_name') or ''} {owner.get('last_name') or ''}".strip() or "Noma'lum"
    lines = [
        "📨 <b>E'lon bo'yicha aloqa</b>",
        f"📍 Yo'nalish: <b>{safe(cargo.get('from_region'))} -> {safe(cargo.get('to_region'))}</b>",
        f"📦 Yuk: <b>{safe(cargo.get('cargo_type'))}</b>",
        f"👤 Ism: <b>{safe(owner_name)}</b>",
        f"📞 Telefon: <b>{safe(owner.get('phone'))}</b>",
    ]

    keyboard: Optional[InlineKeyboardMarkup] = None
    username = owner.get("username")
    if isinstance(username, str) and username.strip():
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✉️ Telegramda yozish", url=f"https://t.me/{username.strip()}")]]
        )

    await message.answer("\n".join(lines), reply_markup=keyboard)


def format_money(value: Optional[float]) -> str:
    if value is None:
        return "Noma'lum"
    return f"{value:,.0f}".replace(",", " ")


def safe(value: Any) -> str:
    if value is None:
        return "-"
    return escape(str(value))


def is_pro_active(user: Optional[dict[str, Any]]) -> bool:
    if not user:
        return False
    pro_until = normalize_datetime(user.get("pro_until"))
    return bool(pro_until and pro_until > now_utc())


def role_label(role: Optional[str]) -> str:
    if role in ROLE_LABELS:
        return ROLE_LABELS[role]
    return "Belgilanmagan"


def normalize_region(raw: str) -> Optional[str]:
    value = raw.strip()
    value_key = re.sub(r"[^a-z0-9]", "", value.lower())
    for region in REGIONS:
        region_key = re.sub(r"[^a-z0-9]", "", region.lower())
        if value_key == region_key:
            return region
    return None


def main_menu_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    buttons = [
        BTN_MENU_CARGO,
        BTN_MENU_DRIVER,
        BTN_MENU_PROFILE,
        BTN_MENU_ANALYSIS,
        BTN_MENU_STATS,
        BTN_MENU_PRO,
        BTN_MENU_NEWS,
        BTN_MENU_CONTACT,
        BTN_MENU_SETTINGS,
    ]
    if is_admin:
        buttons.append(BTN_ADMIN_PANEL)
    builder = ReplyKeyboardBuilder()
    for text in buttons:
        builder.button(text=text)
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def phone_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📲 Raqam yuborish", request_contact=True)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def role_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=ROLE_LABELS[ROLE_DRIVER]), KeyboardButton(text=ROLE_LABELS[ROLE_SHIPPER])],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
        resize_keyboard=True,
    )


def skip_cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SKIP)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def payment_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=PAYMENT_OPTIONS[0]), KeyboardButton(text=PAYMENT_OPTIONS[1])],
            [KeyboardButton(text=PAYMENT_OPTIONS[2])],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def region_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    for region in REGIONS:
        builder.button(text=region)
    builder.button(text=BTN_CANCEL)
    builder.adjust(3, 3, 3, 3, 1)
    return builder.as_markup(resize_keyboard=True)


def cargo_confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CARGO_CONFIRM), KeyboardButton(text=BTN_CARGO_EDIT)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def settings_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SETTINGS_ROLE)],
            [KeyboardButton(text=BTN_SETTINGS_LANG)],
            [KeyboardButton(text=BTN_BACK_MAIN)],
        ],
        resize_keyboard=True,
    )


def admin_panel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BROADCAST)],
            [KeyboardButton(text=BTN_ADMIN_STATS), KeyboardButton(text=BTN_ADMIN_USERS)],
            [KeyboardButton(text=BTN_ADMIN_PRO), KeyboardButton(text=BTN_ADMIN_CHANNELS)],
            [KeyboardButton(text=BTN_ADMIN_GUIDE)],
            [KeyboardButton(text=BTN_BACK_MAIN)],
        ],
        resize_keyboard=True,
    )


def admin_pro_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_PRO_ADD), KeyboardButton(text=BTN_PRO_REMOVE)],
            [KeyboardButton(text=BTN_BACK_ADMIN)],
        ],
        resize_keyboard=True,
    )


def admin_channels_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CH_SET_CATALOG), KeyboardButton(text=BTN_CH_SET_REGION)],
            [KeyboardButton(text=BTN_REQ_ADD), KeyboardButton(text=BTN_REQ_REMOVE)],
            [KeyboardButton(text=BTN_REQ_LIST)],
            [KeyboardButton(text=BTN_CH_LIST)],
            [KeyboardButton(text=BTN_BACK_ADMIN)],
        ],
        resize_keyboard=True,
    )


def broadcast_audience_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BC_ALL)],
            [KeyboardButton(text=BTN_BC_DRIVERS), KeyboardButton(text=BTN_BC_SHIPPERS)],
            [KeyboardButton(text=BTN_BC_PRO)],
            [KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
    )


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception:  # noqa: BLE001
        pass


def load_config() -> Config:
    load_env_file()

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN topilmadi. Environment variable sifatida kiriting.")

    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017").strip()
    mongodb_db = os.getenv("MONGODB_DB", "logistik_bot").strip()

    admin_ids: set[int] = set()
    for chunk in os.getenv("ADMIN_IDS", "").split(","):
        value = chunk.strip()
        if not value:
            continue
        if value.isdigit():
            admin_ids.add(int(value))

    support_contact = os.getenv("SUPPORT_CONTACT", "@support").strip()
    news_channel = os.getenv("NEWS_CHANNEL", "").strip()

    return Config(
        bot_token=token,
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        admin_ids=admin_ids,
        support_contact=support_contact,
        news_channel=news_channel,
    )


async def init_database() -> None:
    global mongo_client, db, users_col, cargo_col, settings_col, region_channels_col, admins_col

    if CONFIG is None:
        raise RuntimeError("Config hali yuklanmagan.")

    mongo_client = AsyncIOMotorClient(CONFIG.mongodb_uri)
    db = mongo_client[CONFIG.mongodb_db]
    users_col = db["users"]
    cargo_col = db["cargo_posts"]
    settings_col = db["settings"]
    region_channels_col = db["region_channels"]
    admins_col = db["admins"]

    await users_col.create_index("role")
    await users_col.create_index("pro_until")
    await users_col.create_index("profile_completed")
    await users_col.create_index("updated_at", background=True)

    await cargo_col.create_index("owner_id")
    await cargo_col.create_index("created_at", background=True)
    await cargo_col.create_index([("from_region", 1), ("to_region", 1)], background=True)
    await cargo_col.create_index("price")

    await region_channels_col.create_index("chat_id")

    await settings_col.update_one(
        {"_id": "catalog_chat"},
        {"$setOnInsert": {"chat_id": None, "updated_at": now_utc()}},
        upsert=True,
    )
    await settings_col.update_one(
        {"_id": "mandatory_channels"},
        {"$setOnInsert": {"channels": [], "updated_at": now_utc()}},
        upsert=True,
    )

    for region in REGIONS:
        await region_channels_col.update_one(
            {"_id": region},
            {"$setOnInsert": {"region": region, "chat_id": None, "updated_at": now_utc()}},
            upsert=True,
        )


async def close_database() -> None:
    global mongo_client
    if mongo_client is not None:
        mongo_client.close()


async def is_admin_user(user_id: int) -> bool:
    if CONFIG and user_id in CONFIG.admin_ids:
        return True
    if admins_col is None:
        return False
    admin_doc = await admins_col.find_one({"_id": user_id})
    return bool(admin_doc)


async def ensure_user(from_user: Any) -> dict[str, Any]:
    payload = {
        "username": from_user.username,
        "tg_first_name": from_user.first_name,
        "tg_last_name": from_user.last_name,
        "updated_at": now_utc(),
    }
    doc = await users_col.find_one_and_update(
        {"_id": from_user.id},
        {
            "$set": payload,
            "$setOnInsert": {
                "first_name": None,
                "last_name": None,
                "phone": None,
                "role": None,
                "profile_completed": False,
                "driver_profile": {},
                "pro_until": None,
                "lang": None,
                "created_at": now_utc(),
            },
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc


async def fetch_user(user_id: int) -> Optional[dict[str, Any]]:
    return await users_col.find_one({"_id": user_id})


async def open_main_menu(message: Message, text: str) -> None:
    if not message.from_user:
        return
    admin = await is_admin_user(message.from_user.id)
    await message.answer(text, reply_markup=main_menu_keyboard(admin))


def build_profile_text(user: dict[str, Any]) -> str:
    lines = [
        "👤 <b>Profil ma'lumotlari</b>",
        f"🆔 ID: <code>{user['_id']}</code>",
        f"🙍 Ism: <b>{safe(user.get('first_name'))}</b>",
        f"🙍 Familiya: <b>{safe(user.get('last_name'))}</b>",
        f"📱 Telefon: <b>{safe(user.get('phone'))}</b>",
        f"🎯 Rol: <b>{safe(role_label(user.get('role')))}</b>",
        f"💎 Status: <b>{'PRO' if is_pro_active(user) else 'Oddiy'}</b>",
    ]

    if user.get("role") == ROLE_DRIVER:
        driver = user.get("driver_profile") or {}
        lines.extend(
            [
                "",
                "🚛 <b>Mashina ma'lumoti</b>",
                f"• Turi: {safe(driver.get('car_type'))}",
                f"• Sig'imi: {safe(driver.get('capacity_ton'))} tonna",
                f"• Hajmi: {safe(driver.get('volume_m3'))} m3",
                f"• Yo'nalish: {safe(driver.get('routes'))}",
                f"• Narx/km: {safe(driver.get('price_per_km'))}",
                f"• Izoh: {safe(driver.get('note'))}",
            ]
        )

    return "\n".join(lines)


def profile_completeness(user: dict[str, Any]) -> tuple[int, list[str]]:
    checks: list[tuple[str, bool]] = [
        ("Ism", bool(user.get("first_name"))),
        ("Familiya", bool(user.get("last_name"))),
        ("Telefon", bool(user.get("phone"))),
        ("Rol", bool(user.get("role"))),
    ]

    if user.get("role") == ROLE_DRIVER:
        driver = user.get("driver_profile") or {}
        checks.extend(
            [
                ("Mashina turi", bool(driver.get("car_type"))),
                ("Yuk sig'imi", bool(driver.get("capacity_ton"))),
                ("Hajmi", bool(driver.get("volume_m3"))),
                ("Yo'nalish", bool(driver.get("routes"))),
            ]
        )

    done = sum(1 for _, ok in checks if ok)
    total = len(checks)
    score = int((done / total) * 100) if total else 0
    missing = [name for name, ok in checks if not ok]
    return score, missing


def mask_phone(phone: Any) -> str:
    if not phone:
        return "-"
    s = str(phone)
    if len(s) <= 5:
        return s
    return f"{s[:5]}{'*' * max(3, len(s) - 8)}{s[-3:]}"


def build_cargo_preview(data: dict[str, Any]) -> str:
    return (
        "📦 <b>Yuk e'loni preview</b>\n"
        f"📍 Qayerdan: <b>{safe(data.get('from_region'))}</b>\n"
        f"🏁 Qayerga: <b>{safe(data.get('to_region'))}</b>\n"
        f"📦 Yuk turi: <b>{safe(data.get('cargo_type'))}</b>\n"
        f"⚖️ Og'irligi: <b>{safe(data.get('weight_ton'))} tonna</b>\n"
        f"📐 Hajmi: <b>{safe(data.get('volume_m3'))} m3</b>\n"
        f"💰 Narx: <b>{format_money(data.get('price'))} so'm</b>\n"
        f"📅 Yuklash sanasi: <b>{safe(data.get('load_date'))}</b>\n"
        f"💳 To'lov turi: <b>{safe(data.get('payment_type'))}</b>\n"
        f"📝 Izoh: <b>{safe(data.get('comment'))}</b>\n"
    )


def build_cargo_post_text(cargo: dict[str, Any], owner: dict[str, Any], cargo_id: str) -> str:
    pro_badge = "💎 <b>PRO E'LON</b>\n" if is_pro_active(owner) else ""
    owner_name = f"{owner.get('first_name') or ''} {owner.get('last_name') or ''}".strip() or "Noma'lum"
    created_at = normalize_datetime(cargo.get("created_at")) or now_utc()
    timestamp = created_at.strftime("%d.%m.%Y %H:%M")
    route_tag = re.sub(r"[^a-zA-Z0-9_]", "", f"{cargo['from_region']}_{cargo['to_region']}".replace(" ", "_"))

    return (
        "📦 <b>YANGI YUK E'LONI</b>\n"
        f"{pro_badge}"
        f"🆔 <code>{safe(cargo_id)}</code>\n"
        f"📍 <b>Qayerdan:</b> {safe(cargo.get('from_region'))}\n"
        f"🏁 <b>Qayerga:</b> {safe(cargo.get('to_region'))}\n"
        f"📦 <b>Yuk turi:</b> {safe(cargo.get('cargo_type'))}\n"
        f"⚖️ <b>Og'irlik:</b> {safe(cargo.get('weight_ton'))} tonna\n"
        f"📐 <b>Hajm:</b> {safe(cargo.get('volume_m3'))} m3\n"
        f"💰 <b>Narx:</b> {format_money(cargo.get('price'))} so'm\n"
        f"📅 <b>Yuklash:</b> {safe(cargo.get('load_date'))}\n"
        f"💳 <b>To'lov:</b> {safe(cargo.get('payment_type'))}\n"
        f"📝 <b>Izoh:</b> {safe(cargo.get('comment'))}\n"
        f"👤 <b>Yuk beruvchi:</b> {safe(owner_name)}\n"
        f"📞 <b>Aloqa:</b> {safe(mask_phone(owner.get('phone')))} (nomer tugmada)\n"
        f"🕒 <i>{timestamp}</i>\n\n"
        f"#{route_tag} #logistika #yuk"
    )


async def get_catalog_chat_id() -> Optional[int]:
    doc = await settings_col.find_one({"_id": "catalog_chat"})
    if not doc:
        return None
    chat_id = doc.get("chat_id")
    return chat_id if isinstance(chat_id, int) else None


async def get_region_chat_id(region: str) -> Optional[int]:
    doc = await region_channels_col.find_one({"_id": region})
    if not doc:
        return None
    chat_id = doc.get("chat_id")
    return chat_id if isinstance(chat_id, int) else None


async def resolve_target_chats(from_region: str, to_region: str) -> list[int]:
    # Route posting rule:
    # Post only to the origin region chat.
    # Example: Andijon -> Toshkent  => only Andijon chat.
    from_chat = await get_region_chat_id(from_region)
    if from_chat is None:
        return []
    return [from_chat]


async def publish_cargo(
    bot: Bot,
    cargo: dict[str, Any],
    owner: dict[str, Any],
    cargo_id: str,
) -> tuple[list[int], list[str]]:
    text = build_cargo_post_text(cargo, owner, cargo_id)
    bot_username = await get_bot_username(bot)
    inline_markup = build_cargo_inline_keyboard(bot_username, cargo_id)
    target_chats = await resolve_target_chats(cargo["from_region"], cargo["to_region"])

    sent: list[int] = []
    failed: list[str] = []

    for chat_id in target_chats:
        try:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=inline_markup, disable_web_page_preview=True)
            sent.append(chat_id)
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{chat_id}: {normalize_send_error(exc)}")

    return sent, failed


async def get_market_price_rows(limit: int = 5, days: int = 30) -> list[dict[str, Any]]:
    since = now_utc() - timedelta(days=days)
    pipeline = [
        {"$match": {"created_at": {"$gte": since}, "price": {"$type": "number"}}},
        {
            "$group": {
                "_id": {"from": "$from_region", "to": "$to_region"},
                "count": {"$sum": 1},
                "avg_price": {"$avg": "$price"},
                "min_price": {"$min": "$price"},
                "max_price": {"$max": "$price"},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]
    return await cargo_col.aggregate(pipeline).to_list(length=limit)


async def admin_channels_overview_text() -> str:
    catalog = await get_catalog_chat_id()
    region_docs = await region_channels_col.find({}).sort("_id", 1).to_list(length=50)
    mandatory_channels = await get_mandatory_channels()

    connected = 0
    missing: list[str] = []
    lines = [
        "🌐 <b>Kanal/Guruh ulanishi</b>",
        f"📚 Katalog chat: <code>{catalog}</code>" if catalog is not None else "📚 Katalog chat: <b>Ulanmagan</b>",
        "",
    ]

    for doc in region_docs:
        region = doc["_id"]
        chat_id = doc.get("chat_id")
        if isinstance(chat_id, int):
            connected += 1
            lines.append(f"✅ {safe(region)}: <code>{chat_id}</code>")
        else:
            missing.append(region)
            lines.append(f"❌ {safe(region)}: ulanmagan")

    lines.extend(
        [
            "",
            f"🔢 Ulangan viloyatlar: <b>{connected}/{len(REGIONS)}</b>",
        ]
    )

    if missing:
        lines.append("⚠️ Ulanmagan: " + ", ".join(safe(x) for x in missing))

    lines.append("")
    lines.append(f"📌 Majburiy kanallar: <b>{len(mandatory_channels)}</b>")

    return "\n".join(lines)


def build_admin_guide_text() -> str:
    lines = [
        "📘 <b>Admin yo'riqnoma (to'liq)</b>",
        "",
        "1) Adminni sozlash",
        "• `.env` ichida `ADMIN_IDS=...` yozing.",
        "• ID ni `@userinfobot` orqali oling.",
        "• Botni qayta ishga tushiring.",
        "",
        "2) Admin panelga kirish",
        "• Admin foydalanuvchida asosiy menyuda `🛠 Admin panel` tugmasi chiqadi.",
        "• Oddiy foydalanuvchida bu tugma chiqmaydi.",
        "",
        "3) Katalog kanal/guruh ulash",
        "• `🛠 Admin panel` -> `🌐 Kanal/Guruh sozlash` -> `📚 Katalog chat ID`.",
        "• Keyin quyidagidan birini yuboring:",
        "  - Chat ID (`-100...`) raqami",
        "  - `@username` yoki `https://t.me/username` link",
        "  - `https://t.me/username/123` yoki `https://t.me/c/...` message link",
        "  - Yoki shu kanal/guruhdan forward qilingan istalgan post/xabar",
        "",
        "4) 12 viloyat chatlarini ulash",
        "• `🛠 Admin panel` -> `🌐 Kanal/Guruh sozlash` -> `🗺 Viloyat chat ID`.",
        "• Viloyatni tanlang.",
        "• Chat ID (`-100...`) yoki `@username`/`https://t.me/...` link yuboring.",
        "• Yoki o'sha viloyat chatidan forward qilingan xabar yuboring.",
        "• Har bir viloyat uchun takrorlang (12/12).",
        "",
        "5) Ulangan chatlarni tekshirish",
        "• `📋 Ulangan chatlar` tugmasi bilan katalog va barcha viloyatlar holatini ko'rasiz.",
        "",
        "6) Majburiy obuna kanallari",
        "• `➕ Majburiy kanal qo'shish` orqali kanal qo'shing.",
        "• `➖ Majburiy kanal o'chirish` orqali olib tashlang.",
        "• `📌 Majburiy kanallar` bilan ro'yxatni ko'ring.",
        "• Obuna bo'lmagan user botdan foydalana olmaydi.",
        "",
        "7) E'lon qayerga tushadi",
        "• Yangi yuk e'loni: faqat jo'nash viloyati chatiga yuboriladi.",
        "  Masalan: Andijon -> Toshkent bo'lsa, faqat Andijon chatiga tushadi.",
        "• Post ichida inline tugmalar bo'ladi: `☎️ Nomer ko'rish` va `✉️ Xabarga o'tish`.",
        "",
        "8) Muhim texnik shartlar",
        "• Bot ulanishi kerak bo'lgan kanal/guruhga admin qilib qo'shilgan bo'lishi shart.",
        "• Botda xabar yuborish huquqi bo'lishi kerak (`Post/Send messages`).",
        "• `+` yoki `joinchat` invite-linkdan chat ID olib bo'lmaydi.",
        "• Bunday holatda: botni qo'shib forward yuboring yoki `@username`/`-100...` kiriting.",
        "",
        "9) Tezkor komandalar",
        "• `/set_catalog -1001234567890`",
        "• `/set_catalog https://t.me/kanal_username`",
        "• `/set_region Toshkent -1001234567890`",
        "• `/set_region Toshkent https://t.me/toshkent_group`",
        "• `/set_region Qashqadaryo -1001234567890`",
        "• bot ichida chat ID ko'rish: `/chat_id`",
        "",
        "10) Test qilish tartibi",
        "• Oddiy akkauntdan `/start` qilib `📦 Yuk beruvchi` tanlang.",
        "• `📦 Yuk joylash` orqali e'lon yuboring.",
        "• Jo'nash viloyati chatida post chiqqanini tekshiring.",
        "• Postdagi `☎️ Nomer ko'rish` tugmasini bosib, botda aloqa chiqishini tekshiring.",
    ]
    return "\n".join(lines)


async def build_admin_stats_text() -> str:
    now = now_utc()
    day_ago = now - timedelta(days=1)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    total_users, drivers, shippers, pro_users, total_cargo, cargo_day, cargo_week, cargo_month = await asyncio.gather(
        users_col.count_documents({}),
        users_col.count_documents({"role": ROLE_DRIVER}),
        users_col.count_documents({"role": ROLE_SHIPPER}),
        users_col.count_documents({"pro_until": {"$gt": now}}),
        cargo_col.count_documents({}),
        cargo_col.count_documents({"created_at": {"$gte": day_ago}}),
        cargo_col.count_documents({"created_at": {"$gte": week_ago}}),
        cargo_col.count_documents({"created_at": {"$gte": month_ago}}),
    )

    market_rows = await get_market_price_rows(limit=5, days=30)
    overview = await admin_channels_overview_text()

    lines = [
        "📊 <b>Admin statistika</b>",
        "",
        f"👥 Foydalanuvchilar: <b>{total_users}</b>",
        f"🚛 Haydovchilar: <b>{drivers}</b>",
        f"📦 Yuk beruvchilar: <b>{shippers}</b>",
        f"💎 Pro: <b>{pro_users}</b>",
        "",
        f"📦 Jami yuk e'lonlari: <b>{total_cargo}</b>",
        f"🕒 24 soat: <b>{cargo_day}</b>",
        f"📅 7 kun: <b>{cargo_week}</b>",
        f"🗓 30 kun: <b>{cargo_month}</b>",
        "",
        "💰 <b>Narx-navo (30 kun, top yo'nalishlar)</b>",
    ]

    if not market_rows:
        lines.append("Hozircha narx statistikasi uchun ma'lumot yetarli emas.")
    else:
        for idx, row in enumerate(market_rows, start=1):
            route_from = row["_id"].get("from", "-")
            route_to = row["_id"].get("to", "-")
            lines.append(
                f"{idx}. {safe(route_from)} -> {safe(route_to)} | "
                f"{row['count']} ta | min {format_money(row['min_price'])} | "
                f"avg {format_money(row['avg_price'])} | max {format_money(row['max_price'])}"
            )

    lines.extend(["", overview])
    return "\n".join(lines)


async def start_driver_form(message: Message, state: FSMContext, mode: str = "edit") -> None:
    await state.clear()
    await state.set_state(DriverFSM.car_type)
    await state.update_data(driver_mode=mode)
    await message.answer(
        "🚛 <b>Haydovchi anketasi</b>\nMashina turini kiriting (masalan: Fura, Isuzu, Tent, Ref).",
        reply_markup=cancel_keyboard(),
    )


async def start_cargo_form(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(CargoFSM.from_region)
    await message.answer("📍 Yuk qayerdan yuklanadi? Viloyatni tanlang:", reply_markup=region_keyboard())


async def require_completed_profile(message: Message) -> Optional[dict[str, Any]]:
    if not message.from_user:
        return None
    if not await ensure_mandatory_subscription_message(message):
        return None
    user = await ensure_user(message.from_user)
    if not user.get("profile_completed"):
        await message.answer("Profilingiz tugallanmagan. Avval /start orqali to'ldiring.")
        return None
    return user


async def require_admin(message: Message) -> bool:
    if not message.from_user:
        return False
    if await is_admin_user(message.from_user.id):
        return True
    await message.answer("⛔ Sizda admin huquqi yo'q.")
    return False


async def apply_pro(user_id: int, days: int) -> Optional[datetime]:
    user = await fetch_user(user_id)
    if not user:
        return None
    now = now_utc()
    current = normalize_datetime(user.get("pro_until"))
    base = current if current and current > now else now
    new_until = base + timedelta(days=days)
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"pro_until": new_until, "updated_at": now}},
    )
    return new_until


async def remove_pro(user_id: int) -> bool:
    result = await users_col.update_one(
        {"_id": user_id},
        {"$set": {"pro_until": None, "updated_at": now_utc()}},
    )
    return result.matched_count > 0


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    if not await ensure_mandatory_subscription_message(message):
        return

    await state.clear()
    user = await ensure_user(message.from_user)
    payload = extract_start_payload(message.text)

    if payload:
        await handle_start_payload(message, payload)
        if user.get("profile_completed"):
            await open_main_menu(message, "Asosiy menyu.")
        else:
            await message.answer("Botdan to'liq foydalanish uchun /start ni oddiy yuborib ro'yxatdan o'ting.")
        return

    if not is_valid_lang(user.get("lang")):
        await state.set_state(LanguageFSM.select)
        await message.answer(
            "Tilni tanlang / Выберите язык:",
            reply_markup=language_keyboard(),
        )
        return

    if user.get("profile_completed"):
        await open_main_menu(message, "👋 Xush kelibsiz! Asosiy menyudan bo'limni tanlang.")
        return

    await state.set_state(RegistrationFSM.first_name)
    await message.answer(
        "👋 Assalomu alaykum!\n"
        "Logistik platformaga xush kelibsiz.\n\n"
        "🧾 Ismingizni kiriting:",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(LanguageFSM.select)
async def select_language(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    chosen = LANG_LABEL_TO_CODE.get((message.text or "").strip())
    if not chosen:
        await message.answer("Tilni tugma orqali tanlang / Выберите язык кнопкой.", reply_markup=language_keyboard())
        return

    await set_user_lang(message.from_user.id, chosen)
    user = await ensure_user(message.from_user)

    if user.get("profile_completed"):
        await state.clear()
        await open_main_menu(message, "✅ Til saqlandi. Asosiy menyu.")
        return

    await state.set_state(RegistrationFSM.first_name)
    await message.answer(
        "👋 Assalomu alaykum!\n"
        "Logistik platformaga xush kelibsiz.\n\n"
        "🧾 Ismingizni kiriting:",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(Command("cancel"))
@dp.message(F.text == BTN_CANCEL)
async def cancel_flow(message: Message, state: FSMContext) -> None:
    current = await state.get_state()
    if current is None:
        if message.from_user:
            await open_main_menu(message, "Asosiy menyu.")
        return
    await state.clear()
    await open_main_menu(message, "❌ Jarayon bekor qilindi.")


async def route_menu_button(message: Message, state: FSMContext, text: str) -> None:
    if text == BTN_MENU_PROFILE:
        await show_profile(message)
        return
    if text == BTN_MENU_ANALYSIS:
        await show_profile_analysis(message)
        return
    if text == BTN_MENU_STATS:
        await show_stats(message)
        return
    if text == BTN_MENU_CARGO:
        await menu_cargo(message, state)
        return
    if text == BTN_MENU_DRIVER:
        await menu_driver(message, state)
        return
    if text == BTN_MENU_PRO:
        await menu_pro(message)
        return
    if text == BTN_MENU_NEWS:
        await menu_news(message)
        return
    if text == BTN_MENU_CONTACT:
        await menu_contact(message)
        return
    if text == BTN_MENU_SETTINGS:
        await menu_settings(message)
        return
    if text == BTN_SETTINGS_ROLE:
        await settings_role_start(message, state)
        return
    if text == BTN_SETTINGS_LANG:
        await settings_lang_start(message, state)
        return
    if text == BTN_BACK_MAIN:
        await back_to_main(message, state)
        return
    if text == BTN_ADMIN_PANEL:
        await admin_panel(message)
        return
    if text == BTN_ADMIN_STATS:
        await admin_stats(message)
        return
    if text == BTN_ADMIN_USERS:
        await admin_users(message)
        return
    if text == BTN_BROADCAST:
        await admin_broadcast_start(message, state)
        return
    if text == BTN_ADMIN_PRO:
        await admin_pro_menu(message)
        return
    if text == BTN_PRO_ADD:
        await admin_pro_add_start(message, state)
        return
    if text == BTN_PRO_REMOVE:
        await admin_pro_remove_start(message, state)
        return
    if text == BTN_ADMIN_CHANNELS:
        await admin_channels_menu(message)
        return
    if text == BTN_CH_SET_CATALOG:
        await admin_catalog_start(message, state)
        return
    if text == BTN_CH_SET_REGION:
        await admin_region_start(message, state)
        return
    if text == BTN_CH_LIST:
        await admin_channels_list(message)
        return
    if text == BTN_REQ_ADD:
        await admin_required_add_start(message, state)
        return
    if text == BTN_REQ_REMOVE:
        await admin_required_remove_start(message, state)
        return
    if text == BTN_REQ_LIST:
        await admin_required_list(message)
        return
    if text == BTN_ADMIN_GUIDE:
        await admin_help(message)
        return
    if text == BTN_BACK_ADMIN:
        await back_to_admin(message, state)
        return

    if message.from_user:
        await open_main_menu(message, "Kerakli bo'limni menyudan tanlang.")


@dp.message(F.text.func(lambda text: canonicalize_user_text(text) in MENU_INTERRUPT_BUTTONS))
async def menu_interrupt_handler(message: Message, state: FSMContext) -> None:
    text = canonicalize_user_text(message.text)
    if text == BTN_CANCEL:
        await cancel_flow(message, state)
        return
    await state.clear()
    await route_menu_button(message, state, text)

@dp.message(RegistrationFSM.first_name)
async def reg_first_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Ism kamida 2 ta harf bo'lsin. Qayta kiriting:")
        return
    await state.update_data(first_name=text)
    await state.set_state(RegistrationFSM.last_name)
    await message.answer("🧾 Familiyangizni kiriting:")


@dp.message(RegistrationFSM.last_name)
async def reg_last_name(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Familiya kamida 2 ta harf bo'lsin. Qayta kiriting:")
        return
    await state.update_data(last_name=text)
    await state.set_state(RegistrationFSM.phone)
    await message.answer("📱 Telefon raqamingizni yuboring:", reply_markup=phone_keyboard())


@dp.message(RegistrationFSM.phone)
async def reg_phone(message: Message, state: FSMContext) -> None:
    phone: Optional[str] = None
    if message.contact and message.contact.phone_number:
        phone = parse_phone(message.contact.phone_number)
    elif message.text:
        phone = parse_phone(message.text)

    if not phone:
        await message.answer("Telefon formati noto'g'ri. Masalan: +998901234567")
        return

    await state.update_data(phone=phone)
    await state.set_state(RegistrationFSM.role)
    await message.answer("Sizning rolingizni tanlang:", reply_markup=role_keyboard())


@dp.message(RegistrationFSM.role)
async def reg_role(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    role = LABEL_TO_ROLE.get((message.text or "").strip())
    if role not in (ROLE_DRIVER, ROLE_SHIPPER):
        await message.answer("Pastdagi tugmalardan birini tanlang.")
        return

    data = await state.get_data()
    update_data = {
        "first_name": data.get("first_name"),
        "last_name": data.get("last_name"),
        "phone": data.get("phone"),
        "role": role,
        "profile_completed": role == ROLE_SHIPPER,
        "updated_at": now_utc(),
    }
    await users_col.update_one({"_id": message.from_user.id}, {"$set": update_data})

    if role == ROLE_SHIPPER:
        await state.clear()
        await open_main_menu(message, "✅ Ro'yxatdan o'tish tugadi. Endi yuk joylashingiz mumkin.")
        return

    await start_driver_form(message, state, mode="registration")


@dp.message(DriverFSM.car_type)
async def driver_car_type(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Mashina turi juda qisqa. Qayta kiriting:")
        return
    await state.update_data(car_type=text)
    await state.set_state(DriverFSM.capacity_ton)
    await message.answer("⚖️ Yuk sig'imini kiriting (tonna):")


@dp.message(DriverFSM.capacity_ton)
async def driver_capacity(message: Message, state: FSMContext) -> None:
    value = parse_positive_number(message.text or "")
    if value is None:
        await message.answer("Raqam kiriting. Masalan: 20")
        return
    await state.update_data(capacity_ton=value)
    await state.set_state(DriverFSM.volume_m3)
    await message.answer("📐 Hajmini kiriting (m3):")


@dp.message(DriverFSM.volume_m3)
async def driver_volume(message: Message, state: FSMContext) -> None:
    value = parse_positive_number(message.text or "")
    if value is None:
        await message.answer("Raqam kiriting. Masalan: 86")
        return
    await state.update_data(volume_m3=value)
    await state.set_state(DriverFSM.routes)
    await message.answer("📍 Qaysi yo'nalishlarda ishlaysiz? (masalan: Toshkent-Samarqand-Farg'ona)")


@dp.message(DriverFSM.routes)
async def driver_routes(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Yo'nalishni to'liqroq yozing.")
        return
    await state.update_data(routes=text)
    await state.set_state(DriverFSM.price_per_km)
    await message.answer("💵 1 km uchun narx (ixtiyoriy):", reply_markup=skip_cancel_keyboard())


@dp.message(DriverFSM.price_per_km)
async def driver_price(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text == BTN_SKIP:
        await state.update_data(price_per_km=None)
    else:
        value = parse_positive_number(text)
        if value is None:
            await message.answer("Raqam kiriting yoki `⏭ O'tkazib yuborish` ni bosing.")
            return
        await state.update_data(price_per_km=value)

    await state.set_state(DriverFSM.note)
    await message.answer("📝 Qo'shimcha izoh (ixtiyoriy):", reply_markup=skip_cancel_keyboard())


@dp.message(DriverFSM.note)
async def driver_note(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    text = (message.text or "").strip()
    note = None if text == BTN_SKIP else text
    data = await state.get_data()
    await state.clear()

    driver_profile = {
        "car_type": data.get("car_type"),
        "capacity_ton": data.get("capacity_ton"),
        "volume_m3": data.get("volume_m3"),
        "routes": data.get("routes"),
        "price_per_km": data.get("price_per_km"),
        "note": note,
    }

    await users_col.update_one(
        {"_id": message.from_user.id},
        {
            "$set": {
                "role": ROLE_DRIVER,
                "driver_profile": driver_profile,
                "profile_completed": True,
                "updated_at": now_utc(),
            }
        },
    )
    await open_main_menu(message, "✅ Haydovchi anketasi saqlandi.")


@dp.message(CargoFSM.from_region)
async def cargo_from_region(message: Message, state: FSMContext) -> None:
    region = normalize_region(message.text or "")
    if not region:
        await message.answer("Viloyatni tugmadan tanlang.", reply_markup=region_keyboard())
        return
    await state.update_data(from_region=region)
    await state.set_state(CargoFSM.to_region)
    await message.answer("🏁 Yuk qayerga boradi? Viloyatni tanlang:", reply_markup=region_keyboard())


@dp.message(CargoFSM.to_region)
async def cargo_to_region(message: Message, state: FSMContext) -> None:
    region = normalize_region(message.text or "")
    if not region:
        await message.answer("Viloyatni tugmadan tanlang.", reply_markup=region_keyboard())
        return
    await state.update_data(to_region=region)
    await state.set_state(CargoFSM.cargo_type)
    await message.answer("📦 Yuk turini kiriting (masalan: sement, mebel, oziq-ovqat):", reply_markup=cancel_keyboard())


@dp.message(CargoFSM.cargo_type)
async def cargo_type(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Yuk turini to'liqroq kiriting.")
        return
    await state.update_data(cargo_type=text)
    await state.set_state(CargoFSM.weight_ton)
    await message.answer("⚖️ Og'irligini kiriting (tonna):")


@dp.message(CargoFSM.weight_ton)
async def cargo_weight(message: Message, state: FSMContext) -> None:
    value = parse_positive_number(message.text or "")
    if value is None:
        await message.answer("Raqam kiriting. Masalan: 22")
        return
    await state.update_data(weight_ton=value)
    await state.set_state(CargoFSM.volume_m3)
    await message.answer("📐 Hajmini kiriting (m3):")


@dp.message(CargoFSM.volume_m3)
async def cargo_volume(message: Message, state: FSMContext) -> None:
    value = parse_positive_number(message.text or "")
    if value is None:
        await message.answer("Raqam kiriting. Masalan: 86")
        return
    await state.update_data(volume_m3=value)
    await state.set_state(CargoFSM.price)
    await message.answer("💰 Taklif narxini kiriting (so'm):")


@dp.message(CargoFSM.price)
async def cargo_price(message: Message, state: FSMContext) -> None:
    value = parse_positive_number(message.text or "")
    if value is None:
        await message.answer("Narxni raqamda kiriting. Masalan: 2500000")
        return
    await state.update_data(price=value)
    await state.set_state(CargoFSM.load_date)
    await message.answer("📅 Yuklash sanasi (masalan: 25.02.2026 yoki bugun):")


@dp.message(CargoFSM.load_date)
async def cargo_load_date(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Yuklash sanasini kiriting.")
        return
    await state.update_data(load_date=text)
    await state.set_state(CargoFSM.payment_type)
    await message.answer("💳 To'lov turini tanlang:", reply_markup=payment_keyboard())


@dp.message(CargoFSM.payment_type)
async def cargo_payment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    if text not in PAYMENT_OPTIONS:
        await message.answer("To'lov turini tugmadan tanlang.", reply_markup=payment_keyboard())
        return
    await state.update_data(payment_type=text)
    await state.set_state(CargoFSM.comment)
    await message.answer("📝 Qo'shimcha izoh (ixtiyoriy):", reply_markup=skip_cancel_keyboard())


@dp.message(CargoFSM.comment)
async def cargo_comment(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    comment = "-" if text == BTN_SKIP else text
    await state.update_data(comment=comment)

    data = await state.get_data()
    await state.set_state(CargoFSM.confirm)
    await message.answer(build_cargo_preview(data), reply_markup=cargo_confirm_keyboard())


@dp.message(CargoFSM.confirm)
async def cargo_confirm(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return

    text = (message.text or "").strip()
    if text == BTN_CARGO_EDIT:
        await state.set_state(CargoFSM.from_region)
        await message.answer("Tahrirlash boshlandi. Qayerdan yuklanadi?", reply_markup=region_keyboard())
        return

    if text != BTN_CARGO_CONFIRM:
        await message.answer("Pastdagi tugmalardan birini tanlang.", reply_markup=cargo_confirm_keyboard())
        return

    data = await state.get_data()
    await state.clear()

    owner = await fetch_user(message.from_user.id)
    if not owner:
        await message.answer("Xatolik: foydalanuvchi topilmadi.")
        return

    cargo_doc = {
        "owner_id": message.from_user.id,
        "from_region": data["from_region"],
        "to_region": data["to_region"],
        "cargo_type": data["cargo_type"],
        "weight_ton": data["weight_ton"],
        "volume_m3": data["volume_m3"],
        "price": data["price"],
        "load_date": data["load_date"],
        "payment_type": data["payment_type"],
        "comment": data["comment"],
        "created_at": now_utc(),
        "status": "active",
    }

    result = await cargo_col.insert_one(cargo_doc)
    cargo_id = str(result.inserted_id)

    sent, failed = await publish_cargo(message.bot, cargo_doc, owner, cargo_id)
    await cargo_col.update_one(
        {"_id": result.inserted_id},
        {
            "$set": {
                "posted_chats": sent,
                "post_failures": failed,
                "updated_at": now_utc(),
            }
        },
    )

    lines = [
        "✅ Yuk e'loningiz saqlandi va yuborildi.",
        f"🆔 E'lon ID: <code>{cargo_id}</code>",
        f"📤 Yuborilgan chatlar: <b>{len(sent)}</b>",
    ]

    if not sent:
        lines.append("⚠️ Hech bir chat ulanmagan. Admin paneldan katalog/viloyat chat ID larni kiriting.")
    if failed:
        lines.append(f"❗ Yuborishda xatolar: <b>{len(failed)}</b>")
        preview_errors = failed[:3]
        lines.append("Sabab:")
        for err in preview_errors:
            lines.append(f"• <code>{safe(err)}</code>")

    await open_main_menu(message, "\n".join(lines))


@dp.message(SettingsFSM.role)
async def settings_change_role(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    role = LABEL_TO_ROLE.get((message.text or "").strip())
    if role not in (ROLE_DRIVER, ROLE_SHIPPER):
        await message.answer("Rolni tugmadan tanlang.")
        return

    user = await fetch_user(message.from_user.id)
    if not user:
        await message.answer("Foydalanuvchi topilmadi.")
        return

    if role == ROLE_SHIPPER:
        await users_col.update_one(
            {"_id": message.from_user.id},
            {"$set": {"role": ROLE_SHIPPER, "profile_completed": True, "updated_at": now_utc()}},
        )
        await state.clear()
        await open_main_menu(message, "✅ Rolingiz yuk beruvchi qilib yangilandi.")
        return

    driver_profile = user.get("driver_profile") or {}
    profile_ready = bool(
        driver_profile.get("car_type")
        and driver_profile.get("capacity_ton")
        and driver_profile.get("volume_m3")
        and driver_profile.get("routes")
    )
    await users_col.update_one(
        {"_id": message.from_user.id},
        {"$set": {"role": ROLE_DRIVER, "profile_completed": profile_ready, "updated_at": now_utc()}},
    )
    if profile_ready:
        await state.clear()
        await open_main_menu(message, "✅ Rolingiz haydovchi qilib yangilandi.")
        return

    await start_driver_form(message, state, mode="settings")


@dp.message(AdminBroadcastFSM.audience)
async def admin_broadcast_audience(message: Message, state: FSMContext) -> None:
    mapping = {
        BTN_BC_ALL: "all",
        BTN_BC_DRIVERS: "drivers",
        BTN_BC_SHIPPERS: "shippers",
        BTN_BC_PRO: "pro",
    }
    audience = mapping.get((message.text or "").strip())
    if not audience:
        await message.answer("Auditoriyani tugmadan tanlang.", reply_markup=broadcast_audience_keyboard())
        return
    await state.update_data(audience=audience)
    await state.set_state(AdminBroadcastFSM.content)
    await message.answer("Yuboriladigan xabarni yuboring (text/photo/video ham bo'lishi mumkin).", reply_markup=cancel_keyboard())


@dp.message(AdminBroadcastFSM.content)
async def admin_broadcast_content(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    if not await require_admin(message):
        await state.clear()
        return

    data = await state.get_data()
    audience = data.get("audience", "all")
    now = now_utc()

    query: dict[str, Any] = {}
    if audience == "drivers":
        query = {"role": ROLE_DRIVER}
    elif audience == "shippers":
        query = {"role": ROLE_SHIPPER}
    elif audience == "pro":
        query = {"pro_until": {"$gt": now}}

    sent = 0
    failed = 0
    cursor = users_col.find(query, {"_id": 1})
    async for doc in cursor:
        user_id = doc["_id"]
        try:
            await message.copy_to(chat_id=user_id)
            sent += 1
            await asyncio.sleep(0.03)
        except Exception:  # noqa: BLE001
            failed += 1

    await state.clear()
    await message.answer(
        f"✅ Broadcast yakunlandi.\n📤 Yuborildi: <b>{sent}</b>\n❗ Xato: <b>{failed}</b>",
        reply_markup=admin_panel_keyboard(),
    )


@dp.message(AdminProFSM.add)
async def admin_pro_add_state(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Format: <code>user_id kun</code>\nMasalan: <code>123456789 30</code>")
        return
    if not parts[0].lstrip("-").isdigit() or not parts[1].isdigit():
        await message.answer("Noto'g'ri format. Raqam kiriting.")
        return

    user_id = int(parts[0])
    days = int(parts[1])
    if days <= 0:
        await message.answer("Kun soni 0 dan katta bo'lishi kerak.")
        return

    new_until = await apply_pro(user_id, days)
    if not new_until:
        await message.answer("Foydalanuvchi topilmadi.")
        return

    await state.clear()
    await message.answer(
        f"✅ Pro qo'shildi.\n👤 User: <code>{user_id}</code>\n📅 Tugash sanasi: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>",
        reply_markup=admin_pro_keyboard(),
    )

    try:
        await message.bot.send_message(
            chat_id=user_id,
            text=f"🎉 Sizga PRO status qo'shildi.\n📅 Tugash: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>",
        )
    except Exception:  # noqa: BLE001
        pass


@dp.message(AdminProFSM.remove)
async def admin_pro_remove_state(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if not raw.lstrip("-").isdigit():
        await message.answer("Faqat user_id kiriting. Masalan: <code>123456789</code>")
        return

    user_id = int(raw)
    ok = await remove_pro(user_id)
    if not ok:
        await message.answer("Foydalanuvchi topilmadi.")
        return

    await state.clear()
    await message.answer(f"✅ Pro o'chirildi: <code>{user_id}</code>", reply_markup=admin_pro_keyboard())

    try:
        await message.bot.send_message(chat_id=user_id, text="ℹ️ Sizning PRO statusingiz bekor qilindi.")
    except Exception:  # noqa: BLE001
        pass


@dp.message(AdminChannelFSM.catalog_chat)
async def admin_set_catalog_chat(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return
    chat_id, error = await resolve_chat_id_from_message(message)
    if chat_id is None:
        await message.answer(error or "Chat ID topilmadi.")
        return

    await settings_col.update_one(
        {"_id": "catalog_chat"},
        {"$set": {"chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    await state.clear()
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ Katalog chat saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
        text.append("Botni shu chatga admin/member qilib qo'shing va qayta tekshiring.")
    await message.answer("\n".join(text), reply_markup=admin_channels_keyboard())


@dp.message(AdminChannelFSM.region_select)
async def admin_select_region(message: Message, state: FSMContext) -> None:
    region = normalize_region(message.text or "")
    if not region:
        await message.answer("Viloyatni tugmadan tanlang.", reply_markup=region_keyboard())
        return
    await state.update_data(selected_region=region)
    await state.set_state(AdminChannelFSM.region_chat)
    await message.answer(
        f"{safe(region)} uchun chatni ulang.\n"
        "• `-100...` chat ID yuboring yoki\n"
        "• `@username` / `https://t.me/...` link yuboring yoki\n"
        "• Shu viloyat chatidan forward qilingan xabar yuboring.",
        reply_markup=cancel_keyboard(),
    )


@dp.message(AdminChannelFSM.region_chat)
async def admin_set_region_chat(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return
    chat_id, error = await resolve_chat_id_from_message(message)
    if chat_id is None:
        await message.answer(error or "Chat ID topilmadi.")
        return
    data = await state.get_data()
    region = data.get("selected_region")
    if not region:
        await state.clear()
        await message.answer("Xatolik: viloyat tanlanmadi.", reply_markup=admin_channels_keyboard())
        return

    await region_channels_col.update_one(
        {"_id": region},
        {"$set": {"region": region, "chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    await state.clear()
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ {safe(region)} chati saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
        text.append("Botni shu chatga admin/member qilib qo'shing va qayta tekshiring.")
    await message.answer("\n".join(text), reply_markup=admin_channels_keyboard())


@dp.message(StateFilter(None), Command("profil"))
@dp.message(StateFilter(None), F.text == BTN_MENU_PROFILE)
async def show_profile(message: Message) -> None:
    if not message.from_user:
        return
    if not await ensure_mandatory_subscription_message(message):
        return
    user = await ensure_user(message.from_user)
    if not user.get("profile_completed"):
        await message.answer("Profilingiz tugallanmagan. /start orqali davom eting.")
        return
    await message.answer(build_profile_text(user), reply_markup=main_menu_keyboard(await is_admin_user(message.from_user.id)))


@dp.message(StateFilter(None), Command("tahlil"))
@dp.message(StateFilter(None), F.text == BTN_MENU_ANALYSIS)
async def show_profile_analysis(message: Message) -> None:
    if not message.from_user:
        return
    if not await ensure_mandatory_subscription_message(message):
        return
    user = await ensure_user(message.from_user)
    if not user.get("profile_completed"):
        await message.answer("Profilingiz tugallanmagan. /start ni bosing.")
        return

    score, missing = profile_completeness(user)
    cargo_total = await cargo_col.count_documents({"owner_id": message.from_user.id})
    cargo_30 = await cargo_col.count_documents({"owner_id": message.from_user.id, "created_at": {"$gte": now_utc() - timedelta(days=30)}})

    lines = [
        "🧠 <b>Profil tahlili</b>",
        f"📊 To'liqlik: <b>{score}%</b>",
        f"📦 Jami e'lonlar: <b>{cargo_total}</b>",
        f"🗓 Oxirgi 30 kun: <b>{cargo_30}</b>",
    ]

    if missing:
        lines.append("⚠️ Yetishmayotgan ma'lumotlar: " + ", ".join(safe(x) for x in missing))
    else:
        lines.append("✅ Profil to'liq.")

    if user.get("role") == ROLE_DRIVER and score < 100:
        lines.append("💡 Tavsiya: mashina parametrlarini to'liq kiritsangiz buyurtma topish ehtimoli oshadi.")
    elif user.get("role") == ROLE_SHIPPER and cargo_total == 0:
        lines.append("💡 Tavsiya: birinchi yuk e'lonini joylashtiring.")

    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard(await is_admin_user(message.from_user.id)))


@dp.message(StateFilter(None), Command("statistika"))
@dp.message(StateFilter(None), F.text == BTN_MENU_STATS)
async def show_stats(message: Message) -> None:
    if not message.from_user:
        return
    if not await ensure_mandatory_subscription_message(message):
        return
    user = await ensure_user(message.from_user)
    if not user.get("profile_completed"):
        await message.answer("Profilingiz tugallanmagan. /start ni bosing.")
        return

    uid = message.from_user.id
    now = now_utc()
    my_total, my_30, my_avg_rows = await asyncio.gather(
        cargo_col.count_documents({"owner_id": uid}),
        cargo_col.count_documents({"owner_id": uid, "created_at": {"$gte": now - timedelta(days=30)}}),
        cargo_col.aggregate(
            [
                {"$match": {"owner_id": uid, "price": {"$type": "number"}}},
                {"$group": {"_id": None, "avg_price": {"$avg": "$price"}}},
            ]
        ).to_list(length=1),
    )

    market_rows = await get_market_price_rows(limit=5, days=30)
    my_avg = my_avg_rows[0]["avg_price"] if my_avg_rows else None

    lines = [
        "📊 <b>Statistika</b>",
        "",
        f"📦 Sizning jami e'lonlaringiz: <b>{my_total}</b>",
        f"🗓 Oxirgi 30 kun: <b>{my_30}</b>",
        f"💰 O'rtacha narx: <b>{format_money(my_avg)} so'm</b>",
        "",
        "💹 <b>Narx-navo (bozor, 30 kun)</b>",
    ]

    if market_rows:
        for i, row in enumerate(market_rows, start=1):
            route_from = row["_id"].get("from", "-")
            route_to = row["_id"].get("to", "-")
            lines.append(
                f"{i}. {safe(route_from)} -> {safe(route_to)} | "
                f"{row['count']} ta | min {format_money(row['min_price'])} | "
                f"avg {format_money(row['avg_price'])} | max {format_money(row['max_price'])}"
            )
    else:
        lines.append("Hozircha statistik ma'lumot yetarli emas.")

    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard(await is_admin_user(uid)))


@dp.message(StateFilter(None), Command("yuk"))
@dp.message(StateFilter(None), F.text == BTN_MENU_CARGO)
async def menu_cargo(message: Message, state: FSMContext) -> None:
    user = await require_completed_profile(message)
    if not user:
        return
    if user.get("role") != ROLE_SHIPPER:
        await message.answer("📌 Yuk joylash faqat `Yuk beruvchi` roli uchun. Sozlamadan rolni almashtiring.")
        return
    await start_cargo_form(message, state)


@dp.message(StateFilter(None), F.text == BTN_MENU_DRIVER)
async def menu_driver(message: Message, state: FSMContext) -> None:
    user = await require_completed_profile(message)
    if not user:
        return
    await start_driver_form(message, state, mode="edit")


@dp.message(StateFilter(None), F.text == BTN_MENU_PRO)
async def menu_pro(message: Message) -> None:
    if not await ensure_mandatory_subscription_message(message):
        return
    text = (
        "💎 <b>PRO tarif</b>\n"
        "PRO foydalanuvchi afzalliklari:\n"
        "• E'lonlar ajratib ko'rsatiladi\n"
        "• Yuqoriroq ko'rinish imkoniyati\n"
        "• Tezkor navbat\n\n"
        "Tariflar (misol):\n"
        "• 7 kun\n"
        "• 30 kun\n"
        "• 90 kun\n\n"
        "Ulash uchun admin bilan bog'laning."
    )
    await message.answer(text)


@dp.message(StateFilter(None), F.text == BTN_MENU_NEWS)
async def menu_news(message: Message) -> None:
    if not await ensure_mandatory_subscription_message(message):
        return
    if CONFIG and CONFIG.news_channel:
        await message.answer(f"📣 Yangiliklar kanali:\n{safe(CONFIG.news_channel)}")
    else:
        await message.answer("📣 Yangiliklar bo'limi hali sozlanmagan.")


@dp.message(StateFilter(None), F.text == BTN_MENU_CONTACT)
async def menu_contact(message: Message) -> None:
    if not await ensure_mandatory_subscription_message(message):
        return
    support = CONFIG.support_contact if CONFIG else "@support"
    await message.answer(f"☎️ Bog'lanish: {safe(support)}")


@dp.message(StateFilter(None), F.text == BTN_MENU_SETTINGS)
async def menu_settings(message: Message) -> None:
    if not await ensure_mandatory_subscription_message(message):
        return
    await message.answer("⚙️ Sozlamalar:", reply_markup=settings_keyboard())


@dp.message(StateFilter(None), F.text == BTN_SETTINGS_ROLE)
async def settings_role_start(message: Message, state: FSMContext) -> None:
    user = await require_completed_profile(message)
    if not user:
        return
    await state.set_state(SettingsFSM.role)
    await message.answer("Yangi rolni tanlang:", reply_markup=role_keyboard())


@dp.message(StateFilter(None), F.text == BTN_SETTINGS_LANG)
async def settings_lang_start(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    await state.set_state(LanguageFSM.select)
    await message.answer("Tilni tanlang / Выберите язык:", reply_markup=language_keyboard())


@dp.message(StateFilter(None), Command("lang"))
async def cmd_lang(message: Message, state: FSMContext) -> None:
    if not await ensure_mandatory_subscription_message(message):
        return
    await state.set_state(LanguageFSM.select)
    await message.answer("Tilni tanlang / Выберите язык:", reply_markup=language_keyboard())


@dp.message(StateFilter(None), F.text == BTN_BACK_MAIN)
async def back_to_main(message: Message, state: FSMContext) -> None:
    await state.clear()
    await open_main_menu(message, "Asosiy menyu.")


@dp.message(StateFilter(None), Command("admin"))
@dp.message(StateFilter(None), F.text == BTN_ADMIN_PANEL)
async def admin_panel(message: Message) -> None:
    if not await require_admin(message):
        return
    await message.answer("🛠 <b>Admin panel</b>", reply_markup=admin_panel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_ADMIN_STATS)
async def admin_stats(message: Message) -> None:
    if not await require_admin(message):
        return
    text = await build_admin_stats_text()
    await message.answer(text, reply_markup=admin_panel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_ADMIN_USERS)
async def admin_users(message: Message) -> None:
    if not await require_admin(message):
        return
    users = await users_col.find({}, {"first_name": 1, "last_name": 1, "role": 1, "phone": 1, "pro_until": 1, "updated_at": 1}).sort(
        "updated_at", DESCENDING
    ).to_list(length=20)

    if not users:
        await message.answer("Foydalanuvchilar topilmadi.", reply_markup=admin_panel_keyboard())
        return

    lines = ["📋 <b>Oxirgi foydalanuvchilar (20 ta)</b>"]
    for user in users:
        status = "PRO" if is_pro_active(user) else "Oddiy"
        name = f"{user.get('first_name') or '-'} {user.get('last_name') or '-'}".strip()
        lines.append(
            f"• <code>{user['_id']}</code> | {safe(name)} | {safe(role_label(user.get('role')))} | {status}"
        )
    await message.answer("\n".join(lines), reply_markup=admin_panel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_BROADCAST)
async def admin_broadcast_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminBroadcastFSM.audience)
    await message.answer("Qaysi auditoriyaga yuborilsin?", reply_markup=broadcast_audience_keyboard())


@dp.message(StateFilter(None), F.text == BTN_ADMIN_PRO)
async def admin_pro_menu(message: Message) -> None:
    if not await require_admin(message):
        return
    await message.answer("💎 Pro boshqaruvi", reply_markup=admin_pro_keyboard())


@dp.message(StateFilter(None), F.text == BTN_PRO_ADD)
async def admin_pro_add_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminProFSM.add)
    await message.answer("Format: <code>user_id kun</code>\nMasalan: <code>123456789 30</code>", reply_markup=cancel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_PRO_REMOVE)
async def admin_pro_remove_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminProFSM.remove)
    await message.answer("Format: <code>user_id</code>\nMasalan: <code>123456789</code>", reply_markup=cancel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_ADMIN_CHANNELS)
async def admin_channels_menu(message: Message) -> None:
    if not await require_admin(message):
        return
    await message.answer("🌐 Kanal/Guruh sozlash", reply_markup=admin_channels_keyboard())


@dp.message(StateFilter(None), F.text == BTN_CH_SET_CATALOG)
async def admin_catalog_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminChannelFSM.catalog_chat)
    await message.answer(
        "Katalog chatni ulang.\n"
        "• `-100...` chat ID yuboring yoki\n"
        "• `@username` / `https://t.me/...` link yuboring yoki\n"
        "• Katalog kanal/guruhdan forward qilingan xabar yuboring.",
        reply_markup=cancel_keyboard(),
    )


@dp.message(StateFilter(None), F.text == BTN_CH_SET_REGION)
async def admin_region_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminChannelFSM.region_select)
    await message.answer("Viloyatni tanlang:", reply_markup=region_keyboard())


@dp.message(StateFilter(None), F.text == BTN_CH_LIST)
async def admin_channels_list(message: Message) -> None:
    if not await require_admin(message):
        return
    text = await admin_channels_overview_text()
    await message.answer(text, reply_markup=admin_channels_keyboard())


@dp.message(StateFilter(None), F.text == BTN_REQ_ADD)
async def admin_required_add_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminChannelFSM.required_add)
    await message.answer(
        "Majburiy kanalni qo'shish uchun yuboring:\n"
        "• `-100...` chat ID yoki\n"
        "• `@username` / `https://t.me/...` link yoki\n"
        "• kanaldan forward xabar",
        reply_markup=cancel_keyboard(),
    )


@dp.message(StateFilter(None), F.text == BTN_REQ_REMOVE)
async def admin_required_remove_start(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        return
    await state.set_state(AdminChannelFSM.required_remove)
    await message.answer(
        "Majburiy kanalni o'chirish uchun yuboring:\n"
        "• `-100...` chat ID yoki\n"
        "• `@username` / `https://t.me/...` link yoki\n"
        "• kanaldan forward xabar",
        reply_markup=cancel_keyboard(),
    )


@dp.message(StateFilter(None), F.text == BTN_REQ_LIST)
async def admin_required_list(message: Message) -> None:
    if not await require_admin(message):
        return
    text = await mandatory_channels_overview_text()
    await message.answer(text, reply_markup=admin_channels_keyboard())


@dp.message(AdminChannelFSM.required_add)
async def admin_required_add_state(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return

    chat_id, error = await resolve_chat_id_from_message(message)
    if chat_id is None:
        await message.answer(error or "Chat aniqlanmadi.")
        return

    ok, result_text = await add_mandatory_channel(message.bot, chat_id)
    if not ok:
        await message.answer(f"❗ Qo'shishda xato: {safe(result_text)}")
        return

    await state.clear()
    await message.answer(result_text, reply_markup=admin_channels_keyboard())


@dp.message(AdminChannelFSM.required_remove)
async def admin_required_remove_state(message: Message, state: FSMContext) -> None:
    if not await require_admin(message):
        await state.clear()
        return

    chat_id, error = await resolve_chat_id_from_message(message)
    if chat_id is None:
        await message.answer(error or "Chat aniqlanmadi.")
        return

    deleted = await remove_mandatory_channel(chat_id)
    await state.clear()
    if not deleted:
        await message.answer("Berilgan kanal majburiy ro'yxatda topilmadi.", reply_markup=admin_channels_keyboard())
        return
    await message.answer(f"✅ Majburiy kanal o'chirildi: <code>{chat_id}</code>", reply_markup=admin_channels_keyboard())


@dp.message(StateFilter(None), Command("admin_help"))
@dp.message(StateFilter(None), F.text == BTN_ADMIN_GUIDE)
async def admin_help(message: Message) -> None:
    if not await require_admin(message):
        return
    await message.answer(build_admin_guide_text(), reply_markup=admin_panel_keyboard())


@dp.message(StateFilter(None), F.text == BTN_BACK_ADMIN)
async def back_to_admin(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not await require_admin(message):
        return
    await message.answer("🛠 Admin panel", reply_markup=admin_panel_keyboard())


@dp.message(Command("pro_add"))
async def admin_pro_add_command(message: Message) -> None:
    if not await require_admin(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Format: <code>/pro_add user_id kun</code>")
        return
    if not parts[1].lstrip("-").isdigit() or not parts[2].isdigit():
        await message.answer("Format noto'g'ri.")
        return
    user_id = int(parts[1])
    days = int(parts[2])
    if days <= 0:
        await message.answer("Kun soni 0 dan katta bo'lishi kerak.")
        return

    new_until = await apply_pro(user_id, days)
    if not new_until:
        await message.answer("Foydalanuvchi topilmadi.")
        return
    await message.answer(
        f"✅ Pro qo'shildi.\n👤 User: <code>{user_id}</code>\n📅 Tugash: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>"
    )


@dp.message(Command("pro_remove"))
async def admin_pro_remove_command(message: Message) -> None:
    if not await require_admin(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Format: <code>/pro_remove user_id</code>")
        return
    user_id = int(parts[1])
    ok = await remove_pro(user_id)
    if not ok:
        await message.answer("Foydalanuvchi topilmadi.")
        return
    await message.answer(f"✅ Pro o'chirildi: <code>{user_id}</code>")


@dp.message(Command("set_catalog"))
async def admin_set_catalog_command(message: Message) -> None:
    if not await require_admin(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer(
            "Format:\n"
            "• <code>/set_catalog -1001234567890</code>\n"
            "• <code>/set_catalog @kanal_username</code>\n"
            "• <code>/set_catalog https://t.me/kanal_username</code>"
        )
        return
    chat_id, error = await resolve_chat_id_from_text(message.bot, parts[1])
    if chat_id is None:
        await message.answer(error or "Chat ID noto'g'ri.")
        return
    await settings_col.update_one(
        {"_id": "catalog_chat"},
        {"$set": {"chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ Katalog chat saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
    await message.answer("\n".join(text))


@dp.message(Command("set_region"))
async def admin_set_region_command(message: Message) -> None:
    if not await require_admin(message):
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer(
            "Format:\n"
            "• <code>/set_region Toshkent -1001234567890</code>\n"
            "• <code>/set_region Toshkent @toshkent_group</code>\n"
            "• <code>/set_region Toshkent https://t.me/toshkent_group</code>"
        )
        return
    chat_id, error = await resolve_chat_id_from_text(message.bot, parts[-1])
    if chat_id is None:
        await message.answer(error or "Chat ID noto'g'ri.")
        return
    region_raw = " ".join(parts[1:-1]).replace("_", " ")
    region = normalize_region(region_raw)
    if not region:
        await message.answer("Viloyat noto'g'ri. To'g'ri variantlar: " + ", ".join(REGIONS))
        return
    await region_channels_col.update_one(
        {"_id": region},
        {"$set": {"region": region, "chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ {safe(region)} chati saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
    await message.answer("\n".join(text))


@dp.message(Command("set_catalog_here"))
async def admin_set_catalog_here(message: Message) -> None:
    if not await require_admin(message):
        return
    if message.chat.type == "private":
        await message.answer("Bu buyruqni katalog kanal/guruh ichida yuboring.")
        return

    chat_id = int(message.chat.id)
    await settings_col.update_one(
        {"_id": "catalog_chat"},
        {"$set": {"chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ Shu chat katalog sifatida saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
    await message.answer("\n".join(text))


@dp.message(Command("set_region_here"))
async def admin_set_region_here(message: Message) -> None:
    if not await require_admin(message):
        return
    if message.chat.type == "private":
        await message.answer("Bu buyruqni viloyat kanal/guruh ichida yuboring.\nFormat: <code>/set_region_here Toshkent</code>")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Format: <code>/set_region_here Toshkent</code>")
        return

    region = normalize_region(parts[1].replace("_", " "))
    if not region:
        await message.answer("Viloyat noto'g'ri. To'g'ri variantlar: " + ", ".join(REGIONS))
        return

    chat_id = int(message.chat.id)
    await region_channels_col.update_one(
        {"_id": region},
        {"$set": {"region": region, "chat_id": chat_id, "updated_at": now_utc()}},
        upsert=True,
    )
    ok, status_text = await check_chat_writable(message.bot, chat_id)
    text = [f"✅ {safe(region)} uchun shu chat saqlandi: <code>{chat_id}</code>"]
    if ok:
        text.append(f"✅ Tekshiruv: {safe(status_text)}")
    else:
        text.append(f"⚠️ Tekshiruv: {safe(status_text)}")
    await message.answer("\n".join(text))


@dp.message(Command("chat_id"))
async def cmd_chat_id(message: Message) -> None:
    chat_username = f"@{message.chat.username}" if message.chat.username else "yo'q"
    await message.answer(
        "🆔 <b>Chat ma'lumoti</b>\n"
        f"• Chat ID: <code>{message.chat.id}</code>\n"
        f"• Type: <code>{message.chat.type}</code>\n"
        f"• Username: <code>{safe(chat_username)}</code>"
    )


@dp.callback_query(F.data == "check_sub")
async def callback_check_sub(callback: CallbackQuery) -> None:
    if not callback.from_user:
        await callback.answer()
        return

    if await is_admin_user(callback.from_user.id):
        await callback.answer("Admin uchun obuna tekshiruvi shart emas.")
        return

    missing = await get_missing_mandatory_channels(callback.bot, callback.from_user.id)
    if missing:
        if callback.message:
            try:
                await callback.message.edit_reply_markup(reply_markup=mandatory_subscribe_keyboard(missing))
            except Exception:  # noqa: BLE001
                pass
        await callback.answer("Hali barcha kanallarga obuna bo'lmadingiz.", show_alert=True)
        return

    await callback.answer("✅ Obuna tasdiqlandi.")
    if callback.message:
        user = await fetch_user(callback.from_user.id)
        if not user:
            user = await ensure_user(callback.from_user)
        if user and not is_valid_lang(user.get("lang")):
            ctx = await dp.fsm.get_context(
                bot=callback.bot,
                chat_id=callback.from_user.id,
                user_id=callback.from_user.id,
            )
            await ctx.set_state(LanguageFSM.select)
            await callback.message.answer("Tilni tanlang / Выберите язык:", reply_markup=language_keyboard())
            return
        if user and user.get("profile_completed"):
            await callback.message.answer(
                "✅ Obuna tasdiqlandi. Davom etishingiz mumkin.",
                reply_markup=main_menu_keyboard(await is_admin_user(callback.from_user.id)),
            )
        else:
            await callback.message.answer("✅ Obuna tasdiqlandi. Endi /start ni bosing.")


@dp.message(StateFilter(None))
async def fallback(message: Message) -> None:
    if message.text and message.text.startswith("/"):
        await message.answer("Buyruq topilmadi. Menyudan foydalaning yoki /start ni bosing.")
        return
    if message.from_user:
        await open_main_menu(message, "Kerakli bo'limni menyudan tanlang.")


async def on_startup(bot: Bot) -> None:
    await init_database()
    private_commands = [
        BotCommand(command="start", description="Botni ishga tushirish"),
        BotCommand(command="profil", description="Profilni ko'rish"),
        BotCommand(command="yuk", description="Yuk joylash"),
        BotCommand(command="tahlil", description="Profil tahlili"),
        BotCommand(command="statistika", description="Statistika"),
        BotCommand(command="admin", description="Admin panel"),
        BotCommand(command="admin_help", description="Admin yo'riqnoma"),
        BotCommand(command="lang", description="Til / Язык"),
        BotCommand(command="chat_id", description="Joriy chat ID"),
        BotCommand(command="cancel", description="Joriy amalni bekor qilish"),
    ]
    await bot.set_my_commands(private_commands, scope=BotCommandScopeAllPrivateChats())
    try:
        await bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
    except Exception:  # noqa: BLE001
        pass
    logging.info("Bot ishga tushdi.")


async def on_shutdown(bot: Bot) -> None:
    await close_database()
    logging.info("Bot to'xtatildi.")


async def main() -> None:
    global CONFIG
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    CONFIG = load_config()

    bot = Bot(
        token=CONFIG.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass


