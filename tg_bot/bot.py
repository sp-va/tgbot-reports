import asyncio
import os
import json
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command

from db.crud import GROUP_TYPES

load_dotenv()
token = os.environ.get("TELEGRAM_TOKEN")

bot = Bot(token=token)
dispatcher = Dispatcher()


@dispatcher.message(Command("start"))
async def start_message(message: types.Message):
    username = message.from_user.username
    await message.answer(text=f"Привет, {username}")


@dispatcher.message()
async def input_data(message: types.Message):
    text = message.text
    try:
        to_dict = json.loads(text)
        print(to_dict)
        dt_from = to_dict["dt_from"]
        dt_upto = to_dict["dt_upto"]
        group_type = to_dict["group_type"]
        result = await GROUP_TYPES[group_type](dt_from, dt_upto)
        await message.answer(text=str(result))
    except:
        await message.answer(text="Неверный формат входных данных!")
    finally:
        await message.answer(text=f"Твой текст: {text}")


async def main():
    await dispatcher.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

