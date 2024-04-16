from motor import motor_asyncio

client = motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db = client["bot_db"]
collection = db["samples"]
