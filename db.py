import asyncpg
import asyncio


async def dbInit():
    global connectionPool
    connectionPool = await createConnectionPool()
    await createTables()

async def createConnectionPool():
    return await asyncpg.create_pool()

async def createTables():
    sql = """CREATE TABLE IF NOT EXISTS crawlPeers(peerID TEXT PRIMARY KEY);"""
    async with connectionPool.acquire() as connection:
        await connection.execute(sql)

async def inDatabase(peerID):
    sql = "SELECT peerID FROM crawlPeers WHERE peerID = $1;"
    async with connectionPool.acquire() as connection:
        result = await connection.fetch(sql, peerID)
    return [peerID, True] if len(result) > 0 else [peerID, False]

async def insertPeers(peers):
    async def insertPeer(peerID):
        try:
            sql = "INSERT INTO crawlPeers (peerID) VALUES ($1);"
            async with connectionPool.acquire() as connection:
                await connection.execute(sql, peerID)
            print("peer inserted:", peerID)
        except asyncpg.exceptions.UniqueViolationError:
            print("peer already inserted:", peerID)
    await asyncio.gather(*[insertPeer(peerID) for peerID in peers])

async def getPeers():
    sql = ("SELECT peerID FROM crawlPeers;")
    async with connectionPool.acquire() as connection:
        peers = await connection.fetch(sql)
    return peers

async def main():
    await dbInit()

if __name__ == "__main__":
    asyncio.run(main())