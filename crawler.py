import asyncio
import random

import db
import requests


async def filterOnlinePeers(peers):
    peers = await asyncio.gather(*[requests.peerOnline(peerID) for peerID in peers])
    peers = [peerID for peerID, peerOnline in peers if peerOnline]
    return peers


async def filterOutImportedPeers(peers):
    peers = await asyncio.gather(*[db.inDatabase(peerID) for peerID in peers])
    peers = [peerID for peerID, inDatabase in peers if not inDatabase]
    return peers


async def getCrawlPeers(peers):
    crawlPeers = await asyncio.gather(*[requests.getCrawlPeers(peerID) for peerID in peers])
    crawlPeers = [y for x in crawlPeers for y in x]
    crawlPeers = list(set(crawlPeers))
    return crawlPeers


def getRandomSample(population, length):
    if len(population) > length:
        return random.sample(population, length)
    else:
        return population


async def crawlPass():
    print("getting all peers from the database...")
    seedPeers = [peer[0] for peer in await db.getPeers()]
    print(len(seedPeers))

    print("getting subset of peers which are online...")
    seedPeers = await filterOnlinePeers(seedPeers)
    print(len(seedPeers))

    print("getting random subset of peers...")
    seedPeers = getRandomSample(seedPeers, 100)
    print(len(seedPeers))
    
    print("crawling each peer...")
    crawlPeers = await getCrawlPeers(seedPeers)
    print(len(crawlPeers))
    
    print("geting subset of peers which are not in the database...")
    crawlPeers = await filterOutImportedPeers(crawlPeers)
    print(len(crawlPeers))
    
    #print("getting subset of peers which are online...")
    #crawlPeers = await filterOnlinePeers(crawlPeers)
    #print(len(crawlPeers))
    
    print("inserting the peers into the database...")
    await db.insertPeers(crawlPeers)


async def importConnectedPeers():
    print("getting the connected peers...")
    connectedPeers = await requests.fetchConnectedPeers()
    print(len(connectedPeers))

    print("getting subset of peers which are not in the database...")
    connectedPeers = await filterOutImportedPeers(connectedPeers)
    print(len(connectedPeers))

    print("inserting the peers into the database...")
    await db.insertPeers(connectedPeers)


async def crawl():
    requests.initSemaphore()
    await db.dbInit()
    while True:
        await importConnectedPeers()
        await crawlPass()


if __name__ == "__main__":
    asyncio.run(crawl())