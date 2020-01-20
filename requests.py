import asyncio
import aiohttp


def initSemaphore():
    global semaphore
    semaphore = asyncio.Semaphore(64)

async def asyncRequest(endpoint):
    async with semaphore:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://127.0.0.1:4002"+endpoint, timeout=10) as response:
                    data = await response.json()
            if type(data) == dict and data.get("success") == False:
                return "failure"
            else:
                return data         
        except asyncio.TimeoutError:
            return "timeout"

async def fetchConnectedPeers():
    endpoint = "/ob/peers"
    connectedPeers = await asyncRequest(endpoint)
    if type(connectedPeers) == list and len(connectedPeers) > 0 and type(connectedPeers[0]) == str:
        return connectedPeers
    else:
         return []

async def getCrawlPeers(peerID):
    async def fetchCrawlPeers(endpoint):
        peers = await asyncRequest(endpoint)
        if type(peers) == list and len(peers) > 0 and type(peers[0]) == str:
            return peers
        else:
            return []
    endpoints = ["/ob/closestpeers/{}".format(peerID),
                 "/ob/following/{}".format(peerID),
                 "/ob/followers/{}".format(peerID)]
    crawlPeers = await asyncio.gather(*[fetchCrawlPeers(endpoint) for endpoint in endpoints])
    crawlPeers = [y for x in crawlPeers for y in x]
    crawlPeers = list(set(crawlPeers))
    return crawlPeers

async def peerOnline(peerID):
    endpoint = "/ob/status/{}".format(peerID)
    status = await asyncRequest(endpoint)
    if type(status) == dict and status.get("status") == "online":
        return [peerID, True]
    else:
        return [peerID, False]


async def main():
    pass

if __name__ == "__main__":
    asyncio.run(main())

