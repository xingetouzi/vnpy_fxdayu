from functools import partial

async def fetch(session, url, method="GET", json=None):
    func = getattr(session, method.lower())
    if json:
        func = partial(func, url, json=json)
    else:
        func = partial(func, url)
    async with func() as resp:
        ret = await resp.json()
        return resp.status, ret

async def fetch_stream(session, url):
    async with session.get(url) as response:
        async for data in response.content:
            yield data
    return
