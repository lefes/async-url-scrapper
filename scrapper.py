import psycopg2
import asyncio
import asyncpg
import aiohttp
from time import sleep
from datetime import datetime
from urllib.parse import urlparse
import re

from config import DB

EXCLUDE = ['.jpg', '.png', '.pdf', '.psd', '.gif', '.avi', '.mpeg', '.mov',
             '.flac', '.flv', '.mkv', '.dvd', '.odt', '.xls', '.doc', '.docx',
              '.xlsx', '.mpp', '.zip', '.tar', '.rar', '.xml', '.css', '.js']

async def scrap(url, session, pool):
    try:
        async with session.get(url) as responce:
            status = responce.status
            data = await responce.read()
            real_url = responce.real_url
            date = datetime.now()
            size = len(data)
    except ConnectionResetError:
        return ''
    except TimeoutError:
        return ''
    except:
        return ''
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO url_info VALUES('{}', '{}', {}, '{}', '{}')".format(url, real_url, size, status, date))
    internalLinks = []
    externalLinks = []
    nl = urlparse(url).netloc
    rr = re.compile(r'href="(\/[^"]*|\?[^"]*|[^"]*' + nl + '[^"]*)"')
    try:
        data = data.decode('utf-8')
    except:
        data = ''
    for link in re.findall(rr, data):
        if link:
            if nl not in link:
                linkFull = url+link
            else:
                linkFull = link
            if linkFull[:-4] not in EXCLUDE and linkFull[:-4] not in EXCLUDE:
                if linkFull not in internalLinks:
                    if link != '/':
                        internalLinks.append(linkFull)
    externalLinks += await getExternal(url, data)
    for link in internalLinks:
        try:
            async with session.get(link) as responce:
                data = await responce.read()
                externalLinks += await getExternal(link, data)
        except ConnectionResetError:
            pass
        except TimeoutError:
            pass
        except:
            pass
    for link in externalLinks:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO links VALUES('{}', '{}');".format(url, link))
        async with pool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM urls WHERE url='{}';".format(link))
        if not result:
            date = datetime.now()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("INSERT INTO urls(url, time_added) VALUES('{}', '{}');".format(link, date))

async def getExternal(url, data):
    extLinks = []
    urlLink = urlparse(url)
    rr = re.compile(r'href="((http|www|\/\/)[^"]+)"')
    if type(data) != str:
        try:
            data = data.decode('utf-8')
        except:
            data = ''
    for link in re.findall(rr, data):
        if link[0]:
            if urlLink.netloc not in link[0]:
                p = urlparse(link[0])
                if p.netloc and '.' in p.netloc:
                    if p.scheme:
                        extLink = p.scheme + '://' + p.netloc
                    else:
                        extLink = urlLink.scheme + '://' + p.netloc
                    if extLink not in extLinks:
                        if '.tumblr.com' not in extLink:
                            extLinks.append(extLink)
    return extLinks

async def crawling(conn, pool):
    c = conn.cursor()
    while True:
        c.execute('''SELECT url FROM urls WHERE checked='FALSE' ORDER BY random() LIMIT 1 FOR UPDATE;''')
        url = c.fetchone()
        if url:
            url = (url[0]).strip()
            c.execute("UPDATE urls SET checked='TRUE' WHERE url='{}'".format(url))
            conn.commit()
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                print('---'+url+'--- START ---')
                await scrap(url, session, pool)
                print('---'+url+'--- DONE ---')
        else:
            await asyncio.sleep(1)

async def main():
    conn = psycopg2.connect(user=DB['username'], password=DB['password'],
                                 database=DB['dbname'], host=DB['ip'])
    pool = await asyncpg.create_pool(user=DB['username'], password=DB['password'],
                                database=DB['dbname'], host=DB['ip'], max_size=20, min_size=20)
    tasks = []
    for _ in range(100):
        task = asyncio.create_task(crawling(conn, pool))
        tasks.append(task)
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())