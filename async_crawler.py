# -*- coding: utf-8 -*- 

import os, datetime
import asyncio, aiohttp, aiofiles

async def image_downloader(task_q):
    async with aiohttp.ClientSession() as session:
        while not task_q.empty():
            url = await task_q.get()
            try:
                async with session.get(url, timeout=5) as resp:
                    assert resp.status == 200
                    content = await resp.read()
            except Exception as err:
                print('Error for url {}: {}'.format(url, err))
            else:
                fname = split_fname(url)
                print('{} is ok'.format(fname))
                await save_file(fname, content)

def split_fname(url):
    form = url.strip().split('.')[-1][:3]
    fname = url.strip().split('\\')[-1]
    fname = fname + '.' + form
    return fname

async def save_file(fname, content):
    async with aiofiles.open(fname, mode='wb') as f:
        await f.write(content)

async def produce_tasks(url_file, task_q):
    with open(url_file, 'r') as f:
        urls = f.readlines()
        for url in urls:
            url = url.strip()
            if os.path.isfile(split_fname(url)):
                continue
            await task_q.put(url)

async def run(url_file):
    task_q = asyncio.Queue(maxsize=1000)
    task_producer = asyncio.ensure_future(produce_tasks(url_file, task_q))
    workers = [asyncio.ensure_future(image_downloader(task_q)) for _ in range(10)]
    try:
        await asyncio.wait(workers + [task_producer])
    except Exception as err:
        print(err.msg)

if __name__ == '__main__':
    print('start at', datetime.datetime.utcnow())
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asyncio.ensure_future(run()))
    print('end at', datetime.datetime.utcnow())
