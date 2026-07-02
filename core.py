import asyncio
import httpx
import json
import aiofiles
import random
import os

class web_catalog_scanner:
    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config = self.load_config()
        self.url = config['url'].rstrip('/')
        self.file = config['file']
        if not os.path.isabs(self.file):
            self.file = os.path.join(base_dir, self.file)
        self.proxy = config['proxy']
        self.header = config['header']
        self.status = config['status']
        self.timeout = config['timeout']
        self.retries = config['retries']
        self.suffixes = config['suffixes']
        self.random_UA = config['random_UA']
        self.max_worker = config['max_worker']
        self.result_file = config['result_file']
        if not os.path.isabs(self.result_file):
            self.result_file = os.path.join(base_dir, self.result_file)
        self.write_queue = asyncio.Queue()
        self.progress_queue = asyncio.Queue()
        self.total_task = 0

    async def web_scan(self):
        tasks = []
        try:
            async with httpx.AsyncClient(timeout = self.timeout,proxy = self.proxy) as client:
                writer_task = asyncio.create_task(self.writer())
                progress_task = asyncio.create_task(self.show_progress())
                queue = asyncio.Queue(maxsize=self.max_worker * 2)
                prod = asyncio.create_task(self.producer(self.file, queue))
                consumers = [
                    asyncio.create_task(self.consumer(queue,client))
                    for _ in range(self.max_worker)
                ]
                tasks.append(prod)
                tasks.append(writer_task)
                tasks.append(progress_task)
                tasks.extend(consumers)
                await prod
                await queue.join()
                await asyncio.gather(*consumers)
                await self.progress_queue.put(None)
                await progress_task
                await self.write_queue.put(None)
                await writer_task
                print("扫描完成")
        except scanner_error as e:
            print(f"扫描失败: {e}")
        except asyncio.CancelledError:
            print("扫描被取消")
        except Exception as e:
            print(f"未预期的错误: {e}")
        finally:
            other_tasks = [t for t in tasks if t not in (writer_task, progress_task)]
            for t in other_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*other_tasks, return_exceptions=True)
            if not writer_task.done():
                await self.write_queue.put(None)
                await writer_task
            if not progress_task.done():
                await self.progress_queue.put(None)
                await progress_task       


    def check(self,response):
        return response.status_code in self.status

    async def request(self,client,url,header = None):
        try:
            response = await client.get(url,headers = header)
            return response
        except Exception:
            return None
    
    async def writer(self):
        try:
            async with aiofiles.open(self.result_file,'w',encoding='utf-8') as fp:
                while True:
                    line = await self.write_queue.get()
                    if line is None:
                        break
                    await fp.write(line + '\n')
        except Exception as e:
            raise scanner_error(f"写入结果文件失败 ({self.result_file}): {e}") from e


    def load_config(self,path="config.json"):
        if not os.path.isabs(path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            path = os.path.join(base_dir, path)
        try:
            print("导入配置文件中......")
            with open(path,'r',encoding='utf-8') as fp:
                config = json.load(fp)
            print("导入成功")
            return config
        except FileNotFoundError as e:
            raise scanner_error(f"配置文件不存在: {path}") from e
        except json.JSONDecodeError as e:
            raise scanner_error(f"配置文件 JSON 格式错误: {e}") from e
        except Exception as e:
            raise scanner_error(f"加载配置文件失败: {e}") from e
    

    async def producer(self,file,queue):
        try:
            self.total_task = await self.count_line(file)
            async with aiofiles.open(file,'r',encoding='utf-8') as fp:
                async for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    url = f"{self.url}/{line.lstrip('/')}"
                    if  '.' in line.rsplit('/', 1)[-1]:
                        await queue.put(url)
                    else:
                        for i in self.suffixes:
                            await queue.put(url + i)
        except Exception as e:
            raise scanner_error(f"读取字典文件失败 ({file}): {e}") from e
        else:
            for _ in range(self.max_worker):
                await queue.put(None)


    async def consumer(self,queue,client):
        while True:
            url = await queue.get()
            if url is None:
                queue.task_done() 
                break
            try:
                for attempt in range(0,self.retries+1):
                    header = self.header.copy()
                    header.update({"User-Agent": random.choice(self.random_UA)})
                    response = await self.request(client,url,header)
                    if (response == None):
                        if attempt < self.retries:
                            await asyncio.sleep(1 * (attempt + 1))
                    else:
                        if (self.check(response)):
                            await self.write_queue.put(url)
                        break
            finally:
                if url is not None:
                    await self.progress_queue.put(1)
                queue.task_done() 

    async def show_progress(self):
        last_percent = 0
        completed = 0
        while True:
            item = await self.progress_queue.get()
            if item is None:
                break
            completed += 1
            if self.total_task > 0:
                percent = completed / self.total_task * 100
            else:
                percent = 0
            percent = int(percent * 10)
            if (percent > last_percent):
                print(f"\r进度: {completed}/{self.total_task} ({percent/10:.1f}%)", end="", flush=True)
                last_percent = percent
        print()

    async def count_line(self,file):
        count = 0
        try:
            async with aiofiles.open(file,'r',encoding='utf-8') as f:
                async for line in f:
                    if line.strip() and '.' in line.rsplit('/', 1)[-1]:
                        count += 1
                    elif line.strip():
                        count += len(self.suffixes)
            return count
        except Exception as e:
            raise scanner_error(f"加载字典文件失败: {e}") from e

class scanner_error(Exception):
    pass