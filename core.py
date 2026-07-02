import asyncio      # 异步编程支持，用于并发扫描
import httpx        # 异步 HTTP 客户端，用于发送请求
import json         # 读取 JSON 格式的配置文件
import aiofiles     # 异步文件读写，避免阻塞事件循环
import random       # 随机选择 User-Agent
import os           # 处理文件路径


class web_catalog_scanner:
    """
    Web 目录扫描器
    基于 asyncio + 生产者-消费者模式，异步并发扫描目标网站的目录和文件。
    """

    def __init__(self):
        """
        初始化扫描器：加载配置文件，解析各项参数。
        """
        # 获取当前脚本所在目录，用于把相对路径转成绝对路径
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # 加载 config.json 配置
        config = self.load_config()

        # 目标网址，去掉末尾多余的斜杠，避免生成 URL 时出现双斜杠
        self.url = config['url'].rstrip('/')

        # 字典文件路径，如果是相对路径则基于脚本目录拼接
        self.file = config['file']
        if not os.path.isabs(self.file):
            self.file = os.path.join(base_dir, self.file)

        # 代理设置，例如 "http://127.0.0.1:8080"；为 null 时不使用代理
        self.proxy = config['proxy']

        # 自定义请求头，可在此放 Cookie、Token 等
        self.header = config['header']

        # 状态码白名单：只有返回这些状态码的 URL 才会被记录
        self.status = config['status']

        # 单次请求超时时间（秒）
        self.timeout = config['timeout']

        # 请求失败后的重试次数
        self.retries = config['retries']

        # 需要拼接的后缀列表，空字符串 "" 表示扫描目录本身
        self.suffixes = config['suffixes']

        # 随机 User-Agent 池，每次请求随机选一个
        self.random_UA = config['random_UA']

        # 并发消费者数量，相当于同时发起请求的线程数
        self.max_worker = config['max_worker']

        # 扫描结果保存路径，如果是相对路径则基于脚本目录拼接
        self.result_file = config['result_file']
        if not os.path.isabs(self.result_file):
            self.result_file = os.path.join(base_dir, self.result_file)

        # 写入队列：消费者把命中的 URL 放入这里，writer 负责写入文件
        self.write_queue = asyncio.Queue()

        # 进度队列：消费者每完成一个任务放入 1，进度条任务读取并更新显示
        self.progress_queue = asyncio.Queue()

        # 总任务数，由 producer 计算后写入，用于显示进度百分比
        self.total_task = 0

    async def web_scan(self):
        """
        启动整个扫描流程：
        1. 创建 HTTP 客户端
        2. 启动生产者、消费者、写入器、进度条任务
        3. 等待所有任务完成或出现异常
        4. 清理资源，尽量保证已扫描到的结果不丢失
        """
        tasks = []
        try:
            # 创建异步 HTTP 客户端，设置超时和代理
            async with httpx.AsyncClient(timeout=self.timeout, proxy=self.proxy) as client:
                # 启动结果写入任务
                writer_task = asyncio.create_task(self.writer())
                # 启动进度显示任务
                progress_task = asyncio.create_task(self.show_progress())
                # 创建 URL 任务队列，限制队列大小以控制内存占用
                queue = asyncio.Queue(maxsize=self.max_worker * 2)
                # 启动生产者任务，负责从字典读取并生成 URL
                prod = asyncio.create_task(self.producer(self.file, queue))
                # 启动多个消费者任务，负责并发请求 URL
                consumers = [
                    asyncio.create_task(self.consumer(queue, client))
                    for _ in range(self.max_worker)
                ]

                # 收集所有任务，便于后续统一取消/清理
                tasks.append(prod)
                tasks.append(writer_task)
                tasks.append(progress_task)
                tasks.extend(consumers)

                # 等待生产者完成（字典读取完毕）
                await prod
                # 等待队列中所有 URL 被处理完
                await queue.join()
                # 等待所有消费者任务退出
                await asyncio.gather(*consumers)

                # 正常结束：发送哨兵 None，让进度条和写入任务优雅退出
                await self.progress_queue.put(None)
                await progress_task
                await self.write_queue.put(None)
                await writer_task

                print("扫描完成")
        except scanner_error as e:
            # 扫描器内部错误（如配置文件、字典文件问题）
            print(f"扫描失败: {e}")
        except asyncio.CancelledError:
            # 用户手动取消（如 Ctrl+C）
            print("扫描被取消")
        except Exception as e:
            # 其他未预期错误
            print(f"未预期的错误: {e}")
        finally:
            # 异常退出时：先取消生产者和消费者，停止产生新结果
            other_tasks = [t for t in tasks if t not in (writer_task, progress_task)]
            for t in other_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*other_tasks, return_exceptions=True)

            # 再给写入器和进度条发送哨兵，让它们把已入队的结果处理完再退出
            if not writer_task.done():
                await self.write_queue.put(None)
                await writer_task
            if not progress_task.done():
                await self.progress_queue.put(None)
                await progress_task

    def check(self, response):
        """
        判断 HTTP 响应状态码是否在白名单中。
        参数 response：httpx 返回的响应对象
        返回：True 表示命中，False 表示忽略
        """
        return response.status_code in self.status

    async def request(self, client, url, header=None):
        """
        发送一次 GET 请求。
        参数 client：httpx 异步客户端
        参数 url：请求目标 URL
        参数 header：请求头字典
        返回：响应对象；发生异常时返回 None，由调用方决定是否重试
        """
        try:
            response = await client.get(url, headers=header)
            return response
        except Exception:
            return None

    async def writer(self):
        """
        结果写入器：从 write_queue 中取出 URL，写入 result_file。
        当取到 None 哨兵时停止写入。
        """
        try:
            async with aiofiles.open(self.result_file, 'w', encoding='utf-8') as fp:
                while True:
                    line = await self.write_queue.get()
                    if line is None:
                        break
                    await fp.write(line + '\n')
        except Exception as e:
            raise scanner_error(f"写入结果文件失败 ({self.result_file}): {e}") from e

    def load_config(self, path="config.json"):
        """
        加载 JSON 配置文件。
        参数 path：配置文件路径，默认读取脚本同目录下的 config.json
        返回：解析后的配置字典
        """
        if not os.path.isabs(path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            path = os.path.join(base_dir, path)
        try:
            print("导入配置文件中......")
            with open(path, 'r', encoding='utf-8') as fp:
                config = json.load(fp)
            print("导入成功")
            return config
        except FileNotFoundError as e:
            raise scanner_error(f"配置文件不存在: {path}") from e
        except json.JSONDecodeError as e:
            raise scanner_error(f"配置文件 JSON 格式错误: {e}") from e
        except Exception as e:
            raise scanner_error(f"加载配置文件失败: {e}") from e

    async def producer(self, file, queue):
        """
        生产者：读取字典文件，把每一行拼接成完整 URL 放入任务队列。
        规则：
        - 空行跳过
        - 如果行中已包含类似文件扩展名（最后一节含点号），直接作为一个 URL
        - 否则为每一行拼接所有后缀，生成多个 URL
        最后放入 max_worker 个 None 哨兵，通知消费者结束。
        """
        try:
            # 先计算总任务数，用于进度条显示
            self.total_task = await self.count_line(file)
            async with aiofiles.open(file, 'r', encoding='utf-8') as fp:
                async for line in fp:
                    line = line.strip()
                    if not line:
                        continue
                    url = f"{self.url}/{line.lstrip('/')}"
                    # 判断是否为带扩展名的路径
                    if '.' in line.rsplit('/', 1)[-1]:
                        await queue.put(url)
                    else:
                        # 为无扩展名的路径拼接所有后缀
                        for i in self.suffixes:
                            await queue.put(url + i)
        except Exception as e:
            raise scanner_error(f"读取字典文件失败 ({file}): {e}") from e
        else:
            # 所有 URL 生成完毕后，向每个消费者发送结束哨兵
            for _ in range(self.max_worker):
                await queue.put(None)

    async def consumer(self, queue, client):
        """
        消费者：从队列中取出 URL，发起 HTTP 请求，根据状态码判断是否命中。
        每个 URL 会重试 retries 次；命中后放入 write_queue 等待写入文件。
        """
        while True:
            url = await queue.get()
            if url is None:
                # 收到哨兵，结束当前消费者
                queue.task_done()
                break
            try:
                for attempt in range(0, self.retries + 1):
                    # 复制基础请求头，并随机设置 User-Agent
                    header = self.header.copy()
                    header.update({"User-Agent": random.choice(self.random_UA)})
                    response = await self.request(client, url, header)
                    if response is None:
                        # 请求异常，若不是最后一次则指数退避重试
                        if attempt < self.retries:
                            await asyncio.sleep(1 * (attempt + 1))
                    else:
                        # 状态码命中则记录结果
                        if self.check(response):
                            await self.write_queue.put(url)
                        break
            finally:
                # 通知进度条更新
                if url is not None:
                    await self.progress_queue.put(1)
                # 标记当前队列任务已完成
                queue.task_done()

    async def show_progress(self):
        """
        进度显示：从 progress_queue 读取完成的任务数，实时显示百分比。
        取到 None 哨兵时停止更新。
        """
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
            # 把百分比放大 10 倍取整，用于控制刷新频率
            percent = int(percent * 10)
            if percent > last_percent:
                print(f"\r进度: {completed}/{self.total_task} ({percent / 10:.1f}%)", end="", flush=True)
                last_percent = percent
        print()

    async def count_line(self, file):
        """
        计算字典文件对应的总任务数。
        带扩展名的行算 1 个任务；无扩展名的行按后缀数量计算任务数。
        返回值用于进度条百分比计算。
        """
        count = 0
        try:
            async with aiofiles.open(file, 'r', encoding='utf-8') as f:
                async for line in f:
                    if line.strip() and '.' in line.rsplit('/', 1)[-1]:
                        count += 1
                    elif line.strip():
                        count += len(self.suffixes)
            return count
        except Exception as e:
            raise scanner_error(f"加载字典文件失败: {e}") from e


class scanner_error(Exception):
    """
    扫描器内部自定义异常，用于区分扫描逻辑错误和其他未预期错误。
    """
    pass
