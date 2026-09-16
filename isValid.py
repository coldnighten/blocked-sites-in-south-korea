import asyncio
import aiohttp
import time
import logging
import sys
from aiohttp import TCPConnector
from urllib.parse import urlparse, urljoin

# 并发限制
CONCURRENT_REQUESTS = 50
# 连接池大小
CONNECTION_LIMIT = 100
# 每批次处理的 URL 数量
BATCH_SIZE = 1000
# 请求超时时间（秒）
TIMEOUT_SECONDS = 10

# 默认输入/输出文件名
DEFAULT_INPUT_FILE = 'domains.txt'
DEFAULT_OUTPUT_FILE = 'output.txt'

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("log.log"),
    logging.StreamHandler()
])

# 全局已写入集合，用于输出去重
written_urls = set()

async def fetch_url(session, url, output_file):
    start_time = time.time()
    try:
        async with session.get(url, timeout=TIMEOUT_SECONDS, allow_redirects=False) as response:
            end_time = time.time()
            duration = end_time - start_time
            status = response.status

            if 200 <= status < 300:
                # 2xx → 有效，记录原始 URL
                logging.info(f"VALID (2xx) - {url} (status: {status}, took {duration:.2f}s)")
                write_valid_url(url, output_file)
            elif 300 <= status < 400:
                # 3xx → 有效，但输出跳转后的目标链接
                location = response.headers.get('Location', '')
                # 处理相对路径跳转
                if location and not location.startswith(('http://', 'https://')):
                    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                    location = urljoin(base_url, location)
                logging.info(f"VALID (3xx) - {url} → {location} (status: {status}, took {duration:.2f}s)")
                write_valid_url(location, output_file)
            else:
                # 4xx / 5xx → 无效，仅记录日志
                logging.info(f"INVALID - {url} (status: {status}, took {duration:.2f}s)")

    except asyncio.TimeoutError:
        logging.error(f"TIMEOUT - {url} (exceeded {TIMEOUT_SECONDS}s)")
    except aiohttp.ClientConnectorError as e:
        logging.error(f"CONNECTION FAILED - {url}: {e}")
    except Exception as e:
        logging.error(f"ERROR - {url}: {e}")

def write_valid_url(valid_url, output_file):
    """写入有效 URL，附带输出去重"""
    if valid_url not in written_urls:
        written_urls.add(valid_url)
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(f"{valid_url}\n")

async def bound_fetch(sem, session, url, output_file):
    async with sem:
        await fetch_url(session, url, output_file)

def clean_url(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return url

async def process_urls(urls, output_file):
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=CONNECTION_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in urls:
            cleaned_url = clean_url(url.strip())
            if cleaned_url:
                task = bound_fetch(sem, session, cleaned_url, output_file)
                tasks.append(task)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logging.error("Tasks were cancelled due to asyncio.CancelledError")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

async def read_and_deduplicate_input(input_file):
    """读取输入文件，并做输入去重（保留首次出现的顺序）"""
    seen = set()
    unique_urls = []
    with open(input_file, 'r', encoding='utf-8') as file:
        for line in file:
            url = line.strip()
            if url and url not in seen:
                seen.add(url)
                unique_urls.append(url)
    return unique_urls

async def process_file_in_batches(input_file, output_file):
    unique_urls = await read_and_deduplicate_input(input_file)
    logging.info(f"Loaded {len(unique_urls)} unique URLs from {input_file}")

    for i in range(0, len(unique_urls), BATCH_SIZE):
        batch = unique_urls[i:i + BATCH_SIZE]
        await process_urls(batch, output_file)

def main():
    # 命令行参数：python script.py [input_file] [output_file]
    # 不传参则使用默认值
    input_file = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_FILE
    output_file = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_FILE

    # 清空输出文件
    open(output_file, 'w').close()
    # 清空全局去重集合（每次运行重置）
    written_urls.clear()

    # 创建事件循环并运行处理
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_file_in_batches(input_file, output_file))

if __name__ == '__main__':
    start_time = time.time()
    main()
    logging.info(f"Completed in {time.time() - start_time} seconds")
