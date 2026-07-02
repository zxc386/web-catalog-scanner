import core
import asyncio
if __name__ == '__main__':
    scaner1 = core.web_catalog_scanner()
    asyncio.run(scaner1.web_scan())