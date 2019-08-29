import asyncio
from aiobloom.aiobloom import BloomFilter


# 1 minite 1


    
async def main():
    call_n = 0
    b = BloomFilter(capacity=100)
    await b.connect()
    test_key = "test_key"
    test_key_false = "false"
    await b.add("test_key")
    print(await b.exist(test_key))
    print(await b.exist(test_key_false))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())