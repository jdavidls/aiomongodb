import asyncio, bson
from aiomongodb.connection import Connection


async def main():
	c = Connection()
	await c.connect()

	doc = {'listCommands':1}

	response = c.OP_QUERY(b'test.$cmd', bson.BSON.encode(doc), 1)
	response = await response
	response = bson.BSON.decode(response[1])
	print(response)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
