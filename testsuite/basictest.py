import asyncio, bson, collections, aiomongodb



async def main():
	c = aiomongodb.Client()
	await c.connect()
	db = c.database('test')


	query = db._cmd.find_one({'listCommands': 1})

	async for item in query:
		for command, info in item.commands.items():
			print(command)
			print('  ', info)


	#doc = {'listCommandss':1}

	#response = await c.OP_QUERY(b'test.$cmd', bson.BSON.encode(doc), 1)

	#response = bson.BSON.decode(response.payload, as_class=collections.OrderedDict)

	#print(response)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
