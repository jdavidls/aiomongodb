'''
	HighLevel -> LowLevel (protocol) classes


'''
#cybson
import collections, asyncio, bson, random

from .connection import Connection

MappingProxy = type(type.__dict__)


class AttributeKeyError(AttributeError, KeyError):
	pass

class odict(collections.OrderedDict):
	__getattr__ = collections.OrderedDict.__getitem__
	__setattr__ = collections.OrderedDict.__setitem__
	__delattr__ = collections.OrderedDict.__delitem__
	def __missing__(self, key):
		raise AttributeKeyError(key)

_empty_doc = MappingProxy(odict())


bson_encode = bson.BSON.encode
_bson_decode = bson.BSON.decode
_bson_decode_all = bson.decode_all

#def bson_encode(doc):
#	return _bson_encode(doc)

def bson_decode(raw):
	return _bson_decode(raw, odict)

def bson_encode_multi(docs):
	return b''.join( _bson_encode(doc) for doc in docs )

def bson_decode_multi(raw):
	doc = _bson_decode_all(raw, odict)
	return isinstance(doc, list) and doc or [doc]


class Client:
	def __init__(self, loop=None, host=None, port=None, connections=8):
		'''
		'''
		self._loop = loop = loop or asyncio.get_event_loop()
		self._host = host = host or connection.default_host
		self._port = port = port or connection.default_port

		self._next_connection = 0

		self._connection_pool = [ Connection(loop, host, port) for n in range(connections) ]

		self._databases = databases = {}
		self._databases_proxy = MappingProxy(databases)
		self._is_connected = False

		self._cursors = set()

		self._server_version = None

	@property
	def databases(self):
		return self._databases_proxy

	async def connect(self):
		disconnection_futures = await asyncio.gather(
			*(c.connect() for c in self._connection_pool),
			loop = self._loop
		)

		for disconnection_future in disconnection_futures:
			disconnection_future.add_done_callback(self._connection_lost)

		self._is_connected = True

	def _connection_lost(self, connection):

		print('connection lost', connection)
		self._connection_pool.remove(connection)

		if not self._is_connected:
			return

		reconnection = self._loop.create_task(connection.connect())
		@reconnection.add_done_callback
		def reconnection_made(disconnection_future):
			print('reconnection made', connection)
			self._connection_pool.add(connection)
			disconnection_future.add_done_callback(self._connection_lost)

	async def disconnect():
		self._is_connected = False
		raise NotImplementedError

	def connection(self):
		#next_connection_idx = self._next_connection_idx
		#connection = self._connection_pool[next_connection_idx]
		# la conexion debe estar activa,
		#self._next_connection_idx = (next_connection_idx + 1) % len(self._connection_pool)
		#return connection

		return random.sample(self._connection_pool, 1)[0]

	def database(self, name):
		database = self._databases.get(name, None)
		if database is None:
			database = Database(self, name)
			self._databases[name] = database
		return database



class Database:
	def __init__(self, client, name):
		self._client = client
		self._name = name

		self._collections = collections = {}
		self._collections_proxy = MappingProxy(collections)

		self._cmd = Collection(self, '$cmd')

	client = property(lambda self: self._client)
	name = property(lambda self: self._name)
	collections = property(lambda self: self._collections_proxy)

	def collection(self, name):
		collection = self._collections.get(name, None)
		if collection is None:
			collection = Collection(self, name)
			self._collections[name] = collection
		return collection

	#
	# def __getattr__(self, command):
	# 	async def cmd(**parameters):
	# 		odict((command, 1) + parameters.items())
	# 	return cmd

class Collection:
	'''
		contempla:
			sesiones (colleciones)
			sistemas de comunicacion (cursores esperando datos)
	'''
	# batch_size = 100
	def __init__(self, database, name):
		self._client = database._client
		self._database = database # proxy(database)
		self._name = name

		self._cstr_name = b'.'.join((database._name.encode(), name.encode()))

	def find(self, query, projection=_empty_doc):
		return Query(self, query, projection)

	def find_one(self, query, projection=_empty_doc):
		return Query(self, query, projection, 0, 1)

	def __getitem__(self, id):
		return Query(self, {'_id': id}, _empty_doc, 0, 1)

class Query:
	'''
		Representa una consulta, cachea la codificacion
		para futuros usos.
	'''
	def __init__(self, collection, query, projection=_empty_doc, skip=0, limit=None):
		self._client = collection._client
		self._collection = collection

		self._query = query
		self._encoded_query = bson_encode(query)

		self._projection = projection
		self._encoded_projection = projection and bson_encode(projection) or b''

		self._skip = skip
		self._limit = limit
		self._tailable = False
		self._exhaust = False

	async def __aiter__(self):
		'''
			returns a cursor.
		'''
		return Cursor(self)



	def __len__(self):
		'''
			returna un future que se resolverá con el
			numero de elementos que alcanza esta query
		'''
		return asyncio.Future()

	# getitem -> future
	# setitem -> future(error)
	# slice -> cursor



class Cursor:
	'''
	'''
	def __init__(self, query):
		self._client = query._client
		self._query = query

		self._deque = collections.deque()
		self._cursor_id = None

		self._cstr_collection = query._collection._cstr_name
		self._encoded_query = query._encoded_query
		self._encoded_projection = query._encoded_projection

		self._batch_length = 25
		self._skip = query._skip
		self._limit = query._limit

		self._connection = connection = self._client.connection()

		self._future = future = connection.OP_QUERY(
			self._cstr_collection,
			self._encoded_query,
			self._encoded_projection,
			min(self._limit or 0xFFFFFFFF, self._batch_length),
			self._skip,
		)

	async def __anext__(self):
		deque = self._deque

		if not deque:
			future = self._future
			if future is None:
				raise StopAsyncIteration

			try:
				reply = await future
			except:
				## make reconection and request new query
				raise

			## try
			items = bson_decode_multi(reply.bson_payload)
			## raise BSON DECODE ERROR
			deque.extend(items)

			if self._limit:
				self._limit -= reply.number_returned

			self._skip += reply.number_returned

			self._cursor_id = cursor_id = reply.cursor_id

			if cursor_id:
				self._future = self._connection.OP_GET_MORE(
					self._cstr_collection,
					min(self._limit or 0xFFFFFFFF, self._batch_length),
					cursor_id
				)
			else:
				self._future = None ## stop the cursor here

		item = deque.popleft()
		## process item
		item = self._process_item(item)
		return item


	def _process_item(self, item):

		return item
