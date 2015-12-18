'''
	Low level connection Protocol
	no BSON involved
'''
import struct, collections, asyncio


__all__ = 'Connection', 'ReplyError', 'CursorNotFound', \
	'QueryFailure', 'ConnectionLost'

class ReplyError(Exception):
	pass

class CursorNotFound(ReplyError):
	pass

class QueryFailure(ReplyError):
	pass

class ConnectionLost(ReplyError):
	pass


class ChunkBuffer:
	def __init__(self):
		self._deque = collections.deque()
		self._length = 0

	def __len__(self):
		return self._length

	def append(self, chunk):
		self._deque.append(chunk)
		self._length += len(chunk)

	def extract(self, length, joiner=b''.join):
		if self._length < length:
			return None

		self._length -= length

		deque = self._deque
		remainder_len = length

		chunks = collections.deque()
		chunk = deque.popleft()
		length -= len(chunk)

		while length > 0:
			chunks.append(chunk)
			chunk = deque.popleft()
			length -= len(chunk)

		if length < 0:
			deque.appendleft(chunk[length:])
			chunk = chunk[:length]

		chunks.append(chunk)

		return joiner(chunks)


unpack_reply = struct.Struct('<iiii iqii').unpack	# 36 bytes
pack_request = struct.Struct('<iiii i').pack		# 20 bytes
pack_update = struct.Struct('<xi').pack				# 5 bytes
pack_query = struct.Struct('<xii').pack				# 9 bytes
pack_get_more = struct.Struct('<xiq').pack			# 13 bytes
pack_delete = struct.Struct('<xi').pack				# 5 bytes
pack_kill_cursors = struct.Struct('<i').pack		# 4 bytes
pack_cursor = struct.Struct('<q').pack				# 8 bytes
join_bytes = b''.join

class Connection(asyncio.Protocol):
	def __init__(self, host='localhost', port=27017, loop=None):
		self._host = host
		self._port = port
		self._loop = loop or asyncio.get_event_loop()

		self._connection_future = None
		self._disconnection_future = None
		self._reconnect = True
		self._is_connected = False
		self._transport = False

		self._chunk_buffer = None
		self._current_reply = None
		self._request_futures = {}
		self._next_request_id = None

	#def __del__(self):
	#	if self._is_connected:
	#		self._loop.create_task(self.disconnect())

	async def connect(self):
		if not self._is_connected:
			self._connection_future = asyncio.Future()
			self._disconnection_future = asyncio.Future()
			await self._loop.create_connection(lambda: self, self._host, self._port)
			await self._connection_future

	async def disconnect(self):
		if self._transport:
			self._reconnect = False
			self._transport.close()
			await self._disconnection_future


	def connection_made(self, transport):
		self._transport = transport
		self._is_connected = True

		self._chunk_buffer = ChunkBuffer()
		self._current_reply = None

		self._request_futures = {}
		self._next_request_id = 1

		# authentication here

		self._connection_future.set_result(None)


	def connection_lost(self, exc):

		self._transport = None
		self._is_connected = False

		# lanzar retries??
		if exc:
			for future in self._request_futures:
				future.set_exception(exc)
		else:
			for future in self._request_futures:
				future.cancel() # cancel or connection lost?
		self._request_futures = {}

		# TODO if reconnect->reconnect
		self._disconnection_future.set_result(None)

	def data_received(self, chunk):
		append_to_chunk_buffer = self._chunk_buffer.append
		extract_from_chunk_buffer = self._chunk_buffer.extract

		append_to_chunk_buffer(chunk)

		reply = self._current_reply

		while True:
			if not reply:
				## unpack a reply from buffer
				reply = extract_from_chunk_buffer(36)

				if reply is None:
					self._current_reply = None
					return

				reply = unpack_reply(reply)

			message_length,\
			request_id,\
			response_to,\
			op_code,\
			response_flags,\
			cursor_id,\
			starting_from,\
			number_returned = reply

			assert(op_code == 1)

			payload_length = message_length - 36

			if payload_length:
				payload_data = extract_from_chunk_buffer(payload_length)
				if payload_data is None:
					self._current_reply = reply
					return ## awaiting current reply
			else:
				payload_data = b''

			request_future = self._request_futures.get(response_to, None)

			if request_future is None:
				reply = None
				continue ## ignores current reply

			if response_flags & 1: # CursorNotFound
				request_future.set_exception( CursorNotFound((reply, payload_data)) )

			if response_flags & 2: # QueryFailure future.set_exception
				request_future.set_exception( QueryFailure((reply, payload_data)) )

			#if response_flags & 4: # ShardConfigStale -- ignored for now
			#if response_flags & 8: # AwaitCapable -- ignored for now

			request_future.set_result((reply, payload_data))

	def _send_request(self, *chunks):
		self._transport.write(join_bytes(chunks))

		request_id = self._next_request_id
		request_future = asyncio.Future()
		self._request_futures[request_id] = request_future
		self._next_request_id = (request_id + 1) & 0xFFFFFFFF
		return request_future


	def OP_UPDATE(self, collection, selector, update, upsert=False, multi_update=False):
		return self._send_request(
			pack_request(
				# message length
				25 + len(collection) + len(selector) + len(update),
				self._next_request_id,	# request id
				0,						# response_to
				2001,					# op code
				0,						# flags
			),
			collection, 				# full collection name
			pack_update(upsert | multi_update << 1), # options
			selector,
			update
		)

	def OP_INSERT(self, collection, documents, continue_on_error=False):
		return self._send_request(
			pack_request(
				# message length
				21 + len(collection) + len(documents),
				self._next_request_id,	# request id
				0,						# response to
				2002,					# op code
				continue_on_error << 0,	# flags
			),
			collection, 				# full collection name
			b'\0',
			documents,
		)

	def OP_QUERY(self, collection, query, count, skip=0, selector=b'',
			tailable_cursor=False, await_data=False, exhaust=False):
		return self._send_request(
			pack_request(
				# message length
				29 + len(collection) + len(query) + len(selector),
				self._next_request_id,	# request id
				0,						# response to
				2004,					# op code
										# flags
				tailable_cursor << 1 \
				| await_data << 5 \
				| exhaust << 6
			),
			collection, 				# full collection name
			pack_query(skip, count),
			query,
			selector
		)

	def OP_GET_MORE(self, collection, count, cursor_id):
		return self._send_request(
			pack_request(
				33 + len(collection),	# message length
				self._next_request_id,	# request id
				0,						# response to
				2005,					# op code
				0						# flags
			),
			collection, 				# full collection name
			pack_get_more(count, cursor_id)
		)

	def OP_DELETE(self, collection, selector, single_remove=False):
		return self._send_request(
			pack_request(
				25 + len(collection) + len(selector), # message length
				self._next_request_id,	# request id
				0,						# response to
				2006,					# op code
				0						# flags
			),
			collection, 				# full collection name
			pack_delete(single_remove<<0),
			selector
		)

	def OP_KILL_CURSORS(self, cursors=[]):
		return self._send_request(
			pack_request(
				24 + len(cursors) * 8, 	# message length
				self._next_request_id,	# request id
				0,						# response to
				2005,					# op code
				0						# flags
			),
			pack_kill_cursors(len(cursors)),
			*( pack_cursor(cursor) for cursor in cursors )
		)
