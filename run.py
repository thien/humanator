import sqlite3

conn = sqlite3.connect('chat.db')


results = {}

def getHeaders(cursor):
	return list(map(lambda x: x[0], cursor.description))

def getChatHandle(conn):
	handle = {}
	command = 'SELECT * from chat_handle_join'
	cursor = conn.execute(command)
	headers = getHeaders(cursor)
	for results in cursor:
		container = {}
		count = 0
		for entry in results:
			container[headers[count]] = entry
			count += 1
		# print(container)
		handle[container['handle_id']] = container
	return handle

def createResultsContainer(cursor):
	headers = getHeaders(cursor)
	container = {}
	for results in cursor:
		container = {}
		count = 0
		for entry in results:
			container[headers[count]] = entry
			count += 1
	return container

def getContactDetails(conn):
	contacts = {}
	command = 'SELECT * from chat'
	cursor = conn.execute(command)
	headers = getHeaders(cursor)
	for results in cursor:
		container = {}
		count = 0
		for entry in results:
			container[headers[count]] = entry
			count += 1
		# print(container)
		contacts[container['ROWID']] = container
	return contacts

def chatIdentifier(handle,contacts,message):
	handle_id = message['handle_id']
	chat_id = handle[handle_id]['chat_id']
	identifier = contacts[chat_id]['chat_identifier']
	return identifier

def getMessages(conn):
	messages = []
	command = 'SELECT * FROM message WHERE text like "%Gus%";'
	cursor = conn.execute(command)
	headers = getHeaders(cursor)
	# add to messages list
	for results in cursor:
		container = {}
		count = 0
		for entry in results:
			container[headers[count]] = entry
			count += 1
		messages.append(container)

	return messages

def printMessage(handle,contacts,message):
	person = chatIdentifier(handle,contacts,message)
	print(person,":", message['text'])

def init(conn):
	handle = getChatHandle(conn)
	contacts = getContactDetails(conn)
	messages = getMessages(conn)

	for i in messages:
		printMessage(handle,contacts,i)

init(conn)