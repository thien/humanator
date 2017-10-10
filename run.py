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
	return (person, message['text'])

def init(conn):
	handle = getChatHandle(conn)
	contacts = getContactDetails(conn)
	messages = getMessages(conn)
	metadata = {}

	import spacy
	nlp = spacy.load('en')

	for i in messages:
		message = printMessage(handle,contacts,i)
		message = (message[0],nlp(message[1]))
		print(message)
		metadata.setdefault(message[0],{})
		metadata[message[0]].setdefault('entities',{})
		for ent in message[1].ents:
			metadata[message[0]]['entities'].setdefault(ent,[])
			metadata[message[0]]['entities'][ent].append(ent.label_)

	for i in metadata:
		print(metadata[i]['entities'].keys())
init(conn)