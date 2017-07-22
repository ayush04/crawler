import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sqlite3, re
import certifi

# Create a list of words to ignore
ignoreWords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])

class crawler:
	# Initialize the crawler with db name
	def __init__(self, dbname):
		self.connection = sqlite3.connect(dbname)
	
	def __del__(self):
		self.connection.close()

	def dbCommit(self):
		self.connection.commit()

	# Auxilliary function for getting an entry id and adding it if its not present
	def getEntryId(self, table, field, value, createNew=True):
		cursor = self.connection.execute("select rowid from %s where %s='%s'" % (table, field, value))
		result = cursor.fetchone()
		if(result == None):
			cursor = self.connection.execute("insert into %s(%s) values ('%s')" % (table, field, value))
			return cursor.lastrowid
		else:
			return result[0]
	
	# Index an individual page
	def addToIndex(self, url, soup):
		if(self.isIndexed(url)):
			return
		print('Indexing %s ' %url)

		# Get Individual words
		text = self.getTextOnly(soup)
		words = self.separateWords(text)

		# Get URL id
		urlId = self.getEntryId('urllist', 'url', url)

		# Link each word to this URL
		for i in range(len(words)):
			word = words[i]
			if(word in ignoreWords):
				continue
			wordId = self.getEntryId('wordlist', 'word', word)
			self.connection.execute('insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)' % (urlId, wordId, i))

	# Extract text from an HTML page
	def getTextOnly(self, soup):
		str = soup.string
		if(str==None):
			contents = soup.contents
			resultText = ''
			for content in contents:
				subText = self.getTextOnly(content)
				resultText += subText + '\n'
			return resultText
		else:
			return str.strip()

	# Separate words by any non-whitespace character
	def separateWords(self, text):
		splitter = re.compile('\\W*')
		return [s.lower() for s in splitter.split(text) if s!='']

	# Return true if this url is already indexed
	def isIndexed(self, url):
		result = self.connection.execute("select rowid from urllist where url = '%s'" % url).fetchone()

		if(result != None):
			# Check if URL is actually been crawled
			result1 = self.connection.execute("select * from wordlocation where urlid=%d" % result[0]).fetchone()
			if(result1 != None):
				return True
		return False

	# Add a link between two pages
	def addLinkRef(self, urlFrom, urlTo, linkText):
		words = self.separateWords(linkText)
		fromId = self.getEntryId('urllist','url',urlFrom)
		toId = self.getEntryId('urllist','url',urlTo)
		if fromId == toId: 
			return
		
		cursor = self.connection.execute("insert into link(fromid,toid) values (%d,%d)" % (fromId, toId))
		linkId = cursor.lastrowid

		for word in words:
			if word in ignoreWords: continue
			wordId = self.getEntryId('wordlist','word', word)
			self.connection.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkId, wordId))

	# Starting with a list of pages, do a BFS to a given depth, indexing pages as we go
	def crawl(self, pages, depth=2):
		for i in range(depth):
			newPages = set()
			for page in pages:
				try:
					http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
					response = http.request('GET', page)
				except:
					print('Could not open %s' % page)
					continue
				soup = BeautifulSoup(response.data)
				self.addToIndex(page, soup)

				links = soup('a')
				for link in links:
					if('href' in dict(link.attrs)):
						url = urljoin(page, link['href'])
						if(url.find("'") != -1):
							continue
						url = url.split('#')[0]

						if(url[0:4] == 'http' and not self.isIndexed(url)):
							newPages.add(url)
						linkText = self.getTextOnly(link)
						self.addLinkRef(page, url, linkText)
					self.dbCommit()
				
				pages = newPages

	# Create db tables
	def createIndexTables(self):
		self.connection.execute('create table urllist(url)')
		self.connection.execute('create table wordlist(word)')
		self.connection.execute('create table wordlocation(urlid, wordid, location)')
		self.connection.execute('create table link(fromid integer, toid integer)')
		self.connection.execute('create table linkwords(wordid, linkid)')
		self.connection.execute('create index wordidx on wordlist(word)')
		self.connection.execute('create index urlidx on urllist(url)')
		self.connection.execute('create index wordurlidx on wordlocation(wordid)')
		self.connection.execute('create index urltoidx on link(toid)')
		self.connection.execute('create index urlfromidx on link(fromid)')

	# Calculate PageRank
	def calculatePageRank(self, iterations=20):
		# Clear out current PageRank tables
		self.connection.execute('drop table if exists pagerank')
		self.connection.execute('create table pagerank(urlid primary key, score)')

		# Initialize every URL with a PageRank of 1
		self.connection.execute('insert into pagerank select rowid, 1.0 from urllist')
		self.dbCommit()

		for i in range(iterations):
			print('Iteration %d' % i)
			for (urlId,) in self.connection.execute('select rowid from urllist'):
				pr = 0.15

				# Loop through all the pages that link to this one
				for (linker,) in self.connection.execute('select distinct fromid from link where toid=%d' % urlId):
					# Get PageRank of linker
					linkingpr = self.connection.execute('select score from pagerank where urlid=%d' % linker).fetchone()[0]

					# Get the total number of links from the linker
					linkingCount = self.connection.execute('select count(*) from link where fromid=%d' % linker).fetchone()[0]
					pr += 0.85*(linkingpr/linkingCount)
				
				self.connection.execute('update pagerank set score=%f where urlid=%d' % (pr, urlId))
			self.dbCommit()

	
class searcher:
	def __init__(self, dbname):
		self.connection = sqlite3.connect(dbname)

	def __del__(self):
		self.connection.close()

	def getMatchRows(self, query):
		# Strings to build the query
		fieldList = 'w0.urlid'
		tableList = ''
		clauseList = ''
		wordIds = []

		# Split the words by spaces
		words = query.split(' ')
		tableNumber = 0

		for word in words:
			# Get the word ID
			wordRow = self.connection.execute("select rowid from wordlist where word = '%s'" % word).fetchone()
			if(wordRow != None):
				wordId = wordRow[0]
				wordIds.append(wordId)
				if(tableNumber > 0):
					tableList += ','
					clauseList += ' and '
					clauseList += 'w%d.urlid=w%d.urlId and ' % (tableNumber-1, tableNumber)
				
				fieldList += ',w%d.location' % tableNumber
				tableList += 'wordLocation w%d' % tableNumber
				clauseList += 'w%d.wordId = %d' % (tableNumber, wordId)
				tableNumber += 1

		# Create the query from the separate parts
		fullQuery = 'select %s from %s where %s' % (fieldList, tableList, clauseList)
		print(fullQuery)

		cursor = self.connection.execute(fullQuery)
		rows = [row for row in cursor]

		return rows, wordIds

	def getScoredList(self, rows, wordIds):
		totalScores = dict([(row[0], 0) for row in rows])

		# weights = [(1.0, self.frequencyScore(rows))]
		# weights = [(1.5, self.locationScore(rows)), (1.0, self.frequencyScore(rows)), (1.5, self.distanceScore(rows)), (2.0, self.pageRankScore(rows))]
		weights = [(2.0, self.pageRankScore(rows))]

		for(weight, scores) in weights:
			for url in totalScores:
				totalScores[url] += weight*scores[url]

		return totalScores

	def getUrlName(self, id):
		return self.connection.execute('select url from urllist where rowid=%d' % id).fetchone()[0]

	def query(self, q):
		rows, wordIds = self.getMatchRows(q)
		scores = self.getScoredList(rows, wordIds)
		rankedScores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)

		for(score, urlId) in rankedScores[0:10]:
			print('%f\t%s' % (score, self.getUrlName(urlId)))

	def normalizeScore(self, scores, smallIsBetter=0):
		vsmall = 0.00001 # To avoid division by 0 errors
		if smallIsBetter:
			minScore = min(scores.values())
			return dict([(u, float(minScore)/max(vsmall, l)) for u, l in scores.items()])
		else:
			maxScore = max(scores.values())
			if maxScore == 0:
				maxScore = vsmall
			return dict([(u, float(c)/maxScore) for u, c in scores.items()])

	# Word frequency
	def frequencyScore(self, rows):
		counts = dict([(row[0], 0) for row in rows])
		for row in rows:
			counts[row[0]] += 1
		return self.normalizeScore(counts)

	# Document location
	def locationScore(self, rows):
		locations = dict([(row[0], 1000000) for row in rows])
		for row in rows:
			location = sum(row[1:])
			if location < locations[row[0]]:
				locations[row[0]] = location
		return self.normalizeScore(locations, smallIsBetter=1)

	# Word Distance
	def distanceScore(self, rows):
		# If there is one word
		if len(rows[0])<=2:
			return dict([(row[0], 1.0) for row in rows])

		# Initialize the dictionary with large values
		minDistance = dict([(row[0], 1000000) for row in rows])

		for row in rows:
			distance = sum([abs(row[i] - row[i-1]) for i in range(2, len(row))])
			if distance < minDistance[row[0]]:
				minDistance[row[0]] = distance
		return self.normalizeScore(minDistance, smallIsBetter=1)

	# PageRank score
	def pageRankScore(self, rows):
		pageRanks = dict([(row[0], self.connection.execute('select score from pagerank where urlid=%d' % row[0]).fetchone()[0]) for row in rows])
		maxRank = max(pageRanks.values())

		normalizedScores = dict([(u, float(l)/maxRank) for (u, l) in pageRanks.items()])
		return normalizedScores
