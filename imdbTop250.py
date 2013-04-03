#!/bin/env python
# -*- coding: utf-8 -*-

# Set default encoding to UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import getopt
import imdb
import os
import textwrap
import sqlite3
import json
#from imdbTop250data import top250data

_COLOR_WHITE  = ''
_COLOR_LIME   = ''
_COLOR_ORANGE = ''
_COLOR_GRAY   = ''
_COLOR_END    = ''
_UPDATE_CHAR  = '\n'


def usage():
	opts = [
			'--help, -h          -- Show this help',
			'--download, -d      -- Fetch data from IMDb and exit',
			'--db-download URL   -- Download DB from URL',
			'--genre=VAL, -g VAL -- Show only films from this genre',
			'--minyear=VAL       -- Show no films older than VAL',
			'--maxyear=VAL       -- Show no films younger than VAL',
			'--list-genres, -l   -- List all genres and exit',
			'-c                  -- Enforce simple colorization',
			'--cc                -- Enforce more colors'
		]
	print('Usage %s [options] [search]\n\nOPTIONS:\n  %s' % \
			(sys.argv[0], '\n  '.join(opts)) )


def download():
	try:
		os.remove( os.path.expanduser('~/.imdbTop250data.db.new') )
	except OSError:
		pass
	with sqlite3.connect( os.path.expanduser('~/.imdbTop250data.db.new') ) as con:
		cur = con.cursor()    
		cur.execute('''CREATE TABLE movies(
			imdb_id INT NOT NULL,
			color_info STRING, 
			countries STRING, 
			cover_url STRING,
			cover_url_fullsize STRING,
			languages STRING,
			plot_outline STRING,
			plot STRING,
			rank INT,
			rating STRING,
			title STRING,
			url STRING,
			votes INT,
			year INT,
			PRIMARY KEY (imdb_id) )''')
		cur.execute('''CREATE TABLE akas(
			imdb_id INT,
			country STRING,
			type STRING,
			title STRING )''')
		cur.execute('''CREATE TABLE genres(
			imdb_id INT NOT NULL,
			genre STRING,
			PRIMARY KEY (imdb_id, genre))''')
		cur.execute('''CREATE TABLE config(
			key STRING NOT NULL,
			val STRING,
			PRIMARY KEY (key))''')


		ia = imdb.IMDb()
		print( 'Downloading movie data…' )
		for mov in ia.get_top250_movies():
			url = 'http://imdb.com/title/tt%s/' % mov.movieID
			rank = mov['top 250 rank']

			m = None
			tries = 0
			while not m:
				try:
					m = ia.get_movie(mov.movieID)
				except (IOError, imdb.IMDbDataAccessError) as e:
					tries += 1
					if tries > 3:
						raise e
					os.system('sleep 2')
					print 'Failt to download data. trying again…'
			for g in m['genres']:
				cur.execute('''insert into genres 
					(imdb_id, genre) values (?,?) ''',
					(m.movieID, g))
			if not 'akas' in m.keys():
				m['akas'] = []
			for a in m['akas']:
				a = a.split('::')
				if len(a) < 2:
					continue
				for lang in a[1].split(', '):
					lang = lang.split(' (', 1)
					type = lang[1].rstrip(')') if len(lang) > 1 else ''
					cur.execute('''insert into akas
							( imdb_id, country, type, title ) values (?,?,?,?) ''',
							( m.movieID, lang[0], type, a[0] ))
			color_info = json.dumps(m.get('color info'))
			countries  = json.dumps(m.get('countries'))
			languages  = json.dumps(m.get('languages'))
			plot       = json.dumps(m.get('plot'))
			cur.execute('''insert into movies
					( imdb_id, color_info, countries, cover_url, cover_url_fullsize,
					languages, plot_outline, plot, rank, rating, title, url, votes,
					year) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ''',
					( m.movieID, color_info, countries, m.get('cover url'),
						m.get('full-size cover url'), languages,
						m.get('plot outline'), plot, rank, m.get('rating'),
						m.get('title'), url, m.get('votes'), m.get('year') ))
			print 'Finished %3s/250%s' % (rank, _UPDATE_CHAR),
			sys.stdout.flush()
		print ''
		import datetime
		date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		cur.execute('''insert into config (key,val)
				values (?,?) ''', ('date', date) )
		con.commit()
	os.rename( os.path.expanduser('~/.imdbTop250data.db.new'),
			os.path.expanduser('~/.imdbTop250data.db') )


def db_download( url ):
	import urllib2
	try:
		print( 'Downloading DB...' )
		r = urllib2.urlopen( url )
		f = open( os.path.expanduser('~/.imdbTop250data.db.new'), 'wb' )
		f.write( r.read() )
		f.close()
		r.close()
	except (StandardError) as e:
		print( 'Error: %s' % str(e) )
		return

	# Check database
	try:
		print( 'Checking database...' )
		with sqlite3.connect( os.path.expanduser('~/.imdbTop250data.db.new') ) as con:
			cur = con.cursor()    
			cur.execute('''select imdb_id, genre from genres limit 0, 1''')
			cur.fetchone()
			cur.execute('''select imdb_id, color_info, countries, cover_url,
					cover_url_fullsize, languages, plot_outline, plot, rank, rating,
					title, url, votes, year from movies limit 0, 1 ''')
			cur.fetchone()
			cur.execute('''select imdb_id, country, type, title 
					from akas limit 0, 1 ''')
			cur.fetchone()
			cur.execute('''select key, val from config where key = 'date' ''')
			k, v = cur.fetchone()
			print( 'Database OK' )
			print( 'Last DB update: %s' % v )
	except BaseException:
		print( 'Error: Invalid database' )
		os.remove(os.path.expanduser('~/.imdbTop250data.db.new') )
		return
	os.rename( os.path.expanduser('~/.imdbTop250data.db.new'),
			os.path.expanduser('~/.imdbTop250data.db') )


def listGenres():
	with sqlite3.connect( os.path.expanduser('~/.imdbTop250data.db') ) as con:
		cur = con.cursor()    
		cur.execute('''select distinct genre 
				from genres order by genre''')
		for genre, in cur.fetchall():
			print(genre)



def localSearch( search, genrefilter, minyear, maxyear ):
	with sqlite3.connect( os.path.expanduser('~/.imdbTop250data.db') ) as con:
		cur = con.cursor()    

		# Build sql for genrefilter
		join = ''
		filter = []
		filterval = list(genrefilter)
		i = 0
		for g in genrefilter:
			join += '''inner join genres g%(nr)i 
				on m.imdb_id = g%(nr)i.imdb_id ''' % { 'nr' : i }
			filter.append('g%i.genre = ? ' % i)
			i += 1
		if not minyear is None:
			filter.append('year >= ? ')
			filterval.append(minyear)
		if not maxyear is None:
			filter.append('year <= ? ')
			filterval.append(maxyear)
		filtersql = '' if not filter else \
				'%s where %s' % (join, ' and '.join(filter))

		cur.execute('''select m.imdb_id, color_info, countries, cover_url,
			cover_url_fullsize, languages, plot_outline, plot, rank, rating,
			title, url, votes, year from movies m %s''' % filtersql, 
			tuple(filterval))
		for imdb_id, color_info, countries, cover_url, cover_url_fullsize, \
				languages, plot_outline, plot, rank, rating, title, url, votes, \
				year in cur.fetchall():
			cur.execute('''select country, title from akas
				where country in ('Germany', 'UK', 'West Germany')
				and imdb_id = ? ''', (imdb_id,))
			title_aka = []
			country_short = { 'Germany':'GER', 'West Germany':'BRD', 'UK':'UK' }
			for aka_country, aka_title in cur.fetchall():
				title_aka.append( '%3s: “%s”' % (country_short[aka_country], aka_title))
			title_aka = '\n\t        '.join(title_aka)

			if False in [ s.lower() in (title + title_aka).lower() for s in search ]:
				continue

			if title_aka:
				title_aka = '\t%sAkas  :%s %s\n' % ( _COLOR_GRAY, _COLOR_END, title_aka )

			cur.execute('''select genre from genres where imdb_id = ? ''', (imdb_id,))
			genres = []
			for genre, in cur.fetchall():
				genres.append( genre )

			try:
				print( ('%3s: %s%s%s\n%s' \
						+ '\t%sRating:%s %3s (Votes: %6s)\n' \
						+ '\t%sYear  :%s %4s\n' \
						+ '\t%sURL   :%s %s\n' \
						+ '\t%sGenres:%s %s\n' \
						+ '\t%s') % \
						( rank, _COLOR_WHITE, title, _COLOR_END, title_aka, 
							_COLOR_GRAY, _COLOR_END, rating, votes, 
							_COLOR_GRAY, _COLOR_END, year,
							_COLOR_GRAY, _COLOR_END, url,   
							_COLOR_GRAY, _COLOR_END, ', '.join(genres), 
							"\n\t".join(textwrap.wrap('  '+plot_outline, 80)) ))
			except IOError:
				pass



def main(argv):                         
	genres  = set()
	minyear = None
	maxyear = None
	color   = 1 if hasattr(sys.stdout, "isatty") and sys.stdout.isatty() else 0
	action = 'search'
	dburl = None
	try:                                
		opts, args = getopt.getopt(argv, "hg:dlc", 
				["help", "genre=", 'download', 'db-download=', 'minyear=',
					'maxyear=', 'list-genres', 'cc']) 

		for opt, arg in opts:
			if opt in ("-h", "--help"):
				action = 'usage'
				break
			if opt in ('-g', '--genre'):
				genres.add( arg )
			if opt == '--maxyear':
				maxyear = int(arg)
			if opt == '--minyear':
				minyear = int(arg)
			if opt == '-c':
				color = 1
			if opt ==  '--cc':
				color = 2
			if opt == '--db-download':
				dburl = arg
				action = 'db-download'
				break
			if opt in ('-d', '--download'):
				action = 'download'
				break
			if opt in ('-l', '--list-genres'):
				action = 'list-genres'
				break
	except (getopt.GetoptError, ValueError):
		usage()
		sys.exit(2)

	# Set up colors
	if color:
		global _UPDATE_CHAR
		_UPDATE_CHAR  = '\r'
		global _COLOR_WHITE
		global _COLOR_END
		_COLOR_WHITE  = '\033[1m'
		_COLOR_END    = '\033[0m'
		if color > 1:
			global _COLOR_LIME
			global _COLOR_ORANGE
			global _COLOR_GRAY
			_COLOR_LIME   = '\033[32;1m'
			_COLOR_GRAY   = '\033[30;1m'
			_COLOR_ORANGE = '\033[33m'

	try:
		if action == 'search':
			localSearch( args, genres, minyear, maxyear )
		elif action == 'download':
			download()
		elif action == 'db-download':
			db_download( dburl )
		elif action == 'list-genres':
			listGenres()
		elif action == 'usage':
			usage()
	except KeyboardInterrupt:
		pass


if __name__ == "__main__":
	main(sys.argv[1:])
