from plexapi.server import PlexServer
import config

def main(search_string):
	plex = PlexServer(config.plex_host, config.plex_api)
	all = []
	for section in plex.library.sections():
		if section.TYPE in ('movie'):
			all = all + section.search()
		elif section.TYPE in ('show'):
			all = all + section.searchEpisodes(duplicate=True)
	for item in all:
		parts = create_media_lists(item)
		for media, video in parts:
			filename = video.file
#			print(filename)
			if search_string in filename:
				print((filename, item.title))

def create_media_lists(movie):
	try:
		patched_items = []
		for zomg in movie.media:
			zomg._initpath = movie.key
			patched_items.append(zomg)

		zipped = zip(patched_items, movie.iterParts())
		parts = sorted(zipped, key=lambda i: i[1].size if i[1].size else 0, reverse=True)
		return parts
	except Exception as e:
		logger.error("Error Getting Video Parts from Plex")
		logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
		return None
		
main("Irishman")