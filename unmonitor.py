import os, requests, logging, sys, config, optparse, operator, xmltodict, urllib3
from logging.handlers import RotatingFileHandler
from plexapi.server import PlexServer
from datetime import datetime, timedelta
import lib.sodarr as sodarr

filename, file_extension = os.path.splitext(os.path.basename(__file__))
formatter = logging.Formatter('%(asctime)s - %(levelname)10s - %(module)15s:%(funcName)30s:%(lineno)5s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
logging.getLogger("requests").setLevel(logging.WARNING)
logger.setLevel(config.LOG_LEVEL)
fileHandler = RotatingFileHandler(config.LOG_FOLDER + '/' + filename + '.log', maxBytes=1024 * 1024 * 1, backupCount=1)
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

error=False

show_user_list = config.unmonitor_shows_users

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sdr = sodarr.API(config.sonarr_host + '/api/', config.sonarr_api)

def clean_series(series_id, series_title, users=[config.plex_server_owner]):
	count=0
	all_episodes = sdr.get_episodes_by_series_id(series_id)

	episodes = [episode for episode in all_episodes if episode['hasFile']]
	episodes = sorted(episodes, key=operator.itemgetter('seasonNumber', 'episodeNumber'))
	logger.debug("# of episodes downloaded: %s", len(episodes))

	monitored_episodes = [episode for episode in all_episodes if episode['monitored']]
	monitored_episodes = sorted(monitored_episodes, key=operator.itemgetter('seasonNumber', 'episodeNumber'))
	logger.debug("# of episodes monitored: %s", len(monitored_episodes))

	last_episode = '1800-01-01'
	for episode in episodes:
		try:
			if (episode['airDate'] >= last_episode) or (episode == episode[0]):
				last_episode = episode['airDate']
		except:
			pass

	files_removed = []
	if len(episodes) > 0:
		for episode in episodes:
			try:
				if episode['hasFile']:
					original_filename = str(episode['episodeFile']['path']).replace("\\\\",'\\')
					filename = remotePathChange(original_filename).replace("\\","/").replace("//","/")

					episode_file_id = episode['episodeFile']['id']
					episode_title = episode['title']
					season_number = episode['seasonNumber']
					episode_number = episode['episodeNumber']
					
					watch = True
					for user in users:
						try:
							plex_episode = get_episode(series_title, season_number, episode_number, user=user)
							watch_indiv = True if get_selected_viewOffset(plex_episode) == -1 else False
						except:
							watch_indiv = False
							
						logger.info("Episode: %s - s%se%s - %s Marked As Watched: %s For User: %s" % (series_title, season_number, episode_number, episode_title, watch_indiv, user))
						if watch_indiv == False: watch = False
					if watch and episode['airDate'] != last_episode:
						count+=1
						episode['monitored'] = False
						sdr.upd_episode(episode)
						logger.info("Removing File from Sonarr - %s" % episode_file_id)
						sdr.rem_episode_file_by_episode_id(episode_file_id)
						get_episode(series_title, season_number, episode_number, True)
						if not os.path.exists(filename): files_removed.append(filename)
					elif watch and episode['airDate'] == last_episode:
						logger.info("Newest Episode that Exists, Not Deleting")
			except Exception as e:
				logger.info("Error Removing Episode - %s" % e)
				
				pass

def remonitor_episodes(series_id, series_title, num_days):
	all_episodes = sdr.get_episodes_by_series_id(series_id)
	for episode in all_episodes:
		monitor = False
		if episode['hasFile']:
			monitor = True
		else:
			try:
				if episode['airDate'] >= (datetime.today() - timedelta(days=num_days)).strftime("%Y-%m-%d"):
					monitor = True
			except KeyError:
				monitor = False

		if episode['monitored'] != monitor:
			episode['monitored'] = monitor
			sdr.upd_episode(episode)

def remotePathChange(original_filename):
	try:
		localPath = config.remote_path_local.split(';')
		remotePath = config.remote_path_remote.split(';')
		count = len(localPath)
		filename = original_filename
		for x in range(0, count):
			if str(remotePath[x]) in original_filename:
				filename = os.path.join(original_filename.replace(remotePath[x], localPath[x]))
				break
		return filename
	except:
		logger.error("Error Changing Remote Path")
		return filename
		
def get_user_tokens(server_id):
	try:
		headers = {'X-Plex-Token':  config.plex_api, 'Accept': 'application/json'}
		api_shared_servers = xmltodict.parse(requests.get('https://plex.tv/api/servers/{server_id}/shared_servers'.format(server_id=server_id), headers=headers, params={}, verify=False).content)
		users = {user['@username']: user['@accessToken'] for user in api_shared_servers['MediaContainer']['SharedServer']}
		return users
	except Exception as e:
		logger.error("Error Getting User Tokens - %s" % e)
		logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
		
		return None
		
def get_episode(series_title, season_number, episode_number, remove=False, user=config.plex_server_owner):
	try:	
		plex = PlexServer(config.plex_host, config.plex_api)
		if user != config.plex_server_owner:
			plex_users = get_user_tokens(plex.machineIdentifier)
			token = plex_users[user]
			plex = PlexServer(config.plex_host, token)
		with  DisableLogger():
			episode = plex.library.section('TV Shows').searchShows(title=series_title)[0].episode(season=season_number, episode=episode_number)
		if remove:
			parts = create_media_lists(episode)
			for media, video in parts:
				if not os.path.isfile(video.file): remove_media_element(media, video, episode)
		return episode
	except Exception as e:
		logger.error("Error Getting Episode - %s" % e)
		logger.error('Error on line {}, {}, {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
		
		return None

def remove_media_element(media, video, movie):
	try:
		logger.info("Removing %s with file: %s from Plex" % (create_plex_title(movie), video.file))
		if not os.path.exists(video.file): media.delete()
	except Exception as e:
		logger.error("Error Removing Elemenet from Plex")
		logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))

def create_plex_title(video):
	if video.type == "movie":
		try:
			title = "%s (%s)" % (video.title, video.originallyAvailableAt.strftime("%Y"))
		except:
			title = video.title
	else:
		title = "%s - %s - %s" % (video.grandparentTitle, video.parentTitle, video.title)
	return title

def get_selected_viewOffset(video):
	if video.viewOffset == 0:
		if video.viewCount > 0:
			selected_viewOffset = -1
		else:
			selected_viewOffset = 0
	else:
		selected_viewOffset = video.viewOffset
	return selected_viewOffset
	
class DisableLogger():
	def __enter__(self):
		logging.disable(logging.CRITICAL)
	def __exit__(self, a, b, c):
		logging.disable(logging.NOTSET)
		
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

if __name__ == "__main__":
	review=""
	try:
		remonitor = [ x[2] for x in show_user_list ]
		users = [ x[1] for x in show_user_list ]
		shows = [ x[0] for x in show_user_list ]
		series = sdr.get_series()
		cleanup_series = {}
		for show in series:
			try:	
				try:
					index = shows.index(show['cleanTitle'])
					user = list(users[index])
					num_days = remonitor[index]
				except ValueError:
					index = -1
				
				if index != -1:
					clean_title = show['cleanTitle']
					logger.info("Checking %s" % clean_title)
					series_id = show['id']
					series_title = show['title']
					if  "remonitor" in user:
						user.remove('remonitor')
						remonitor_episodes(series_id, clean_title, num_days)
					count = clean_series(series_id, series_title, user)
					review+="%s episode removed for %s\n" % (0 if count is None else count, series_title)
			except Exception as e:
				logger.error("Error Unmonitoring Episodes for %s - %s" % (show, e))
				error=True
				
	except Exception as e:
		error=True
		logger.error("Error Running Unmonitor Episodes")
		logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))