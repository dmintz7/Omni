import warnings
import json

import pymysql
from pymysql import OperationalError
from pymysql.converters import escape_string

from flask import Flask, request, make_response
from flask_apscheduler import APScheduler

import plexapi.exceptions
import config
import random
import time
import sodarr
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from plexapi.server import PlexServer

formatter = logging.Formatter('%(asctime)s - %(levelname)10s - %(module)15s:%(funcName)30s:%(lineno)5s - %(message)s')
logger = logging.getLogger()
logger.setLevel("INFO")


consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(config.log_level.upper())
logger.addHandler(consoleHandler)

debugHandler = TimedRotatingFileHandler(os.path.join(config.log_folder, 'debug.log'), when='midnight', backupCount=20)
debugHandler.setFormatter(formatter)
debugHandler.setLevel("DEBUG")
logger.addHandler(debugHandler)

fileHandler = TimedRotatingFileHandler(os.path.join(config.log_folder, 'info.log'), when='midnight', backupCount=20)
fileHandler.setFormatter(formatter)
fileHandler.setLevel("INFO")
logger.addHandler(fileHandler)

errorHandler = TimedRotatingFileHandler(os.path.join(config.log_folder, 'error.log'), when='midnight', backupCount=20)
errorHandler.setFormatter(formatter)
errorHandler.setLevel("ERROR")
logger.addHandler(errorHandler)


def modify_new():
	from_profile = sdr.get_profile_id(config.sonarr_from_profile)
	to_profile = sdr.get_profile_id(config.sonarr_to_profile)

	update_total = 0
	series = sdr.get_series()
	for show in series:
		try:
			if show['profileId'] == from_profile:
				update_total += 1
				logger.info("New Show (%s) Found for Omni, Making Initial Changes" % show['title'])
				show['tags'] = [config.tag_id]
				show['qualityProfileId'] = to_profile
				show['profileId'] = to_profile
				show['monitored'] = False
				for x in show['seasons']:
					x['monitored'] = False
				sdr.upd_series(show)

				add_show(show_id=show['id'])
				refresh_database()
				plex_id = add_plex(show_id=show['id'])
				if plex_id is not None:
					get_watched(plex_id=plex_id)
				update_monitored(show_id=show['id'])
				update_show(show_id=show['id'])
		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
	if not update_total:
		logger.info("No New Shows")


def update_show(show_id=None, plex_id=None):
	updates = execute_sql("select", select={"id", "plex", "title", "last_monitor_season", "last_monitor_episode", "last_watch_season", "last_watch_episode"}, table={"monitor_episodes"})
	for update in random.sample(updates, len(updates)):
		if show_id is not None:
			try:
				if int(update['id']) != int(show_id):
					continue
			except TypeError:
				continue
		if plex_id is not None:
			try:
				if int(update['plex']) != int(plex_id):
					continue
			except TypeError:
				continue

		logger.info("Updating %s" % update['title'])
		monitor_season = update['last_monitor_season']
		monitor_episode = update['last_monitor_episode']
		last_season = update['last_watch_season']
		last_episode = update['last_watch_episode']
		max_season = last_season
		num_changed = 0
		season_count = 0

		series = sdr.get_series_by_series_id(update['id'])
		remaining_episodes = series['totalEpisodeCount'] - (series['seasons'][0]['statistics']['totalEpisodeCount'] if series['seasons'][0]['seasonNumber'] == 0 else 0) - series['episodeCount']
		if remaining_episodes == 0:
			logger.info("Monitoring All Episodes for %s" % series['title'])
			continue

		for season in series['seasons']:
			if season['seasonNumber'] == max_season:
				season_count = season['statistics']['totalEpisodeCount']

		logger.info("Calculating New Stats for Show")
		if (last_episode < int(config.watch_season_episodes)) and (season_count > int(config.max_season_episodes)):
			max_episode = int(config.min_season_episodes)
		else:
			max_episode = season_count

		if ((last_episode / season_count) >= float(config.season_percent_complete)) or (last_episode > int(config.watch_season_episodes) and season_count < int(config.max_season_episodes)):
			max_season += 1
			max_episode = int(config.min_season_episodes)

		if not (max_season == monitor_season and max_episode == monitor_episode):
			num_changed += int(mark_episodes(series, max_season, max_episode))
			logger.info("%s - %s Episodes Updated - Monitor to S%sE%s - Last Watched S%sE%s" % (series['title'], num_changed, max_season, max_episode, last_season, last_episode))
			if num_changed > 0:
				try:
					sdr.command({'name': 'SeriesSearch', 'seriesId': series['id']})
					logger.info("%s Episodes for %s changed to monitor, sending search command" % (num_changed, series['title']))
					execute_sql("update", table={"shows"}, set={"last_monitor_season": max_season, "last_monitor_episode": max_episode}, where={"id": series['id']})
					update_monitored(show_id=series['id'])
				except Exception as e:
					logger.error('Error! Line: {l}, Code: {c}, Message, {m}'.format(l=sys.exc_info()[-1].tb_lineno, c=type(e).__name__, m=str(e)))
			else:
				logger.debug("No Updates Needed")
		else:
			logger.debug("No Updates Needed")


def mark_episodes(series, max_season, max_episode):
	count = 0
	logger.info("Marking Episodes up to S%sE%s" % (max_season, max_episode))

	logger.debug("Marking Seasons")
	series['monitored'] = True
	for x in series['seasons']:
		monitor = False
		if max_season >= x['seasonNumber'] > 0:
			monitor = True

		if x['monitored'] != monitor:
			count += x['statistics']['totalEpisodeCount']
		x['monitored'] = monitor
	sdr.upd_series(series)
	
	logger.debug("Marking Episodes")
	all_episodes = sorted(sdr.get_episodes_by_series_id(series['id']), key=lambda i: (i['seasonNumber'], i['episodeNumber']))
	for episode in all_episodes:
		try:
			monitor = False
			if 0 < episode['seasonNumber'] <= max_season:
				if episode['seasonNumber'] == max_season and episode['episodeNumber'] <= max_episode:
					monitor = True
				elif episode['seasonNumber'] != max_season:
					monitor = True
			if episode['monitored'] != monitor:
				count += 1
				episode['monitored'] = monitor
				sdr.upd_episode(episode)
				time.sleep(1)
		except Exception as e:
			logger.error('Error! Line: {l}, Code: {c}, Message, {m}'.format(l=sys.exc_info()[-1].tb_lineno, c=type(e).__name__, m=str(e)))
	return count


def get_watched(plex_id=None, user_id=None):
	logger.info("Getting Last Watched for User: %s and Show: %s" % ("All" if user_id is None else user_id, "All" if plex_id is None else plex_id))
	for user in users:
		if user_id is not None:
			try:
				if int(user['id']) != int(user_id):
					continue
			except TypeError:
				continue

		logger.debug("Checking User: %s" % user['username'])
		plex_api_user = PlexServer(config.plex_host, user['token'])
		plex_shows = plex_api_user.library.section(config.plex_library).searchShows()

		for show in shows:
			if show['plex'] is None:
				continue

			if plex_id is not None:
				try:
					if int(show['plex']) != int(plex_id):
						continue
				except TypeError:
					continue
			logger.debug("Checking Show: %s" % show['title'])
			plex_series = list(filter(lambda x: (int(x.ratingKey) == int(show['plex'])), plex_shows))[0]
			last_season = 1
			last_episode = 0
			for episode in plex_series.episodes():
				if episode.isWatched:
					last_season = episode.seasonNumber
					last_episode = episode.index
				else:
					try:
						if config.must_watch_previous:
							break
					except AttributeError:
						pass

			logger.info("Updating Show: %s to S%sE%s for User: %s" % (show['title'], last_season, last_episode, user['username']))
			logger.debug(plex_series)
			execute_sql("insert", table={"usersWatch"}, values={"showId": show['id'], "userId": user['id'], "last_watch_season": last_season, "last_watch_episode": last_episode}, on_duplicate={"last_watch_season": last_season, "last_watch_episode": last_episode})


def update_monitored(show_id=None):
	logger.info("Updating Monitor Season and Episode from Sonarr for %s" % (show_id if show_id else "All Shows"))
	series = sdr.get_series()
	for show in series:
		if show_id is not None:
			try:
				if int(show['id']) != int(show_id):
					continue
			except TypeError:
				continue
		try:
			if config.tag_id in show['tags']:
				if int(show['id']) not in [x['id'] for x in shows]:
					add_show(show['id'])
				max_season = 0
				max_episode = 0
				all_episodes = sorted(sdr.get_episodes_by_series_id(show['id']), key=lambda i: (i['seasonNumber'], i['episodeNumber']))
				for episode in all_episodes:
					if episode['monitored'] and episode['seasonNumber'] > 0:
						max_season = episode['seasonNumber']
						max_episode = episode['episodeNumber']

				execute_sql("update", table={"shows"}, set={"last_monitor_season": max_season, "last_monitor_episode": max_episode}, where={"id": show['id']})

		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


def add_plex(show_id=None):
	logger.info("Adding Plex IDs for %s" % (show_id if show_id else "All Shows"))
	for show in shows:
		if show_id is not None:
			try:
				if int(show['id']) != int(show_id):
					continue
			except TypeError:
				continue
		try:
			plex_id = show['plex']
			if show['plex'] is None:
				logger.debug(show)
				try:
					plex_id = plex_api.library.section(config.plex_library).searchShows(title=show['title'])[0].ratingKey
					logger.debug("Adding Plex ID for  %s" % show['title'])
					execute_sql("update", table={"shows"}, set={"plex": plex_id}, where={"id": show['id']})
				except IndexError:
					logger.warning("Show: %s Not Found in Plex" % show['title'])

			if show_id is not None:
				return plex_id
		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


def add_show(show_id=None):
	logger.info("Adding Tagged Show(s) to Database for %s" % (show_id if show_id else "All Shows"))
	series = sdr.get_series()
	for show in series:
		if show_id is not None:
			try:
				if int(show['id']) != int(show_id):
					continue
			except TypeError:
				continue
		try:
			if show['id'] in [x['id'] for x in shows]:
				continue
			if config.tag_id in show['tags']:
				logger.info("Added Show: %s" % show['title'])
				execute_sql("insert", table={"shows"}, values={"id": show['id'], "title": show['title'], "year": show['year'], "status": show['status'], "tvdb": show['tvdbId']}, returnValue="id")

		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


def session_search():
	if plex_api is None:
		refresh_database()
	try:
		for x in plex_api.sessions():
			logger.info("User: %s is watching %s" % (x.usernames[0], create_plex_title(x)))

			plex_id = x.grandparentRatingKey
			user_id = [user['id'] for user in users if user['username'] == x.usernames[0]][0]
			watch_season = int(x.parentIndex)
			watch_episode = int(x.index)
			user_history = execute_sql("select", select={"show_id", "show_title", "plex", "user_id", "username", "last_watch_season", "last_watch_episode"}, table={"last_watched"}, where={"plex": plex_id, "user_id": user_id})
			logger.info(user_history)
			try:
				logger.info((watch_season, int(user_history[0]['last_watch_season']), watch_episode, int(user_history[0]['last_watch_episode'])))
				if watch_season > int(user_history[0]['last_watch_season']) or (watch_season == int(user_history[0]['last_watch_season']) and watch_episode > int(user_history[0]['last_watch_episode'])):
					execute_sql("update", table={"usersWatch"}, set={"last_watch_season": watch_season, "last_watch_episode": watch_episode}, where={"showId": user_history[0]['show_id'], "userId": user_id})
					logger.info("Updated")
					update_show(plex_id=plex_id)
			except IndexError:
				logger.debug("Not a Tagged Show")
			except Exception as e:
				logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))

		if not len(plex_api.sessions()):
			logger.info("Nothing is Being Watched")
	except Exception as e:
		logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))


def recently_watched():
	logger.info("Updating Recently Watched")
	try:
		if plex_api is None:
			refresh_database()
	except NameError:
		refresh_database()

	watched_shows = []
	try:
		for user in users:
			try:
				plex_api_user = PlexServer(config.plex_host, user['token'])
				history = plex_api_user.library.section(config.plex_library).history(maxresults=config.max_results)
				for x in list(set([x.grandparentRatingKey for x in history])):
					if str(x) not in [str(show['plex']) for show in shows]:
						continue
					get_watched(plex_id=x, user_id=user['id'])
					show_id = list(filter(lambda show: (str(x) == str(show['plex'])), shows))[0]['id']
					watched_shows.append(show_id)
			except plexapi.exceptions.Unauthorized:
				pass

		for show_id in list(set(watched_shows)):
			update_monitored(show_id=show_id)
			update_show(show_id=show_id)
	except Exception as e:
		logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


def create_plex_title_json(video):
	if video['librarySectionType'] == "movie":
		try:
			title = "%s (%s)" % (video['title'], video['originallyAvailableAt'])
		except Exception as e:
			logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
			title = video['title']
	else:
		title = "%s - S%sE%s - %s" % (video['grandparentTitle'], video['parentIndex'], video['index'], video['title'])
	return title


def create_plex_title(video):
	if video.type == "movie":
		try:
			title = "%s (%s)" % (video.title, video.originallyAvailableAt.strftime("%Y"))
		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
			title = video.title
	else:
		title = "%s - %s - %s" % (video.grandparentTitle, video.parentTitle, video.title)
	return title


def plex_users():
	if plex_api is None:
		refresh_database()

	try:
		logger.info("Updating Users from Plex")
		execute_sql("insert", table={"users"}, values={"id": plex_api.myPlexAccount().id, "username": plex_api.myPlexAccount().username, "token": config.plex_api}, on_duplicate={"token": config.plex_api})
		account = plex_api.myPlexAccount()
		for user in account.users():
			users_token = account.user(user.username).get_token(plex_api.machineIdentifier)
			execute_sql("insert", table={"users"}, values={"id": user.id, "username": user.username, "token": users_token}, on_duplicate={"token": users_token})
	except Exception as e:
		logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


def execute_sql(query_type, **kwargs):
	query = None
	return_value = None
	try:
		if query_type not in ('select', 'update', 'insert', 'delete', 'replace'):
			return "query type is not valid"
		elif query_type != 'select' and type(kwargs['table']) == set and len(kwargs['table']) > 1:
			return "only select type can contain multiple tables"
		elif query_type == "select":
			select_formatted = ", ".join(kwargs['select'])
			table_formatted = ", ".join(kwargs['table'])
			join_formatted = " and ".join(["%s=%s" % (k, kwargs['join'][k]) for k in kwargs['join']]) if 'join' in kwargs else None
			query = "SELECT %s FROM %s" % (select_formatted, table_formatted)
			if 'where' in kwargs:
				where_formatted = " and ".join(["%s='%s'" % (k, kwargs['where'][k]) for k in kwargs['where']])
				query = "%s WHERE %s" % (query, where_formatted)
				if join_formatted:
					query = "%s AND %s" % (query, join_formatted)
		elif query_type == "update":
			table_formatted = "".join(kwargs['table'])
			set_formatted = ", ".join(["%s='%s'" % (k, kwargs['set'][k]) for k in kwargs['set']])
			where_formatted = " and ".join(["%s='%s'" % (k, kwargs['where'][k]) for k in kwargs['where']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "UPDATE %s SET %s WHERE %s" % (table_formatted, set_formatted, where_formatted)
		elif query_type == "delete":
			table_formatted = "".join(kwargs['table'])
			where_formatted = " and ".join(["%s='%s'" % (k, escape_string(kwargs['where'][k])) for k in kwargs['where']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "DELETE FROM %s WHERE %s" % (table_formatted, where_formatted)
		elif query_type == "insert":
			table_formatted = "".join(kwargs['table'])
			columns = ",".join([str(k) for k in kwargs['values']])
			values = ",".join(["'" + escape_string(str(kwargs['values'][k])) + "'" for k in kwargs['values']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(`id`)" % (table_formatted, columns, values)
			query += ";" if 'on_duplicate' not in kwargs else ",%s" % ", ".join(["%s='%s'" % (k, kwargs['on_duplicate'][k]) for k in kwargs['on_duplicate']])
		elif query_type == "replace":
			table_formatted = "".join(kwargs['table'])
			columns = ",".join([str(k) for k in kwargs['values']])
			values = ",".join(["'" + escape_string(str(kwargs['values'][k])) + "'" for k in kwargs['values']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "REPLACE INTO %s (%s) VALUES (%s)" % (table_formatted, columns, values)
			query += ";"

		if query is not None:
			query = query.replace("'None'", "Null").replace("None", "Null")
			logger.debug(query)
			with warnings.catch_warnings():
				warnings.simplefilter("ignore")
				conn = pymysql.connect(host=config.host, port=config.port, user=config.user, password=config.passwd, database=config.dbname, cursorclass=pymysql.cursors.DictCursor)
				cursor = conn.cursor()
				cursor.execute(query)
				if query_type == "select":
					result = cursor.fetchall()
				else:
					conn.commit()
					if return_value == "id":
						result = cursor.lastrowid
					elif return_value == "count":
						result = cursor.rowcount
				cursor.close()
				conn.close()
			return result
		else:
			logger.error(query)
			return None
	except OperationalError:
		pass
	except Exception as e:
		logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
		logger.error(query)
		return None


def refresh_database():
	logger.info("Refreshing Database")

	global users
	global shows
	global plex_api
	global sdr

	os.chmod(config.log_folder, 0o777)

	try:
		plex_api = PlexServer(config.plex_host, config.plex_api)
	except Exception as e:
		logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
		plex_api = None
		logger.error("Can't Connect to Plex")

	try:
		sdr = sodarr.API(config.sonarr_host + '/api', config.sonarr_api)
	except Exception as e:
		logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
		sdr = None
		logger.error("Can't Connect to Sonarr")

	users = execute_sql("select", select={"id", "username", "token"}, table={"users"})
	shows = execute_sql("select", select={"id", "title", "tvdb", "plex"}, table={"shows"})


def run_functions():
	logger.info("Running Hourly Functions")
	add_show()
	add_plex()


def full_check():
	logger.info("Running Full Check")
	plex_users()
	add_show()
	add_plex()
	update_monitored()
	get_watched()
	update_show()


app = Flask(__name__)


@app.route('/plex', methods=['POST'])
def plex_webhook():
	content = json.loads(request.form["payload"])
	event = content['event']
	logger.info("Received Plex Webhook - %s" % event)
	item = content['Metadata']
	if item['librarySectionTitle'] == config.plex_library:
		if event == "media.scrobble":
			user_id = [user['id'] for user in users if user['username'] == content['Account']['title']][0]
			plex_id = item['grandparentRatingKey']

			watch_season = int(item['parentIndex'])
			watch_episode = int(item['index'])
			user_history = execute_sql("select", select={"show_id", "show_title", "plex", "user_id", "username", "last_watch_season", "last_watch_episode"}, table={"last_watched"}, where={"plex": plex_id, "user_id": user_id})
			try:
				if watch_season > int(user_history[0]['last_watch_season']) or (watch_season == int(user_history[0]['last_watch_season']) and watch_episode > int(user_history[0]['last_watch_episode'])):
					get_watched(plex_id=plex_id, user_id=user_id)
					update_show(plex_id=plex_id)
			except IndexError:
				logger.debug("Not a Tagged Show")
			except Exception as e:
				logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
		elif event == "library.new":
			plex_id = item['ratingKey']
			show_title = item['title']
			execute_sql("update", table={"shows"}, set={"plex": plex_id}, where={"title": show_title, "plex": None})
			refresh_database()
			update_show(plex_id=plex_id)
	else:
		logger.debug("Not a TV Show")



	return make_response("", 200)


class Config(object):
	JOBS = [
		{'id': 'refresh_database', 'func': refresh_database, 'trigger': 'interval', 'hours': 1},
		{'id': 'run_functions', 'func': run_functions, 'trigger': 'interval', 'hours': 1},
		{'id': 'modify_new', 'func': modify_new, 'trigger': 'interval', 'minutes': 2},
		{'id': 'recently_watched', 'func': recently_watched, 'trigger': 'interval', 'minutes': 10},
		{'id': 'full_check', 'func': full_check, 'trigger': 'interval', 'hours': 12},
	]


# try:
# 	plex_api = PlexServer(config.plex_host, config.plex_api)
# except Exception as e:
# 	logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
# 	plex_api = None
# 	logger.error("Can't Connect to Plex")
#
# try:
# 	sdr = sodarr.API(config.sonarr_host + '/api', config.sonarr_api)
# except Exception as e:
# 	logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
# 	sdr = None
# 	logger.error("Can't Connect to Sonarr")
#
# users = execute_sql("select", select={"id", "username", "token"}, table={"users"})
# shows = execute_sql("select", select={"id", "title", "tvdb", "plex"}, table={"shows"})

refresh_database()
app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
