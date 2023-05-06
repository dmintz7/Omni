import logging
import os
import random
import sys
import time
import warnings

import plexapi.exceptions
import pymysql
from plexapi.server import PlexServer
from pymysql import OperationalError
from pymysql.converters import escape_string

import config
import sodarr

logger = logging.getLogger('root')

global users
global shows
global plex_api
global sdr


def modify_new():
	from_profile = sdr.get_profile_id(config.sonarr_from_profile)
	to_profile = sdr.get_profile_id(config.sonarr_to_profile)

	update_total = 0
	series = sdr.get_series()
	for show in series:
		try:
			if show['qualityProfileId'] == from_profile:
				update_total += 1
				logger.info("New Show (%s) Found for Omni, Making Initial Changes" % show['title'])
				show['tags'] += [config.tag_id]
				show['qualityProfileId'] = to_profile
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
		try:
			if series['title'] == 'Not Found':
				logger.error("Series (%s) Not Found in Sonarr" % update['title'])
				continue
		except KeyError:
			pass

		try:
			specials_episode_count = series['seasons'][0]['statistics']['totalEpisodeCount'] if series['seasons'][0]['seasonNumber'] == 0 else 0
		except KeyError:
			specials_episode_count = 0

		try:
			remaining_episodes = series['statistics']['totalEpisodeCount'] - specials_episode_count - series['statistics']['episodeCount']
		except KeyError:
			remaining_episodes = 0

		try:
			if remaining_episodes == 0 and series['monitored']:
				logger.info("Monitoring All Episodes for %s" % series['title'])
				mark_episodes(series, 9999, 9999, True)
				continue
		except KeyError:
			logger.error(series)
			raise

		try:
			for season in series['seasons']:
				if season['seasonNumber'] == max_season:
					season_count = season['statistics']['totalEpisodeCount']
		except KeyError:
			logger.warning("No Seasons Found")
			return

		logger.info("Calculating New Stats for Show")
		if (last_episode < int(config.watch_season_episodes)) and (season_count > int(config.max_season_episodes)):
			max_episode = int(config.min_season_episodes)
		else:
			max_episode = season_count

		try:
			if ((last_episode / season_count) >= float(config.season_percent_complete)) or (last_episode > int(config.watch_season_episodes) and season_count < int(config.max_season_episodes)):
				max_season += 1
				max_episode = int(config.min_season_episodes)

			if not (max_season == monitor_season and max_episode == monitor_episode):
				num_changed += int(mark_episodes(series, max_season, max_episode))
				logger.info("%s - %s Episodes Updated - Monitor to S%sE%s - Last Watched S%sE%s" % (series['title'], num_changed, max_season, max_episode, last_season, last_episode))
				if num_changed > 0:
					try:
						sdr.command({'name': 'SeriesSearch', 'seriesId': series['id']})
						logger.info("%s Episodes for %s changed, sending search command" % (num_changed, series['title']))
						execute_sql("update", table={"shows"}, set={"last_monitor_season": max_season, "last_monitor_episode": max_episode}, where={"id": series['id']})
						update_monitored(show_id=series['id'])
					except Exception as e:
						logger.error('Error! Line: {l}, Code: {c}, Message, {m}'.format(l=sys.exc_info()[-1].tb_lineno, c=type(e).__name__, m=str(e)))
				else:
					logger.info("No Updates Needed")
			else:
				logger.info("No Updates Needed")
		except Exception as e:
			logger.error('Error! Line: {l}, Code: {c}, Message, {m}'.format(l=sys.exc_info()[-1].tb_lineno, c=type(e).__name__, m=str(e)))


def mark_episodes(series, max_season, max_episode, all_episodes=False):
	count = 0
	if not all_episodes:
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
	logger.debug(sdr.upd_series(series))

	logger.debug("Marking Episodes")
	monitor_episodes_mark = []
	monitor_episodes_unmark = []
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
				if monitor:
					monitor_episodes_mark.append(episode['id'])
				else:
					monitor_episodes_unmark.append(episode['id'])
		except KeyError:
			break
		except Exception as e:
			logger.error('Error! Line: {l}, Code: {c}, Message, {m}'.format(l=sys.exc_info()[-1].tb_lineno, c=type(e).__name__, m=str(e)))

	if monitor_episodes_mark:
		logger.debug(sdr.monitor_episodes(monitor_episodes_mark, True))
	if monitor_episodes_unmark:
		logger.debug(sdr.monitor_episodes(monitor_episodes_unmark, False))
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

		try:
			logger.debug("Checking User: %s" % user['username'])
			plex_api_user = PlexServer(config.plex_host, user['token'])
			plex_shows = plex_api_user.library.section(config.plex_library).searchShows()
		except plexapi.exceptions.Unauthorized:
			logger.error("User (%s) Not Authorized. Verify User Exists and Token is Correct" % user['username'])
			continue

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
			try:
				plex_series = list(filter(lambda x: (int(x.ratingKey) == int(show['plex'])), plex_shows))[0]
				logger.debug("Matched to %s" % plex_series)
			except IndexError:
				logger.error("Show: %s Not Found in Plex" % show['title'])
				execute_sql("update", table={"shows"}, set={"plex": None}, where={"id": show['id']})
				continue

			last_season = 1
			last_episode = 0
			for episode in plex_series.episodes():
				if episode.isPlayed:
					last_season = episode.seasonNumber
					last_episode = episode.index
				else:
					try:
						# noinspection PyUnresolvedReferences
						if config.must_watch_previous:
							break
					except AttributeError:
						pass

			logger.debug("Updating Show: %s to S%sE%s for User: %s" % (show['title'], last_season, last_episode, user['username']))
			execute_sql("insert", table={"usersWatch"}, values={"showId": show['id'], "userId": user['id'], "last_watch_season": last_season, "last_watch_episode": last_episode}, on_duplicate={"last_watch_season": last_season, "last_watch_episode": last_episode})


def update_status():
	current_sonarr = sdr.get_series()
	for show in shows:
		# if show['status'] == 'ended':
		# 	logger.debug("Id: %s Show: %s, show has ended %s" % (show['id'], series['title'], show['status']))
		# 	continue

		series = list(filter(lambda x: x['id'] == show['id'], current_sonarr))[0]
		if series['status'] == show['status']:
			logger.debug("Id: %s Show: %s, status already %s" % (show['id'], series['title'], show['status']))
			continue

		logger.info("Id: %s Show: %s, updating status from %s to %s" % (show['id'], series['title'], show['status'], series['status']))
		execute_sql("update", table={"shows"}, set={"status": series['status'], "year": series['year']}, where={"id": show['id']})


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
					try:
						title = show['title'].split("(")[0].strip()
					except Exception as e:
						logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
						title = show['title']

					plex_id = plex_api.library.section(config.plex_library).searchShows(title=title)[0].ratingKey
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
				execute_sql("insert", table={"shows"}, values={"id": show['id'], "title": show['title'], "year": show['year'], "status": show['status'], "tvdb": show['tvdbId']}, on_duplicate={"id": show['id'], "status": show['status']}, returnValue="id")
				logger.info("Added Show: %s, Id: %s" % (show['title'], show['id']))
		except Exception as e:
			logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))


# noinspection PyTypeChecker
def session_search():
	if plex_api is None:
		refresh_database()
	try:
		for x in plex_api.sessions():
			logger.info("User: %s is watching %s" % (x.usernames[0], create_plex_title(x)))
			try:
				plex_id = x.grandparentRatingKey
				user_id = [user['id'] for user in users if user['username'] == x.usernames[0]][0]
				watch_season = int(x.parentIndex)
				watch_episode = int(x.index)
				user_history = execute_sql("select", select={"show_id", "show_title", "plex", "user_id", "username", "last_watch_season", "last_watch_episode"}, table={"last_watched"}, where={"plex": plex_id, "user_id": user_id})
				try:
					if watch_season > int(user_history[0]['last_watch_season']) or (watch_season == int(user_history[0]['last_watch_season']) and watch_episode > int(user_history[0]['last_watch_episode'])):
						execute_sql("update", table={"usersWatch"}, set={"last_watch_season": watch_season, "last_watch_episode": watch_episode}, where={"showId": user_history[0]['show_id'], "userId": user_id})
						logger.debug("Users Watch Updated")
						update_show(plex_id=plex_id)
				except IndexError:
					logger.debug("Not a Tagged Show")
				except TypeError:
					logger.debug('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
				except Exception as e:
					logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
			except AttributeError:
				pass
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
				# logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
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
		title = "%s - S%sE%s - %s" % (video.grandparentTitle, video.parentIndex, video.index, video.title)
	return title


def plex_users():
	try:
		logger.info("Updating Users from Plex")
		if plex_api is None:
			refresh_database()

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
			where_formatted = " and ".join(["%s='%s'" % (k, escape_string(str(kwargs['where'][k]))) for k in kwargs['where']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "UPDATE %s SET %s WHERE %s" % (table_formatted, set_formatted, where_formatted)
		elif query_type == "delete":
			table_formatted = "".join(kwargs['table'])
			where_formatted = " and ".join(["%s='%s'" % (k, kwargs['where'][k]) for k in kwargs['where']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			query = "DELETE FROM %s WHERE %s" % (table_formatted, where_formatted)
		elif query_type in ("insert", "replace"):
			table_formatted = "".join(kwargs['table'])
			columns = ",".join([str(k) for k in kwargs['values']])
			values = ",".join(["'" + escape_string(str(kwargs['values'][k])) + "'" for k in kwargs['values']])
			return_value = "id" if 'return_value' not in kwargs else kwargs['return_value']
			if query_type == "insert":
				query = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(`id`)" % (table_formatted, columns, values)
				query += ";" if 'on_duplicate' not in kwargs else ",%s" % ", ".join(["%s='%s'" % (k, kwargs['on_duplicate'][k]) for k in kwargs['on_duplicate']])
			elif query_type == "replace":
				query = "REPLACE INTO %s (%s) VALUES (%s)" % (table_formatted, columns, values)
				query += ";"

		if query is not None:
			query = query.replace("'None'", "Null").replace("None", "Null")
			logger.debug(query)
			with warnings.catch_warnings():
				warnings.simplefilter("ignore")
				# noinspection PyUnresolvedReferences
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
	try:
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
			sdr = sodarr.API(config.sonarr_host + '/api/v3', config.sonarr_api)
		except Exception as e:
			logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))
			sdr = None
			logger.error("Can't Connect to Sonarr")

		users = execute_sql("select", select={"*"}, table={"users"})
		shows = execute_sql("select", select={"*"}, table={"shows"})
	except Exception as e:
		logger.error('Error on line {} - {} - {}'.format(type(e).__name__, sys.exc_info()[-1].tb_lineno, e))


class Users(object):
	def __init__(self):
		self.update()

	@staticmethod
	def get_users():
		Users.users = execute_sql("select", select={"*"}, table={"users"})


refresh_database()
