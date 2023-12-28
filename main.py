import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from flask import Flask, request, make_response
from flask_apscheduler import APScheduler

formatter = logging.Formatter('%(asctime)s - %(levelname)10s - %(module)15s:%(funcName)30s:%(lineno)5s - %(message)s')
logger = logging.getLogger()
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
logger.setLevel(os.environ.get('LOG_LEVEL').upper())
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(os.environ.get('LOG_LEVEL').upper())
logger.addHandler(consoleHandler)

fileHandler = RotatingFileHandler('/app/logs/omni.log', maxBytes=1024 * 1024 * 5, backupCount=100)
fileHandler.setFormatter(formatter)
fileHandler.setLevel(os.environ.get('LOG_LEVEL').upper())
logger.addHandler(fileHandler)

import utils

global users
global shows
global plex_api
global sdr


def run_functions():
	logger.info("Running Hourly Functions")
	utils.add_show()
	utils.add_plex()


def full_check():
	logger.info("Running Full Check")
	utils.plex_users()
	utils.add_show()
	utils.add_plex()
	utils.update_status()
	utils.update_monitored()
	utils.get_watched()
	utils.update_show()


app = Flask(__name__)


@app.route('/plex', methods=['POST'])
def plex_webhook():
	try:
		omni_users = utils.execute_sql("select", select={"id", "username", "token"}, table={"users"})
		content = json.loads(request.form["payload"])
		event = content['event']
		if event in ("media.scrobble", "library.new"):
			item = content['Metadata']
			logger.debug("Received Plex Webhook - %s - %s" % (event, item))
			if item['librarySectionTitle'] == os.environ.get('PLEX_LIBRARY'):
				if event == "media.scrobble":
					user_id = [user['id'] for user in omni_users if user['username'] == content['Account']['title']][0]
					plex_id = item['grandparentRatingKey']

					watch_season = int(item['parentIndex'])
					watch_episode = int(item['index'])
					user_history = utils.execute_sql("select", select={"show_id", "show_title", "plex", "user_id", "username", "last_watch_season", "last_watch_episode"}, table={"last_watched"}, where={"plex": plex_id, "user_id": user_id})
					try:
						if watch_season > int(user_history[0]['last_watch_season']) or (watch_season == int(user_history[0]['last_watch_season']) and watch_episode > int(user_history[0]['last_watch_episode'])):
							utils.get_watched(plex_id=plex_id, user_id=user_id)
							utils.update_show(plex_id=plex_id)
					except IndexError:
						logger.debug("Not a Tagged Show")
					except Exception as e:
						logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
				elif event == "library.new":
					if 'local' in item['guid']:
						logger.warning("Local Guid (%s) Found: %s" % (item['guid'], item))
						logger.debug(content)

					plex_id = None
					show_title = None
					try:
						if item['type'] == 'show':
							plex_id = item['ratingKey']
							show_title = item['title']
						elif item['type'] == 'episode':
							plex_id = item['grandparentRatingKey']
							show_title = item['grandparentTitle']
					except KeyError:
						logger.error(item)

					if plex_id:
						utils.execute_sql("update", table={"shows"}, set={"plex": plex_id}, where={"title": show_title, "plex": None})
						utils.refresh_database()
						utils.update_show(plex_id=plex_id)
			else:
				logger.debug("Not a TV Show")
		else:
			logger.debug("Event (%s) is Ignored with Content (%s)" % (event, content))
	except KeyError as e:
		logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
		logger.error(content)
		logger.error(event)
	except Exception as e:
		logger.error('Error on line {}, {}. {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))

	return make_response("", 200)


class Config(object):
	JOBS = [
		{'id': 'refresh_database', 'func': utils.refresh_database, 'trigger': 'interval', 'hours': 1},
		{'id': 'run_functions', 'func': run_functions, 'trigger': 'interval', 'hours': 1},
		{'id': 'modify_new', 'func': utils.modify_new, 'trigger': 'interval', 'minutes': 7},
		{'id': 'recently_watched', 'func': utils.recently_watched, 'trigger': 'interval', 'minutes': 42},
		{'id': 'full_check', 'func': full_check, 'trigger': 'interval', 'hours': 12},
		{'id': 'session_search', 'func': utils.session_search, 'trigger': 'interval', 'minutes': 2},
	]


app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
