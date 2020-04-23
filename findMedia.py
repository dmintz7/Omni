import urllib.parse, sys, os, logging, requests, json, time, config
from logging.handlers import RotatingFileHandler
from slackclient import SlackClient
import xml.etree.ElementTree as ET

DEBUGMODE = True if config.LOG_LEVEL == "DEBUG" else False

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

plex_host = config.plex_host
plex_token = config.plex_api
error_threshold = 50
MediaChuncks = 40

Extras = ['behindthescenes', 'deleted', 'featurette', 'interview', 'scene', 'short', 'trailer']
ExtrasDirs = ['behind the scenes', 'deleted scenes', 'featurettes', 'interviews', 'scenes', 'shorts', 'trailers']
Specials = ['season 00', 'season 0', 'specials'] 
KEYS = ['IGNORE_HIDDEN', 'IGNORED_DIRS', 'VALID_EXTENSIONS', 'IGNORE_SPECIALS']
excludeElements = 'Actor,Collection,Country,Director,Genre,Label,Mood,Producer,Role,Similar,Writer'
excludeFields = 'summary,tagline'
SUPPORTEDSECTIONS = ['movie', 'show']
DEFAULTPREFS = {
				'IGNORE_EXTRAS' : False,
				'IGNORE_SPECIALS' : False,
				'IGNORE_HIDDEN' : True,
				'IGNORED_DIRS' : [".@__thumb",".AppleDouble","lost+found"],
				'VALID_EXTENSIONS' : ['3g2', '3gp', 'asf', 'asx', 'avc', 'avi', 'avs', 'bivx', 'bup', 'divx', 'dv', 'dvr-ms', 'evo', 'fli', 'flv', 'm2t', 'm2ts', 'm2v', 'm4v', 'mkv', 'mov', 'mp4', 'mpeg', 'mpg', 'mts', 'nsv', 'nuv', 'ogm', 'ogv', 'tp', 'pva', 'qt', 'rm', 'rmvb', 'sdp', 'svq3', 'strm', 'ts', 'ty', 'vdr', 'viv', 'vob', 'vp3', 'wmv', 'wpl', 'wtv', 'xsp', 'xvid', 'webm']
				}

CoreUrl = plex_host + '/library/sections/'

def scanShowDB(sectionNumber=0):
	try:
		logger.info('Starting scanShowDB for section %s' % (sectionNumber))
		totalSize = get_xml(CoreUrl + str(sectionNumber) + '/all?X-Plex-Container-Start=1&X-Plex-Container-Size=0&X-Plex-Token=' + plex_token).attrib["totalSize"]
		logger.info('Total size of medias are %s' % (totalSize))
		mediasFromDB = []
		iShow = 0
		iCShow = 0
		while True:
			shows = get_xml(CoreUrl + str(sectionNumber) + '/all?X-Plex-Container-Start=' + str(iCShow) + '&X-Plex-Container-Size=' + str(MediaChuncks) + '&excludeElements=' + excludeElements + '&excludeFields=' + excludeFields + '&X-Plex-Token=' + plex_token)
			for show in shows:
				if DEBUGMODE: logger.info('Scanning database (%s of %s) - %s Seasons: %s, Episodes: %s' % (iShow + 1, totalSize, show.attrib['title'], show.attrib['childCount'], show.attrib['leafCount']))
				iSeason = 0
				iCSeason = 0
				while True:
					seasons = get_xml(plex_host + show.attrib['key'] + '?X-Plex-Container-Start=' + str(iCSeason) + '&X-Plex-Container-Size=' + str(MediaChuncks) + '&excludeElements=' + excludeElements + '&excludeFields=' + excludeFields + '&X-Plex-Token=' + plex_token)
					for season in seasons:
						if season.attrib['title'] == 'All episodes':
							iSeason += 1
							continue
						iSeason += 1
						iEpisode = 0
						iCEpisode = 0
						while True:
							videos = get_xml(plex_host + season.attrib['key'] + '?X-Plex-Container-Start=' + str(iCEpisode) + '&X-Plex-Container-Size=' + str(MediaChuncks) + '&excludeElements=' + excludeElements + '&excludeFields=' + excludeFields + '&X-Plex-Token=' + plex_token)
							for video in videos:
								try:
									for media in video:
										for part in media:
											filename = urllib.parse.unquote(part.attrib['file'])
											if addThisItem(filename): mediasFromDB.append(filename)
									iEpisode += 1
								except:
										logger.info(video.attrib)
							iCEpisode += MediaChuncks
							if len(videos) == 0:
								break
					iCSeason += MediaChuncks
					if len(seasons) == 0:
						break
				iShow += 1
			iCShow += MediaChuncks
			if len(shows) == 0:
				logger.info('Done scanning the database')
				break
		return mediasFromDB
	except Exception as e:
		logger.error('Fatal error in scanShowDB: %s' % e)
		if DEBUGMODE: raise

def findMissingFromDB(mediasFromFileSystem, mediasFromDB):
	logger.info('Finding items missing from Database')
	MissingFromDB = []
	try:
		mediasFromFileSystem
		for item in mediasFromFileSystem:
			if item not in mediasFromDB:
				MissingFromDB.append(item)
		return MissingFromDB
	except ValueError:
		logger.error('Aborted in findMissingFromDB')

def findMissingFromFS(mediasFromFileSystem, mediasFromDB):
	logger.info('Finding items missing from FileSystem')
	MissingFromFS = []
	try:
		for item in mediasFromDB:
			if item not in mediasFromFileSystem:
				MissingFromFS.append(item)
		return MissingFromFS
	except ValueError:
		logger.error('Aborted in findMissingFromFS')

def getFiles(filePath):
	try:
		mediasFromFileSystem = []
		bScanStatusCount = 0
		file_count = 0
		for Path in filePath:
			bScanStatusCount += 1
			logger.info("Scanning filepath #%s: %s" % (bScanStatusCount, Path))
			try:
				for root, subdirs, files in os.walk(Path):
					for file in files:
						filename = os.path.join(root, file)
						if addThisItem(filename):
							file_count+=1
							if DEBUGMODE: logger.info('appending file: %s' %  filename)
							mediasFromFileSystem.append(filename)
			except Exception as e:
				logger.error('Exception happened in FM scanning filesystem: %s' % e)
				if DEBUGMODE: raise
		logger.info('Finished scanning filesystem - %s Files Found' % file_count)

		return mediasFromFileSystem
	except Exception as e:
		logger.error('Exception happend in getFiles: %s' % e)
		if DEBUGMODE: raise

def scanMovieDb(sectionNumber=0):
	try:
		mediasFromDB = []
		logger.info('Starting scanMovieDb for section %s' % (sectionNumber))
		totalSize = get_xml(CoreUrl + str(sectionNumber) + '/all?X-Plex-Container-Start=1&X-Plex-Container-Size=0&X-Plex-Token=' + plex_token).attrib["totalSize"]
		logger.info('Total size of medias are %s' % (totalSize))
		iStart = 0
		iCount = 0
		while True:
			medias = get_xml(CoreUrl + str(sectionNumber) + '/all?X-Plex-Container-Start=' + str(iStart) + '&X-Plex-Container-Size=' + str(MediaChuncks) + '&excludeElements=' + excludeElements + '&excludeFields=' + excludeFields + '&X-Plex-Token=' + plex_token)
			for video in medias:
				if DEBUGMODE: logger.info('Scanning database (%s of %s) - %s' % (iCount, totalSize, video.attrib['title']))
				iCount += 1
				for media in video:
					for part in media:
						filename = part.attrib['file']
						if addThisItem(filename):
							if DEBUGMODE: logger.info('appending file: %s' % filename)
							mediasFromDB.append(filename)

			iStart += MediaChuncks
			if len(medias) == 0:
				logger.info('Done scanning the database')
				break
		return mediasFromDB
	except Exception as e:
		logger.error('Fatal error in scanMovieDb: %s' % e)
		if DEBUGMODE: raise

def scanMedias(sectionNumber, sectionLocations, sectionType):
	try:
		mediasFromDB = []
		mediasFromFileSystem = []
		MissingFromDB = []
		MissingFromFS = []

		if sectionType == 'movie':
			mediasFromDB = scanMovieDb(sectionNumber)
		elif sectionType == 'show':
			mediasFromDB = scanShowDB(sectionNumber)
		else:
			logger.info('Unsupported Section Type: %s' % sectionType)
		mediasFromFileSystem = getFiles(sectionLocations)

		MissingFromFS = findMissingFromFS(mediasFromFileSystem, mediasFromDB)
		MissingFromDB = findMissingFromDB(mediasFromFileSystem, mediasFromDB)

		if len(MissingFromDB) > 0: refresh_plex_section(sectionNumber)
			
		if DEBUGMODE: logger.info("Files Missing from the File System for Section Number: %s: %s" % (sectionNumber, MissingFromFS))
		if DEBUGMODE: logger.info("Files Missing from Plex for Section Number: %s: %s" % (sectionNumber, MissingFromDB))
		return (MissingFromFS, MissingFromDB)
	except Exception as e:
		logger.error('Exception happend in scanMedias: %s' % e)
		if DEBUGMODE: raise

def refresh_plex_section(sectionNumber):
	logger.info("Refreshing Section")
	get_xml(CoreUrl + str(sectionNumber) + '/refresh?X-Plex-Token=' + plex_token)

def addThisItem(file):
	try:
		if os.path.splitext(file)[1].lower()[1:] in DEFAULTPREFS['VALID_EXTENSIONS']:
			parts = splitall(file)
			for part in parts:
				if DEFAULTPREFS['IGNORE_EXTRAS']:
					if part.lower() in ExtrasDirs:
						return False
					for extra in Extras:
						if extra in part.lower():
							return False
				if DEFAULTPREFS['IGNORE_SPECIALS']:
					for special in Specials:
						if special == part.lower():
							return False
				if DEFAULTPREFS['IGNORE_HIDDEN']:
					if part.startswith('.'):
						return False
			return True
		else:
			return False
	except Exception as e:
		logger.error('Exception in addThisItem was %s' % e)
		return False

def splitall(path):
	allparts = []
	while 1:
		parts = os.path.split(path)
		if parts[0] == path:
			allparts.insert(0, parts[0])
			break
		elif parts[1] == path:
			allparts.insert(0, parts[1])
			break
		else:
			path = parts[0]
			allparts.insert(0, parts[1])
	return allparts

def get_xml(url):
	pause = 0
	while True:
		pause+=1
		try:
			resp = requests.get(url)
			data = resp.content.decode('utf-8')
			xml = ET.fromstring(data)
		except ET.ParseError:
			xml = ""
			pass
		except:
			time.sleep(pause)
			continue
		break

	return xml
	
def sendMessage(response, attachments=None):
	try:
		sc = SlackClient(config.slack_api)
		result = sc.api_call("chat.postMessage", channel=config.slack_channel, text=response, as_user=False, attachments=json.dumps(attachments))
		if str(result['ok']) == 'True':
			logger.debug("Succesfully Sent Message - %s" % result)
			return "success"
		else:
			logger.error("Failed Sending Message - %s" % result)
			return "fail"
	except Exception as e:
		logger.error("Error Sending Message - Exception: %s" % e)
		return "error"

if __name__ == "__main__":
	logger.info('scanSection started')
	error = False
	review=""
	try:
		missing_files = []
		missing_db = []
		response = get_xml(CoreUrl + '?X-Plex-Token=' + plex_token)
		for section in response:
			sectionNumber = section.attrib['key']
			sectionTitle = section.attrib['title']
			sectionType = section.attrib['type']
			sectionLocations = []
			for location in section:
				sectionLocations.append(os.path.normpath(location.attrib['path']))
			logger.info('Going to scan section %s with a title of %s and a type of %s and locations as %s' % (sectionNumber, sectionTitle, sectionType, str(sectionLocations)))
			(MissingFromFS, MissingFromDB) = scanMedias(sectionNumber, sectionLocations, sectionType)
			missing_files+=MissingFromFS
			missing_db+=MissingFromDB

		message = ""
		if len(MissingFromFS+MissingFromDB) > error_threshold:
			message = message + "Over Threshold of %s Files Missing. Check Server" % error_threshold
		else:
			if len(missing_files) > 0:
				message = message + "***Files Missing From File System***\n"
				for file in missing_files: message = message + file + '\n'
			if len(missing_db) > 0:
				message = message + "***Files Missing From Plex***\n"
				for file in missing_db: message = message + file + '\n'

		review = "%s Files Missing from FS, %s Files Missing from Plex" % (len(missing_files), len(missing_db))
		if len(message) > 0:
			sendMessage(message)
			logger.info("Files Missing from the File System: %s" % missing_files)
			logger.info("Files Missing from Plex: %s" % missing_db)
			review = "%s Files Missing from FS, %s Files Missing from Plex" % (len(missing_files), len(missing_db))
		else:
			logger.info("No Files Missing")
		
	except Exception as ex:
		logger.error('Fatal error happened in scanSection: %s' % ex)
		if DEBUGMODE: raise