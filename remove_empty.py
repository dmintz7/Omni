import os, sys, config, logging
from logging.handlers import RotatingFileHandler

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

review = ""
total_folders = 0
total_removed = 0

def removeEmptyFolders(path, removeRoot=True):
	global review
	global indiv_folders
	global indiv_removed
	global total_folders
	global total_removed

	'Function to remove empty folders'
	if not os.path.isdir(path):
		return

	# remove empty subfolders
	files = os.listdir(path)
	if len(files):
		for f in files:
			fullpath = os.path.join(path, f)
			if os.path.isdir(fullpath):
				indiv_folders+=1
				total_folders+=1
				removeEmptyFolders(fullpath)

	# if folder empty, delete it
	files = os.listdir(path)
	if len(files) == 0 and removeRoot:
		logger.info("Removing empty folder: %s" % path)
		os.rmdir(path)
		indiv_removed+=1
		total_removed+=1
		

if __name__ == "__main__":
	for path in config.empty_folder_path.split(";"):
		indiv_folders = 0
		indiv_removed = 0
		removeEmptyFolders(path, False)
		logger.info("%s - %s Folders Scanned, %s Folders Removed" % (path, indiv_folders, indiv_removed))

