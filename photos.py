import os, logging, sys, exifread, config, hashlib, shutil
from logging.handlers import RotatingFileHandler
from datetime import datetime

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

def file_hash_hex(file_path, hash_func):
	with open(file_path, 'rb') as f:
		return hash_func(f.read(4096)).hexdigest()

if __name__ == "__main__":
	review=""
	photo_count = 0
	dup_count = 0
	path = "%s/Albums/%s" % (config.photo_path, "iphone")
	temp_paths = config.photo_temp_path.split(";")
	dup_path = "%s/Duplicates" % config.photo_path
	for temp_path in temp_paths:
		try:
			files = ""
			for root, dirs, files in os.walk(temp_path, topdown = False):
				for file in files:
					try:
						photo_count+=1
						filename = os.path.join(root, file)
						if '~' in filename: continue
						file_org, file_extension = os.path.splitext(filename.lower())
						f = open(filename, 'rb')
						tags = exifread.process_file(f)
						try:
							date_to_use = datetime.strptime(tags["EXIF DateTimeOriginal"].printable, "%Y:%m:%d %H:%M:%S").strftime('%Y-%m-%d %H%M')
						except:
							date_to_use = datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y-%m-%d %H%M')
						file_hash = file_hash_hex(filename, hashlib.sha256)
						filename_new = "%s/%s - %s%s" % (path, date_to_use, file_hash, file_extension)
						if os.path.exists(filename_new):
							logger.debug("Already in Folder, Moving to Duplicates")
							dup_count+=1
							x = 1
							filename_new = "%s/%s - %s - %s%s" % (dup_path, date_to_use, file_hash, x, file_extension)
							while os.path.exists(filename_new):
								x+=1
								filename_new = "%s/%s - %s - %s%s" % (dup_path, date_to_use, file_hash, x, file_extension)
						logger.info("Moving File From %s to %s" %  (filename, filename_new))
						shutil.move(filename, filename_new)
					except Exception as e:
						error=True
						logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e))
						
			if photo_count == 0: logger.info("No Photos in %s to Process" % temp_path)
			review = "%s files (%s duplciates) Processed from %s\n" % (photo_count, dup_count, temp_path)
		except Exception as e:
			error=True
			logger.error("Unexpected error:", e)
			pass
