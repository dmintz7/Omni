import os, logging, sys, config, subprocess, pymysql
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

if __name__ == "__main__":
	review=""
	x = 0
	try:
		conn = pymysql.connect(host=config.mysql_host, user=config.mysql_user, passwd=config.mysql_passwd, autocommit=True, use_unicode=True, charset="utf8")
		cur = conn.cursor()
		cur.execute("show databases;")
		databases = list(cur)
		cur.close
		
		folder_name = filename = "%s/SQL Database/%s" % (config.backup_path, datetime.now().strftime("%Y-%m-%d"))
		os.makedirs(folder_name, exist_ok=True)
		for x, db in enumerate(databases):
			try:
				db = db[0]
				logger.info("(%s/%s) Backing Up SQL Database: %s" % (x+1, len(databases), db))
				filename = "%s/%s - %s.sql" % (folder_name, datetime.now().strftime("%Y-%m-%d %H%M%S%f"), db)
				command = 'mysqldump -h %s -u %s -p%s --databases %s' % (config.mysql_host, config.mysql_user, config.mysql_passwd, db)
				with open(filename,'w') as output:
					c = subprocess.Popen(command, stdout=output, shell=True)
					c.wait()
			except Exception as e:
				logger.error("Error Backing Up Database: %s - %s" % (db, e))
				error=True
				raise
	except Exception as e:
		logger.error("Error Backing Up SQL Databases - %s" % e)
		error=True
		raise
	review = "%s Databases Backed Up" % x