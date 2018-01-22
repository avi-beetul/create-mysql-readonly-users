import pymysql
import csv
import os
import sys, getopt
import logging
from gdrive import download_from_gdrive

DATABASE = {
	'rds_host':'xxxxxxxxxxxxxxxx',
	'rds_port':'3306',
	'rds_db_name':'xxxxxxxx',
	'rds_username':'xxxxx',
	'rds_password':'xxxxxxxxxx'
}

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_user_from_csv(conn, file_path):
	try:
		with conn.cursor() as cur:
			if os.path.getsize(file_path) > 0:
				with open(file_path) as csvfile:
					reader = csv.DictReader(csvfile)
					for row in reader:
						try:
							result = query_create(cur, row['Name'], row['Password'])
							if result is None:
								query_grant(cur, row['Name'], row['Password'])

							logger.info("Success: user %s created!" % row['Name'])
						except Exception as e:
							logger.error("%s: %s" % (e.__class__, e))
							logger.info("ERROR: Could not create DB user for %s!" % row['Name'])
	finally:
		conn.close()

def delete_user_from_csv(conn, file_path):
	try:
		with conn.cursor() as cur:
			if os.path.getsize(file_path) > 0:
				with open(file_path) as csvfile:
					reader = csv.DictReader(csvfile)
					for row in reader:
						try:
							query_delete(cur, row['Name'])
							logger.info("Success: user %s deleted!" % row['Name'])
						except Exception as e:
							logger.error("%s: %s" % (e.__class__, e))
							logger.info("ERROR: Could not delete DB user for %s!" % (row['Name']))
	finally:
		conn.close()

def query_create(cur, name, password):
	try:
		cur.execute("CREATE USER IF NOT EXISTS '{0}'@'%' IDENTIFIED BY '{1}';".format(str(name), str(password)))
		return cur.fetchone()
	except Exception as e:
		logger.error("%s: %s" % (e.__class__, e))
		logger.info("User exists!!!")

def query_grant(cur, name, password):
	try:
		cur.execute("GRANT select ON `%`.* TO '{0}'@'%' IDENTIFIED BY '{1}' WITH MAX_USER_CONNECTIONS 3;".format(str(name), str(password)))
		return cur.fetchone()
	except Exception as e:
		logger.error("%s: %s" % (e.__class__, e))
		logger.info("Failed to grant privileges!")

def query_delete(cur, name):
	try:
		cur.execute("DROP USER IF EXISTS '{0}'@'%';".format(str(name)))
		return cur.fetchone()
	except Exception as e:
		logger.error("%s: %s" % (e.__class__, e))
		logger.info("User may not exist in the database!!!")

def main(argv):
	try:
		opts, args = getopt.getopt(argv, "h", ["help"])
	except getopt.GetoptError:
		print("create_db_user.py <create/delete> <filename>")
		sys.exit(2)
	for opt in opts:
		if opt[0] == '-h' or opt[0] == '--help':
			print("create_db_user.py <create/delete> <filename>")
			sys.exit()

	if len(sys.argv) == 3:
		# make DB connection
		try:
			conn = pymysql.connect(host=DATABASE['rds_host'], user=DATABASE['rds_username'],
								   passwd=DATABASE['rds_password'], db=DATABASE['rds_db_name'], connect_timeout=5)
		except:
			logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
			sys.exit()

		# file_path = '/home/user/file_dir/files/readonly_db_users.csv'
		download_from_gdrive(sys.argv[2])
		file_path = "{0}/{1}".format(str(os.path.dirname(os.path.realpath(__file__))), str(sys.argv[2]))
		if file_path:
			print(file_path)
			if sys.argv[1] == 'create':
				create_user_from_csv(conn, file_path)
			elif sys.argv[1] == 'delete':
				delete_user_from_csv(conn, file_path)
			else:
				sys.exit()

			os.remove(file_path)
	else:
		sys.exit("Wrong number of arguments! Try 'create_db_user.py --help' for more information.")


if __name__ == '__main__':
	main(sys.argv[1:])

