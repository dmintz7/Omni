import logging, config, os, sys, requests, socket

health_applications=config.health_checks_base_url + "/ping/" + application_health_checks_udid

apps = config.application_apps
requests.get(health_applications + "/start")
error=False
count=0
count_up=0
count_down=0
for name, ip, port, udid in apps:
	try:
		count+=1
		url = config.health_checks_base_url + "/ping/" + udid
		try:
			requests.get(url + "/start")
			status = "up"
			sock = socket.create_connection((ip, port), timeout=10)
			count_up+=1
		except socket.error:
			count_down+=1
			status = "down"
			
		review = "%s (%s:%s) is %s" % (name, ip, port, status)
		requests.get(url if status == "up" else url + "/fail", data=review)
	except:
		error=True
review  = "%s Checked (Up: %s, Down: %s)" % (count, count_up, count_down)
requests.get(health_applications if ((not error) or (count_down > 0)) else health_applications + "/fail", data=review)