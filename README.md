# Omni
 
Automatically change monitor status in Sonarr upcoming episodes for shows currently being watched on Plex

Watched status is based on any user on the selected Plex instance

Allows shows to be added to Sonarr without monitoring all episodes of the show. 

Script as three different settings:
 * modify_new:
	* checks for any shows with Sonarr profile equivalent to "sonarr_from_profile" in config
	* any shows found will have initial changes made
		* profile for show is changed to "sonarr_to_profile"
		* tag id 2 is added to show. This tag is used to distinguish these shows from other shows in the Sonarr instance
		* enable first 5 episodes of season 1 for show to be monitored. if season is 8 or less than episodes, the entire season will be monitored
		* search for all monitored episodes
 * session_search:
	* check if any shows are currently being watched. Depending on which episode is being watched, future episodes will be monitored and searched
 * full_check: 
	* in random order, search all shows if episodes should be monitored and downloaded
	
New episodes are monitored and downloaded based on the following rules:
 * If episode 3 is being watched/has been watched, monitor the rest of the season
 * If 75% or more of the season has been watched, monitor the first 5 episodes of the next season
 * If any season is 8 or fewer episodes, monitor the whole season


I recommend running the scripts using cron with modify_new and session_search running every 5 minutes and full_check every 12 hours

Needed Updates
* Change tag number to volatile based on name 
