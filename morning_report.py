import config, random, requests
from bs4 import BeautifulSoup
from slackclient import SlackClient

popular_choice = ['age','alone','amazing','anger','anniversary','architecture','art','attitude','beauty','best','birthday','brainy','business','car','chance','change','christmas','communication','computers','cool','courage','dad','dating','death','design','diet','dreams','easter','education','environmental','equality','experience','failure','faith','family','famous','father''s day','fear','finance','fitness','food','forgiveness','freedom','friendship','funny','future','gardening','god','good','government','graduation','great','happiness','health','history','home','hope','humor','imagination','independence','inspirational','intelligence','jealousy','knowledge','leadership','learning','legal','life','love','marriage','medical','memorial day','men','mom','money','morning','mother''s day','motivational','movies','moving on','music','nature','new year''s','parenting','patience','patriotism','peace','pet','poetry','politics','positive','power','relationship','religion','respect','romantic','sad','saint patrick''s day','science','smile','society','space','sports','strength','success','sympathy','teacher','technology','teen','thankful','thanksgiving','time','travel','trust','truth','valentine''s day','veterans day','war','wedding','wisdom','women','work']

def get_quotes(type, number_of_quotes=1):
	url = "http://www.brainyquote.com/quotes/topics/topic_" + type + ".html"
	response = requests.get(url)
	soup = BeautifulSoup(response.text, "html.parser")
	quotes = []
	full_quotes = []
	for quote in soup.find_all('a', {'title': ['view quote', 'view author']}):
		quotes.append(quote.contents[0])

	for x in range(0, int(len(quotes)/2)):
		initial = x * 2
		full_quotes.append(quotes[initial] + " - " + quotes[initial+1])

	random.shuffle(full_quotes)
	result = full_quotes[:number_of_quotes][0]
	if "img alt" in result: result = getSubString(result, "img alt=", "class").strip()
	if result.startswith('"') and result.endswith('"'): result = result[1:-1]

	return (type, result)

def get_random_quote(format=True):
	while True:
		try:
			(topic, quote) = get_quotes(popular_choice[random.randint(0, len(popular_choice) - 1)])
		except:
			continue
			
		if format:
			return "%s\n\n%s" % (topic.title(), quote)
		else:
			return (topic, quote)
		
		
def getSubString(string, firstChar, secondChar, start=1):
	string = str(string)
	startChar = string.find(firstChar, start) + len(firstChar)
	endChar = string.find(secondChar, startChar)
	string = string[startChar:endChar]
	return string
	

if __name__ == "__main__":
	final_message = "Morning Report\n\nDaily Quote\n\n%s" % get_random_quote()
	
	sc = SlackClient(config.slack_api)
	sc.api_call("chat.postMessage", channel=config.report_slack_channel, text=final_message, as_user=False)