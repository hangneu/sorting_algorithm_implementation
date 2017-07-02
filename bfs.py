import requests 
from bs4 import BeautifulSoup
'''
# example code
source_code = requests.get("https://en.wikipedia.org/wiki/Sustainable_energy").text
soup = BeautifulSoup(source_code)
b = soup.find_all("a")
'''
def bfs(url):
	result = []
	source_code = requests.get(url).text
	soup = BeautifulSoup(source_code)
	for item in soup.find_all("a"):
		if item.get("href",0)!=0 and "/wiki/" in item['href'] and "#" not in item['href'] and ":"  not in item['href']:
			if "solar" in item.get("href").lower() or "solar" in item.text.lower():
				new_url = "https://en.wikipedia.org" + item['href']
				result.append(new_url)
	print len(result)
bfs("https://en.wikipedia.org/wiki/Sustainable_energy")
