import requests 
from bs4 import BeautifulSoup
'''
# https://en.wikipedia.org/wiki/Sustainable_energy
example = soup.find_all("a")[10]
title = example['title']
href = example['href']
text = example.text
print title, href, text
'''
DEPTH_MAX = 5
def dfs(url, result, depth):
	if depth > DEPTH_MAX:
		return
	source_code = requests.get(url).text
	soup = BeautifulSoup(source_code)
	for item1 in soup.find_all("p"):
		for item in item1.find_all("a"):
			if "href" in str(item):
				if "solar" in item['href'].lower() or "solar" in item.text.lower():
					if "/wiki/" in item['href'] and ":" not in item['href'] and "#" not in item['href']:
						new_url = "https://en.wikipedia.org"+item['href']
						if new_url not in result:
							# result.append([url, new_url, depth])
							# result.append([new_url, depth, url])
							result.append(new_url)
							dfs(new_url, result, depth+1)
	return result
depth = 1
result = []
dfs("https://en.wikipedia.org/wiki/Sustainable_energy", result, depth)
for item in result:
	print item
	# print ','.join([str(item1) for item1 in item])