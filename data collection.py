### IST 402 - Reddit Data Collection ###

# time to run ~2.1 hours

# This scraper will connect to and scrape subreddits listed within a file for instances of 'x-post', it will then analyze the
# post and title to identify the subreddit that it was crossed from. This scraper produces a txt file with the original and 
# linked subreddit, and a file containing questionable links based on how the crossed subreddit was identified.
#
# The searches are delimited by the key word 'x-post', searches within the last year, and is sorted based on new posts. The 
# posts are accepted up until a cetain time that we based on the 'earliest max out time' of the major subreddits. Since reddit
# limits results on searches to ~1000, we accepted posts only after the earliest time on of the subreddits maxed out, to provide
# a consistent baseline for our analysis. 

import http.client
import json
import re
import threading
 
subredditFile = 'fullSubredditList.txt' #list of subreddits to be analyzed
textFile = 'collected data1.txt' #data produced
fromFile = 'posts using from1.txt' #list of titles and comments produced with 'post from' 
timeFile = 'timeFile1.txt' #time stamps of subreddits that max out
skippedFile = 'skipped subreddits1.txt' #list of subreddits that are skipped and errors recieved

inFile = open(subredditFile, 'r', encoding='utf-8')
outFile = open(textFile, 'a', encoding='utf-8')
fromFile = open(fromFile, 'a', encoding='utf-8') 
timeFile = open(timeFile, 'a', encoding='utf-8') 
skippedFile = open(skippedFile, 'a', encoding='utf-8')

def scrape_reddit():
    global inFile
    global outFile
    global skippedFile
    
    hdr= { 'User-Agent' : 'ist402 reddit scraper4' }
    redditURL = 'www.reddit.com'
    searchURL = '/search.json?q=x-post'
    timeURL = '&sort=new&t=year&restrict_sr=on' 

    conn = http.client.HTTPConnection(redditURL) 

    subredditNum = 0  
    threads = []     
    
    for line in inFile:
        subreddit = str('/r/' + (line.strip()))
        afterURL = ''
        hasNext = True
        subredditNum += 1
        pages = 0 
        
        while hasNext:
            conn.request('GET',subreddit + searchURL + afterURL + timeURL, headers=hdr)
            page = str(conn.getresponse().read().decode("utf-8"))
            
            try:
                parsedPage = json.loads(page)
            except Exception as e:
                skippedFile.write(subreddit + ' , %s' % e)
                break
            
            #checks that the page exists and if not, breaks
            try:
                if parsedPage['data']['after']:
                    afterURL = '&after=' + str(parsedPage['data']['after'])
                else:
                    hasNext = False
            except KeyError as e:
                skippedFile.write(subreddit + ' , %s' % e)
                break
            
            #offload parsing of pages to threads
            try:
                thread = pageThread(parsedPage,pages,hasNext)
                thread.start()
                threads.append(thread)
            except:
                print ('Error: unable to start thread ' + subreddit + ' ' + str(parsedPage['data']['after']))
            
            print(str(subredditNum) + ' - ' + str(pages) + ' - ' + str(parsedPage['data']['after']) + ' - ' + subreddit)
            pages +=1
        
    for x in threads:
        x.join()
        
    print('closing')
    conn.close()
    
    inFile.close()
    outFile.close()
    fromFile.close()
    timeFile.close()
    
    
    
class pageThread (threading.Thread):
    def __init__(self, parsedPage, pages, hasNext):
        threading.Thread.__init__(self)
        self.parsedPage = parsedPage
        self.pages = pages
        self.hasNext = hasNext
    def run(self):
        parse_data(self.parsedPage,self.pages,self.hasNext)
        
        
        
#function to parse the page from reddit into individual posts separated by line in a text file
def parse_data(parsedPage,pages,hasNext):
    global inFile
    global outFile
    global timeFile

    for x in range(len(parsedPage['data']['children'])):
        
        subreddit = str(parsedPage['data']['children'][x]['data']['subreddit'])
        
        title = str(parsedPage['data']['children'][x]['data']['title'])
        title = title.lower()
        title = title.replace("\n", " ") #remove new line characters

        #saves domain if
        if parsedPage['data']['children'][x]['data']['selftext']:
            comment = parsedPage['data']['children'][x]['data']['selftext']
        else:
            comment = parsedPage['data']['children'][x]['data']['domain']
        comment = comment.lower()
        comment = comment.replace("\n", " ") #remove new line characters

        xSubreddit = find_linked_subreddit(title,comment)
        
        #outFile.write('%s,%s,%s\n' % (subreddit,title,comment))
        if xSubreddit != 0:
            outFile.write('%s,%s\n' % (subreddit,xSubreddit))
            
    if pages >= 35 and hasNext == False:
        last = len(parsedPage['data']['children']) - 1 
        time = parsedPage['data']['children'][last]['data']['created_utc']
        timeFile.write('%s,%i\n' % (subreddit,time))
    
    
#function used to identify the linked subreddit within the comment or title
def find_linked_subreddit(title,comment):
    global fromFile
    
    while True:
        x = title.split('/r/')
        try:
            x = re.split('\W',x[1])
            xSubreddit = x[0]
            break
        except IndexError:
            x = title.split('r/')
            try:
                x = re.split('\W',x[1])
                xSubreddit = x[0]
                break
            except IndexError:               
                x = title.split('post from /')
                try:
                    x = re.split('\W',x[1])
                    xSubreddit = x[0]
                    if comment == "i.imgur.com" or "imgur.com":
                        fromFile.write(str(comment)+' , '+str(title)+'\n')
                    else:
                        fromFile.write(str(title)+' , '+str(comment)+'\n')
                        
                    break
                except IndexError:
                    x = title.split('post from ')
                    try:
                        x = re.split('\W',x[1])
                        xSubreddit = x[0]
                        if comment == "i.imgur.com" or "imgur.com":
                            fromFile.write(str(comment)+' , '+str(title)+'\n')
                        else:
                            fromFile.write(str(title)+' , '+str(comment)+'\n')
                            
                        break
                    except IndexError:
                        x = comment.split('/r/')
                        try:
                            x = re.split('\W',x[1])
                            xSubreddit = x[0]
                            break
                        except IndexError:
                            x = comment.split('r/')
                            try:
                                x = re.split('\W',x[1])
                                xSubreddit = x[0]
                                break
                            except IndexError:               
                                x = comment.split('post from /')
                                try:
                                    x = re.split('\W',x[1])
                                    xSubreddit = x[0]
                                    if comment == "i.imgur.com" or "imgur.com":
                                        fromFile.write(str(comment)+' , '+str(title)+'\n')
                                    else:
                                        fromFile.write(str(title)+' , '+str(comment)+'\n')
                            
                                    break
                                except IndexError:
                                    x = comment.split('post from ')
                                    try:
                                        x = re.split('\W',x[1])
                                        xSubreddit = x[0]
                                        if comment == "i.imgur.com" or "imgur.com":
                                            fromFile.write(str(comment)+' , '+str(title)+'\n')
                                        else:
                                            fromFile.write(str(title)+' , '+str(comment)+'\n')
                                
                                        break
                                    except IndexError:
                                        xSubreddit = 0
                                        break
    return xSubreddit



if __name__ == '__main__':
    scrape_reddit()