
'''
Scrape My Professor

Scrapes reviews off of RMP and puts them in a DataFrame and CSV
Can do the scraping by school or by state
'''


from urllib.request import urlopen
from bs4 import BeautifulSoup as bs
import time
import re
import csv
#import ijson
import pandas as pd
import requests
import ast #for making the thing that looks like a list actually BE a list
import numpy as np
from selenium import webdriver
from collections import Counter

import gender_guesser.detector as gender
# this reads in the file for gender detection and creates a gender detector
d=gender.Detector(case_sensitive=False)



#################################################
# clean_text
# text = any string that needs cleaning
# condenses whitespace and removes capitals and puncturation and other characters
# Need to do this for ALL reviews
#################################################
def clean_text(text):
    # strip leading/trailing spaces from text
    text = text.strip()
    #condense remaining whitespace and remove all nonalpha
    text = re.sub('\s+', ' ', text)
    text = re.sub('[^A-Za-z ]+', '', text) 
    text = text.lower() # write all as lowercase
    text = text + ' '
    return text       
    
##############################
# pad
# myList is a list that exists
# length is desired length to force
# padding is particular character to pad with if desired
# Sometimes need to guarantee lists of certain size (even if input not list)
# Need to make sure the typeerror exception is really working
##############################

def pad(myList,length,padding=None):
    pad_list = [None]*length
    if len(myList)<length:
        try:
            return myList+pad_list[:length-len(myList)]
        except TypeError: # catch when for loop fails
            return pad([myList])
    else:
        return myList

##############################
# get_urls
#Creates urls to use for collecting data from a state or specific school ID
#Returns list of urls
#Selection should be a school ID or a state
##############################
def get_urls(selection,how='state'): #type = school if given ID of school
    #base url, from tjgambs github tjgambs@gmail.com
    
    if how == 'state':
        url = 'http://search.mtvnservices.com/typeahead/suggest/?callback=jQuery11100050687990384176373_1446754108140&q=*:*+AND+schoolstate_s:'+selection+'&siteName=rmp&'
    elif how == 'school':
        url = 'http://search.mtvnservices.com/typeahead/suggest/?callback=jQuery11100050687990384176373_1446754108140&q=*:*+AND+schoolid_s:' + str(selection) + '&siteName=rmp&'
    else:
        return "Error. Input type should be 'state' or 'school'."
        
    #need the html & bs to grab the number of faculty found
    school_result = urlopen(url)
    bsSchool = bs(school_result,"lxml").p.string

    
    # Find the string "numFound":XXXX where XXXX is an integer (it could be any length and then return the XXXX part)
    # Use group (?<= on this which will search for the "numFound: and then return what comes after it
    # Use group(0) to actually return what was found
    numberFaculty = int(re.search('(?<=\"numFound\":)[0-9]*', bsSchool).group(0))
    numberUrls = np.ceil(numberFaculty / 4000).astype(int)
    
    urls = []
    
    #write urls that import 4000 professors at a time
    for i in range(numberUrls):
        start=i*4000
        end=(i+1)*4000
        urls.append(url + '&start='+str(start)+'&rows=4000')
     
    return urls


###################################
# add_records
# url = urls from get_urls that returns query
# df = dataframe created to hold results of query
# Takes an url with professor or school data and adds to a dataframe 
# Starts with a df initially set up with columns
####################################
def add_records(url,df):
    school_result = urlopen(url)
    bsSchool = bs(school_result,"lxml").p.string
    
    #first delete returns and new lines
    noSpaceData = bsSchool.replace('\n','').replace('\r','').replace('í','i') +']'
    #this is to fix the fact that there is a name that contains double quotes around a nickname!
    noSpaceData = re.sub('\"[A-Za-z]+"(?= )',' ',noSpaceData)
    #fix name of prof with a single quote in middle of name
    noSpaceData = re.sub('(?<=[A-Za-z])\"(?=[A-Za-z])','',noSpaceData)
    
    #get rid of multiple spaces (replace 2 or more with just 1)
    noSpaceData = re.sub('  +','',noSpaceData)
    
    #get rid of the preamble junk
    noSpaceData = re.sub('^jQuery[0-9]+_[0-9]+[a-zA-Z0-9":({,}]+','',noSpaceData)
    
    # Remove everything after the last ]
    noSpaceData = re.sub('\].*',']',noSpaceData)
    
    #Load it into a pandas dataframe 
    return df.append(ast.literal_eval(noSpaceData),ignore_index=True)

###################################
# prof_df
# school = school pk_id, e.g. 1593
# # Returns dataframe that gives ID ("pk_id")  and url ("url") for all profs in a school 
###################################
def prof_df(school,filename=None):
    #create datafram with columns, note teacherfull name only there if data is from a teacher, others are always there
    recorddata = pd.DataFrame(columns=["pk_id", "schoolcity_s", "schoolcountry_s", "schoolname_s", "schoolstate_s","teacherfullname_s"])
    
    #run through the data from all the urls collected from 
    urls = get_urls(school,'school')
    if urls:   #check if list not empty, then add all the records
        recorddata = pd.concat([add_records(urls[i],recorddata) for i in range(len(urls))],ignore_index=True)
    
    #add the url for each prof in the school
    recorddata['url'] = 'http://www.ratemyprofessors.com/ShowRatings.jsp?tid='+ recorddata['pk_id'].astype(int).astype(str)
    recorddata['schoolID'] = school
    
    if filename is not None:
        recorddata.to_csv(filename+'_proflist'+'.csv', encoding='utf-8', header=True, index=False)
        print('File Output: '+filename+'_proflist'+'.csv')
    return recorddata

###################################
# state_df
# state = e.g. 'MA'
# Returns dataframe that gives ID ("pk_id")  and url ("url") for all profs in state
###################################
def state_df(state,filename=None):
    #create datafram with columns, note teacherfull name only there if data is from a teacher, others are always there
    recorddata = pd.DataFrame(columns=["pk_id", "schoolcity_s", "schoolcountry_s", "schoolname_s", "schoolstate_s","teacherfullname_s"])
    
    #run through the data from all the urls collected from 
    urls = get_urls(state,'state')
    if urls:   #check if list not empty
        recorddata = pd.concat([add_records(urls[i],recorddata) for i in range(len(urls))],ignore_index=True)

    # Keep only records where the teacher name is null, these will be school records
    recorddata = recorddata[recorddata['teacherfullname_s'].isnull()]
            
    #Now I have to run prof_df on each 
    #profs = pd.concat([prof_df(recorddata['pk_id'][i]) for i in range(len(recorddata))],ignore_index=True)
    profs = recorddata['pk_id'].astype(int).map(prof_df)
    profs = pd.concat(list(profs)).reset_index(drop=True)
    

    if filename is not None:
        profs.to_csv(filename+'_proflist'+'.csv', encoding='utf-8', header=True, index=False)
        print('File Output: '+filename+'_proflist'+'.csv')
 
    return profs




################################
# this function grabs all the tags for a prof and turns them into checkboxes
################################
def get_tags(soup):
    all_tags = {
    'tough':'Tough Grader','feedback':'Gives good feedback','respected':'Respected','read':'Get ready to read',
    'participation':'Participation matters','skip':'Skip class? You won\'t pass.','homework':'LOTS OF HOMEWORK',
    'inspirational':'Inspirational','quiz':'BEWARE OF POP QUIZZES','accessible':'ACCESSIBLE OUTSIDE CLASS',
    'papers':'SO MANY PAPERS','criteria':'Clear grading criteria','hilarious':'Hilarious',
    'test':'TEST HEAVY','few':'GRADED BY FEW THINGS','amazing':'Amazing lectures','caring':'Caring',
    'extra':'EXTRA CREDIT','group':'GROUP PROJECTS','lecture':'LECTURE HEAVY'
    }
    all_tags = dict((v.lower(),k.lower()) for k,v in all_tags.items())

    # this grabs tags 
    tag_rows = soup.find_all('div', {'class':'tagbox'})
    # make list of the tag information, all lowercase
    tag_list = [[cell.getText().lower() for cell in tag_rows[i].findAll('span')] for i in range(len(tag_rows))]

    # create a dataframe with the tag names as column names and a dummy variable for each
    # the .apply(pd.Series) makes each of the lists into dataframes
    # stack puts it all back into one column with multiple indexing
    # get_dummies creates the dummy variables
    # sum (level=0) makes all the differnt rows back into one single row
    
    return pd.get_dummies(pd.DataFrame(tag_list).apply(pd.Series).replace(all_tags).stack(dropna=False)).sum(level=0)  


####################################
# get_profData
####################################
def get_profData(soup):
        
    # grabbing professor info, get rid of returns and stripping
    profname = soup.find('h1', {'class':'profname'}).getText().replace('\n','').replace('\r','').strip()

    # grab title that has the department in it
    department = soup.find('div',{'class': "result-title"}).getText().replace('\n','').replace('\r','').strip()
    department = department.replace('Professor in the ','').split(' department',1)[0]
    
    #Get rid of leading and trailing spaces in profname
    #profname = re.sub('(^ +)|( +$)','',profname)
    # profname = profname.strip()
    
    #Middle should have single space
    profname = re.sub(' +',' ',profname)

    #Need first name only for gender 
    #profnameFirst = (re.search('^[a-zA-Z]+', profname).group(0))
    #profnameFirst = profname.split()[0]
    
    gender = d.get_gender(profname.split()[0])
    
    return [profname, department, gender]



#############################
# massage_faculty
# this does some initial processing of the file and returns beautifulsoup object
#############################

def massage_faculty(html):
    soup = bs(html,"lxml")
    
    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.decompose()    # rip it out

    # this first looks for the rows with ads and removes them with 'decompose'
    for tr in soup.find_all("tr", {'class':'ad-placement-container'}): 
        tr.decompose()
    
    # return bs object
    return soup

##############################
# review_df
# Creates the dataframe for a particular professor review
###############################
def review_df(soup,schoolID,profID,filename=None):
    # get the tags, holding for later
    tags = get_tags(soup)
    
    # get rid of tags in soup now
    for div in soup.find_all("div", {'class':'tagbox'}): 
        div.decompose()
        
    #remove extra junk at end of text rating
    for div in soup.find_all('div',{'class':'helpful-links'}):
        div.decompose()

    # this finds all of the remaining 'tr' tag
    data_rows = soup.find_all('tr')[1:] #the 1: says skip the first
    
    # this covers the rows and then gets each of the cells in the row
    # the getText.replace part gets the text and clears all the new lines
    review_data = [[cell.getText().replace('\n', ' ').replace('\r', ' ') for cell in data_rows[i].findAll('td')] for i in range(len(data_rows))]

    # this makes a dataframe and then adds the student ID to it
    review_df = pd.DataFrame(review_data,columns=['numInfo','classInfo','textInfo'])
    review_df = review_df.dropna(how="all")  #drop if there are blank cells now 
 

    # stop if this is actually an empty professor
    if len(review_df) == 0:
        print('No ratings (but passed to review_df function).')
        return

    print(str(len(review_df))+ ' reviews') #print number of reviews, for following while program runs

    # get the date as a column
    review_df["date"] = review_df['numInfo'].str.findall('[0-9]+\/[0-9]+\/[0-9]+')
    review_df["date"] = review_df["date"].apply(lambda x: x[0])
    
    # grabs numbers with decimal in the text, but then they have to be converted to floats 
    # the +[None,None] insures the list of numbers has length at least 2
    review_df['scores']=review_df['numInfo'].str.findall('([0-9]*[.][0-9])+').apply(lambda x: pad(list(map(float, x)),2))
    
    # here apply lambda pd.series grabs each score and those are assigned to columns 'overall' and 'difficulty'
    review_df[['overall','difficulty']] = review_df['scores'].apply(lambda x : pd.Series([x[0], x[1]]))

    # make classInfo a series of words, then pull off class and grade
    #note the use of look ahead/behind, the ? below
    review_df['classInfo']=review_df['classInfo'].str.findall('(?=\S)(\w+)(?<=\S)').apply(lambda x: pad(x,1)) 
    review_df['class'] = review_df['classInfo'].apply(lambda x : pd.Series([x[0]]))
    #note the -1 below to grab the last entry woot!
    review_df['grade'] = review_df['classInfo'].apply(lambda x : pd.Series([x[-1]]))
    
    #add name, dept, gender
    review_df['profname'],review_df['department'],review_df['gender'] = get_profData(soup)
    
    
    #add ID number
    review_df['profID'] = pd.Series(int(profID),range(0,len(review_df)))

    #add schoolID
    review_df['schoolID'] = pd.Series(schoolID,range(0,len(review_df)))
        
    review_df = review_df[['profID','profname','schoolID','department','gender','date','overall','difficulty','class','grade','textInfo']]
    review_df = pd.concat([review_df,tags],axis=1)
    review_df.reset_index(drop=True)
    
    if filename is not None:
        review_df.to_csv(filename+'.csv', mode='a', encoding='utf-8', header=False, index=False, 
                         columns=
                         ['profID',
                          'profname',
                          'gender',
                          'schoolID',
                          'department',
                         'class',
                         'date',
                         'difficulty',
                         'overall',
                         'grade',
                         'textInfo',
                         'accessible',
                         'amazing',
                         'caring',
                         'criteria',
                         'extra',
                         'feedback',
                         'few',
                         'group',
                         'hilarious',
                         'homework',
                         'inspirational',
                         'lecture',
                         'papers',
                         'participation',
                         'quiz',
                         'read',
                         'respected',
                         'skip',
                         'test',
                         'tough'])
        print('Added prof '+str(int(profID))+' to '+filename)
        
    return review_df    
    

####################################
# scrape_faculty
# scrapes one url at a time
# loads all of the ratings, clicking through the "loadMore" button
# If no ratings, it quits
# If ratings, it sends htm to massage faculty
#####################################
def scrape_faculty(myUrl,schoolID,profID,filename=None,output='DataFrame'):
    driver = webdriver.PhantomJS() 
    driver.get(myUrl)
    if 'AddRating' in driver.current_url:
        print("No ratings for prof "+str(int(profID)))
        driver.quit()
    else:     
        while True:
            try:
                
                loadMoreButton = driver.find_element_by_id("loadMore")
                loadMoreButton.click()
                print('Click')
                time.sleep(.1)
            except Exception as e:
                break
        print('Complete Prof ' + str(int(profID)) + ' Time ' + time.strftime('%X %x %Z'))
        html = driver.page_source
        driver.quit()
        if output == 'DataFrame':
            return review_df(massage_faculty(html),schoolID,profID,filename)
        elif output == 'Soup':
            return massage_faculty(html)
        else:
            print("Error. Output must be 'DataFrame' or 'Soup'.")
            

##############################
# get_reviews
# This returns all of the reviews collected in one dataframe or csv
# urls must contain schoolID 
#
# If want to export to CSV, put in filename, but without ".csv"
# schoolURLs can come from school_df(schoolID)
###############################

def get_reviews(urls,filename=None):
    #scrape all of the urls and put in df
    df=pd.concat([scrape_faculty(urls['url'][i],urls['schoolID'][i],urls['pk_id'][i],filename) for i in range(len(urls))],ignore_index=True,axis=0)
    df.reset_index(drop=True)
   
    if filename is not None:
        df.to_csv(filename+'_final.csv', encoding='utf-8', header=True, index=False,columns=
                         ['profID',
                          'profname',
                          'gender',
                          'schoolID',
                          'department',
                         'class',
                         'date',
                         'difficulty',
                         'overall',
                         'grade',
                         'textInfo',
                         'accessible',
                         'amazing',
                         'caring',
                         'criteria',
                         'extra',
                         'feedback',
                         'few',
                         'group',
                         'hilarious',
                         'homework',
                         'inspirational',
                         'lecture',
                         'papers',
                         'participation',
                         'quiz',
                         'read',
                         'respected',
                         'skip',
                         'test',
                         'tough'])
        print('File Output: '+filename+'_final'+'.csv')

        return df

####################################
# pronoun_count
# text is any text you want pronouns for
# This returns a count of pronouns by gender
# LATER: improve to add neutral pronouns, need to deal with
# fact that sometimes 'they' will refer to things that aren't prof
#####################################
def pronoun_count(text):
    pronouns={
        'she':'female',
        'shes':'female',
        'her':'female',
        'hers':'female',
        'herself':'female',
        'ms':'female',
        'mrs':'female',
        'miss':'female',
        'he':'male',
        'hes':'male',
        'his':'male',
        'him':'male',
        'himself':'male',
        'mr':'male'
    }
    genderCount = Counter({'female':0,'male':0})
    wordCount = Counter(text.split())
    for pronoun in list(pronouns.keys()):
        genderCount.update({pronouns[pronoun]:wordCount[pronoun]})
    return genderCount


####################################
# gender_ratio
# genderCount = result of pronoun_count, a list that has gender and count of pronouns
# Creates a ratio of male/female pronouns to all pronouns 
# LATER: want to also include agender/neutral as option for output
####################################
def gender_ratio(genderCount):
    if sum(list(genderCount.values())) == 0:
        return 0
    else: 
        return (genderCount['female'] - genderCount['male'])/sum(list(genderCount.values()))    
    
    
####################################
# gender_review
# reviewText = typically all text associated to a prof, but could be any text
#      run clean_text before this
# threshold = magnatude at which gender is assigned based on text
# gets the gender pronoun count of a review and uses it to decide if male or female
# returns [gender, ratio, number of pronouns, original text]
####################################
def gender_review(reviewText,threshold=0.9):
    genderCount=pronoun_count(reviewText)
    ratio = gender_ratio(genderCount)
    if abs(ratio)<threshold:
        return ['unknown',ratio,sum(list(genderCount.values()))]
    elif ratio > 0:
        return ['female',ratio,sum(list(genderCount.values()))]
    elif ratio < 0:
        return ['male',ratio,sum(list(genderCount.values()))]
    else:
        return ['unknown',ratio,sum(list(genderCount.values()))]

    
###################################################
# clean_csv
# filename = csv filename without '.csv'
# colHeader = True if has column header, list for header if doesn't
# reads and cleans up a CSV of reviews
# writes the clean CSV to file '_clean'
# also returns it as a dataframe
###################################################
def clean_csv(filename,colHeader=True): 
    # Start by reading in csv of reviews
    if colHeader==True:   #if file already has header
        review_df = pd.read_csv(filename+'.csv', encoding='iso-8859-1')
    else:              #if not, add the specified header
        review_df=pd.read_csv(filename+'.csv', header=None, names = colHeader, encoding='iso-8859-1')

    # Drop any missing profID, only a few that sneak in, think most of these are prof comments, so
    # can remove in a better way, put this on TODO list
    review_df = review_df.dropna(subset=['profID'])

    # convert profID to an integer -- should do this when loading the reviews in first place
    # another thing for TODO list!
    review_df.profID = review_df.profID.astype(int)

    # keyword list
    keyword_list = ['few','caring','criteria','skip','feedback','respected','accessible','inspirational',
                    'participation','papers','extra','lecture','read','amazing','tough','test','group',
                    'hilarious','homework','quiz']
    
    # fillna on just the keyword columns
    review_df[keyword_list] = review_df[keyword_list].fillna(0)
    
    # clean text note the use of list(map)
    review_df['cleanText'] = review_df['textInfo'].apply(lambda x:clean_text(x))
    
    return review_df



####################################
# add_gender
# should be a clean review_df dataframe with headers
# adds a gender based on reviews, with 'genderBest' being the review gender if there are pronouns, else the original gender by name from the original dataframe
# also adds count of MF pronouns for each individual review
####################################
def add_gender(review_df,threshold=0.9):
    text_df = review_df[['profID','cleanText']].groupby('profID').sum().reset_index()
    
    # first go through all of the compiled review text and use gender_review to decide gender, adding 
    #    a new column and pronoun/ratio info to dataframe
    gender_df = pd.DataFrame([gender_review(text_df['cleanText'][i],threshold) for i in range(len(text_df))],columns=['gender','ratio','pronouns'])
    text_df[['genderBest']] = gender_df[['gender']]

    #now add this to the original df
    review_df = pd.merge(review_df, text_df[['profID','genderBest']], on='profID',how="left").reset_index()

    
    # next, if the text analysis didn't give a gender, give the gender by name (the column 'gender')
    review_df['genderBest']=review_df['gender'].where(review_df['genderBest']=='unknown',review_df['genderBest'])

    # if any genders still have "mostly", eliminate that
    review_df['genderBest']=review_df['genderBest'].str.replace('mostly_','').replace('andy','unknown')
    
    # add count of pronouns in each review
    
    pronounList = [pronoun_count(review_df['cleanText'][i]) for i in range(len(review_df))]
    review_df['selfPronCnt'] = [pronounList[i][review_df['genderBest'][i]] for i in 
                                range(len(review_df))]

    
    
    return review_df


##########################################
# cleanAddGenderSchool_csv
# filename = csv filename without '.csv' note naming convention that 
#      should also be file called filename
# threshold = specify a threshold for the gender_review function
# colHeader = True if has column header, list for header if doesn't
#pass header a list to add to top of filename if it hass no header
#else if True, then file has header already
#see scrape my prof for current header contents
###########################################
def cleanAddGender_csv(filename,colHeader=True,threshold=0.9): 
    # Start by reading in csv and cleaning of reviews
    review_df = clean_csv(filename,colHeader)

    # add gender
    review_df = add_gender(review_df,threshold)
    
    # get rid of profs with no gender
    review_df = review_df[review_df['genderBest']!='unknown'].reset_index(drop=True)
    
    # add number of mentions of ones own pronouns
    # I know there has to be a better way to do this!
    pronounList = [pronoun_count(review_df['cleanText'][i]) for i in range(len(review_df))]
    review_df['selfPronCnt'] = [pronounList[i][review_df['genderBest'][i]] 
                                  for i in range(len(review_df))]
    
    # make sure schoolID is an integer
    review_df['schoolID'] = review_df['schoolID'].astype(int)
    
    # add school name and other data from schools_df
    schools_df = pd.read_csv(filename+'_proflist.csv',encoding='iso-8859-1')
    schools_df =schools_df[['schoolID','schoolname_s','schoolcity_s','schoolstate_s']].drop_duplicates()
    review_df = pd.merge(review_df,schools_df,on='schoolID',how='left').reset_index(drop=True)
    
    #write to csv
    review_df.to_csv(filename+'_cleanGender.csv', encoding='utf-8', header=True, index=False)
    
    return review_df
    

###############################################
# groupProf_df 
# review_df = clean review_df with gender
# filename = name with no .csv, '_groupProf' will be added
# it returns df compiled by prof with concatenated textInfo
# with summary information
# writes it to a csv
###############################################
def groupProf_df(review_df,filename): 
    
    # keyword list
    keyword_list = ['few','caring','criteria','skip','feedback','respected','accessible','inspirational',
                    'participation','papers','extra','lecture','read','amazing','tough','test','group',
                    'hilarious','homework','quiz']
    
    # group columns that are a sum
    prof_df = review_df[['profID', 'genderBest'] 
                      +keyword_list].groupby(['profID','genderBest']).sum().reset_index()
    
    # group columns that are a count (review count) & add to prof_df
    cnt_df = review_df[['profID','amazing']].groupby('profID').count().reset_index().rename(columns={'amazing': 'reviewcount'})
    prof_df = pd.merge(prof_df, cnt_df, on='profID')
    
    # take average of rating columns & add to prof_df
    rate_df = review_df[['profID','selfPronCnt','overall', 'difficulty']].groupby('profID').mean().reset_index()
    prof_df = pd.merge(prof_df, rate_df, on='profID')
    
    # concatenate text of reviews for each prof
    text_df = review_df[['profID','cleanText']].groupby('profID').sum().reset_index()
    prof_df = pd.merge(prof_df, text_df, on='profID')

    # write this to a csv
    prof_df.to_csv(filename+'_groupProf.csv', encoding='utf-8', header=True, index=False)
    
    return prof_df

###############################################
# yearProf_df 
# review_df = clean review_df with gender
# filename = name with no .csv, '_yearProf' will be added
# it returns df compiled by prof and year with concatenated textInfo
# with summary information
# writes it to a csv
###############################################
def yearProf_df(review_df,filename): 
    
    review_df['date']=pd.to_datetime(review_df['date'])
    review_df['year']=review_df['date'].dt.year
    
    # keyword list
    keyword_list = ['few','caring','criteria','skip','feedback','respected','accessible','inspirational',
                    'participation','papers','extra','lecture','read','amazing','tough','test','group',
                    'hilarious','homework','quiz']
    
    # group columns that are a sum
    prof_df = review_df[['profID', 'year','genderBest'] +
                        keyword_list].groupby(['profID','year','genderBest']).sum().reset_index()
    
    # group columns that are a count (review count) & add to prof_df
    cnt_df = review_df[['profID','year','amazing']].groupby(['profID','year']).count().reset_index().rename(columns={'amazing': 'reviewcount'})
    prof_df = pd.merge(prof_df, cnt_df, on=('profID','year'))
    
    # take average of rating columns & add to prof_df
    rate_df = review_df[['profID', 'year','overall', 'difficulty']].groupby(['profID','year']).mean().reset_index()
    prof_df = pd.merge(prof_df, rate_df, on=('profID','year'))
    
    # concatenate text of reviews for each prof
    text_df = review_df[['profID','year','cleanText']].groupby(['profID','year']).sum().reset_index()
    prof_df = pd.merge(prof_df, text_df, on=('profID','year'))

    # write this to a csv
    prof_df.to_csv(filename+'_yearProf.csv', encoding='utf-8', header=True, index=False)
    
    return prof_df
