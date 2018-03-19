# spark-final-project
**Members**:  
Sam Rastovich  
Nick Gatehouse  
Daniel Kim

##Overview
This project takes a look at the datasets of President Barack Obama
and President Donal Trump's tweet history and approval ratings. A thorough
analysis was completed on each one. 

###Datasets
####approval_ratings
These datasets are the approval ratings of Obama and Trump, respectively

####obama
Obama's tweet history

####trump
Trump's tweet history

####opinon-words
Sets of positive and negative words used for analysis

###Analysis
####Approval Analysis
This spark file takes a deeper look into the comparison between the tweets 
and the approval ratings per president.  

For Obama we took a look at the average approval rating per year, compared to the 
average like count per tweet. We wanted to see if there was a correlation between the two.

####Trump Analysis
This spark file counts the number of positive and negative words used in a single tweet
then compares the sum and either classifies the tweet as negative, positive, or neutral
with these 3 classifications we display: 
total tweets, average favorites, total favorites, average retweets, total retweets

For Trump we found most of his tweets were negative but on average people liked positivity more

for Obama ... 