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

For each president we took a thorough look at approval ratings and the population of the 
participants in the survey. We compared the population of the participants to the
approval and disapproval ratings (Republicans, Democrats, Independents).   

We also took a look at each president's trend in approval ratings over the number 
of likes they received per tweet per year. 

####Trump Analysis
This spark file counts the number of positive and negative words used in a single tweet
then compares the sum and either classifies the tweet as negative, positive, or neutral
with these 3 classifications we display: 
total tweets, average favorites, total favorites, average retweets, total retweets

For Trump we found most of his tweets were negative but on average people liked positivity more

for Obama ... 