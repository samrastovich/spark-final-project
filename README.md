# spark-final-project
**Members**:  
Nick Gatehouse 
Sam Rastovich   
Daniel Kim

## Overview
This project takes a look at the datasets of President Barack Obama
and President Donal Trump's tweet history and approval ratings. A thorough
analysis was completed on each one. 

### Datasets
#### approval_ratings
These datasets are the approval ratings of Obama and Trump, respectively

#### obama
Obama's tweet history

#### trump
Trump's tweet history

#### opinon-words
Sets of positive and negative words used for analysis

### Analysis
#### Approval Analysis
This spark file takes a deeper look into the comparison between the tweets 
and the approval ratings per president.  

For each president we took a thorough look at approval ratings and the population of the 
participants in the survey. We compared the population of the participants to the
approval and disapproval ratings (Republicans, Democrats, Independents).   

We also took a look at each president's trend in approval ratings over the number 
of likes they received per tweet per year. 

#### Obama Approval Analysis
Rather than looking for a relationship between Twitter likes
and retweets, this analysis takes a look at the relationship between the survey organization's political affiliations,
time of presidency (year), and approval/disapproval ratings.

This analysis was created by filtering all survey results by the survey organization's political affiliations,
aggregating the results of the surveys by the date, and then sorting by date.

Although we would have liked to have performed this analysis on the Trump administration, there is insufficient data.

#### Semantic Analysis of Trump then Obama Tweets
This spark file counts the number of positive and negative words used in a single tweet
then compares the sum and either classifies the tweet as negative, positive, or neutral
with these 3 classifications we display: 
total tweets, average favorites, total favorites, average retweets, total retweets

Positive.txt has 2,000 words where Negative.txt has 4,700 so there is a slight skew towards negative words.

*Source*: https://github.com/jeffreybreen/twitter-sentiment-analysis-tutorial-201107/tree/master/data/opinion-lexicon-English

##### Trump
We found most of his tweets were negative but on average people liked positivity more

*Total Negative Tweets: 14001*
- Average Favorites: 3082.58, Total Favorites: 4.3159204E7
- Average Retweets: 1131.11 Total Retweets: 1.5836683E7

*Total Positive Tweets: 4980*
- Average Favorites: 5007.9, Total Favorites: 2.4939321E7
- Average Retweets: 2052.67, Total Retweets: 1.0222288E7

*Total Neutral Tweets: 12194*
- Average Favorites: 2513.94, Total Favorites: 3.0655019E7
- Average Retweets: 1073.44, Total Retweets: 1.3089489E7

##### Obama  
We found most of his tweets were neutral tweets and on average people liked those tweets the most. 

*Total Negative Tweets: 2398*
- Average Favorites: 2836.74, Total Favorites: 6802507 
- Average Retweets: 1779.23 Total Retweets: 4266601

*Total Positive Tweets: 724*
- Average Favorites: 2804.9, Total Favorites: 2030745
- Average Retweets: 1542.6, Total Retweets: 1116844

*Total Neutral Tweets: 3612*
- Average Favorites: 2907.86, Total Favorites: 1.0503189E7
- Average Retweets: 2119.99, Total Retweets: 7657401

#### Time Of Day Analysis
This was just a simple analysis to find out what time of day most of each president's tweets were published.
Trump has an overwhelmingly large dataset than Obama, so the data may be more accurate for Trump.

These tweets do not include retweets and are purely original tweets.