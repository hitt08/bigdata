(a)

//////Configuration variables//////
Date as user input --> args[3] --> "pagerank.date"
Boolean based on args[2] or final round --> "pagerank.finalIteration"

We have two stages: Filter and PageRank

//////Filter-Stage////// (Run only Once)

1. Filter job runs for only once at the start to filter the data/wiki (based on date/version/self-loops etc) and subsequently PageRank jobs runs for the number of rounds provided in the input. 
2. To facilitate Filter job we have a custom FilterInputFormat, FilterMapper and Filter Reducer.

##FilterInputFormat -- sends only required data <key: article_id><value: timestamp, article_title, main> --> Filter Mapper

##FilterMapper:
a. Perform date check -> Skips record if its timestamp/date is after the "pagerank.date"
FilterMapper Output -- <key: article_id><value: timestamp, article_title, main> --> FilterReducer

##FilterReducer:
a. Checks the latest version by keeping track of the maximum date of a particular Article_id
b. Removing self loops and "MAIN" from the value
FilterReducer Output --> <key: article_title><value: main>

//////PageRank-Stage////// (Run for each round)
This stage includes PageRankMapper and PageRankReducer

##PageRankMapper:
a. Input --> <key: article_title><value: article_title, main> OR <key: Line#, article_title, current_score, main> OR <key: line#><value: article_title, main> (Default key for InputFormat)
b. Send pagerank score of current article to the articles in its outlink (In pageRanktoOutlink method)
b.1 If it's the first round then initilize PageRank score as 1 before sending

PageRankMapper Output -- <key: article_title><value: current_score, main> OR <key: article_title, current_score> --> PageRankReducer

##PageRankReducer:
a. Sum all the scores of a particular article in for loop and after that apply the PageRank formulae to get final sum.
b. If the "pagerank.finalIteration" is set to true meaning this is the final round, then write the output as <key:title><value:current_score> otherwise forward the below output to PageRankMapper

PageRankReducer Output -- <key: article_title><value: current_score, main> --> PageRankMapper


(b)

//////Custom File Informat//////
a. Extracting only Timestamp, article_title and main for each article record to pass as the value to the FilterMapper which in turn reduces the amount of data read from disk and/or transferred over the network.
 
//////Using HashSet//////
a. In order to make a simple graph i.e to remove duplicate outlink articles
b. To remove self loops

//////Limiting the Number of Jobs//////
As we have a filter stage, so we are keeping jobs as N + 1 where N is the number of rounds of PageRank stage plus one for filter stage.

P.S Intially experimented with making groups of related articles and computing score of each group. However to avoid complexity and considering scope of this assessment, we shifted towards a more efficient approach for computing PageRank.

(c)

No modifications has been done to the original project structure/maven build files.

(d)

We performed tests for following set of inputs (on idup/uni cluster):

Num of Rounds: 5
Dates:
1. 2002-01-01T00:00:00Z (run on sample and larger)
2. 2007-01-01T00:00:00Z (run on sample)
3. 2009-01-01T00:00:00Z (run on sample and larger)

According to the "sample" results of the above tests, our output generated the correct results upto the 15th decimal point.