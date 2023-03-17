 ## Instructions for the assignment
* Clone this repo on your machine.
* Use your IDE of choice to complete the assignment.
* When you are done with the solution and have pushed it to the repo, [you can submit the assignment here](https://app.snapcode.review/submission_links/c0131526-1a2d-43d9-b2f0-584f64ddff19).
* Once you indicate the completion, your access to the repository will be revoked. Do make sure that you have finished the solution and pushed all the relevant code to the repo.

## Before you start
### Why complete this task?

We want to make the interview process as simple and stress-free as possible. That’s why we ask you to complete the first stage of the process from the comfort of your own home.

Your submission will help us to learn about your skills and approach. If we think you’re a good fit for our network, we’ll use your submission in the next interview stages too.

### About the task

You’ll be creating an ingestion process to ingest files containing vote data. You’ll also create a means to query the ingested data to determine outlier weeks.

There’s no time limit for this task, but we expect it to take less than 2 hours.

### Software prerequisites

The exercise requires docker in order to run, so please make sure you have this installed.

### Bootstrap solution

This repository contains a bootstrap solution that you can use to build upon. You can make any changes you like, as long as the solution can still be executed using the supplied Makefile. We’ll use the Makefile to review your solution.

To view the targets supported by the Makefile please execute make help target.

The base solution uses SQLite3 as the database, and you should treat SQLite3 as if it were a real data warehouse. The database should be saved in the root folder of the project as warehouse.db, as shown in the src/db_test.py file.

* src/ingest.py is provided as the entry point for running the ingestion process.
* src/outliers.py is provided as the entry point for running the outlier detection query.
* src/db.py is empty, but the associated test demonstrates interaction with an SQLite3 database.

### Tips on what we’re looking for

1. **Test coverage**

Your solution must have good test coverage, including common execution paths.

2. **Self-contained tests**

Your tests should be self-contained, with no dependence on being run in a specific order.

3. **Simplicity**

We value simplicity as an architectural virtue and a development practice. Solutions should reflect the difficulty of the assigned task, and shouldn’t be overly complex. We prefer simple, well tested solutions over clever solutions. 

Please avoid:

* layers of abstraction
* patterns
* custom test frameworks
* architectural features that aren’t called for
* libraries like pandas

4. **Self-explanatory code**

The solution you produce must speak for itself. Multiple paragraphs explaining the solution is a sign that the code isn’t straightforward enough to understand on its own.

5. **Dealing with ambiguity**

If there’s any ambiguity, please add this in a section at the bottom of the README. You should also make a choice to resolve the ambiguity.

## Begin the 2-part task

There are two requirements for the task. A user should be able to execute each task independently of the other. For example, ingestion shouldn’t cause the outliers query to be executed.

1. Create an ingestion process that can be run on demand to ingest files containing vote data. You should ensure that data scientists, who will be consumers of the data, do not need to consider duplicate records in their queries.
2. Create a SQL query to calculate and output which weeks are regarded as outliers based on the vote data that was ingested.
The output should contain the year, week number and the number of votes for the week. A week is classified as outlier when the total votes for the week deviate from the average votes per week for the complete dataset by more than 20%</br>  
i.e. Say the mean votes is given by $\bar{x}$ and this specific week's votes is given by $x_i$.
We want to know when $x_i$ differs from $\bar{x}$ by more than $20\%$. When this is true, then the ratio $\frac{x_i}{\bar{x}}$ must be further from $1$ by more than $0.2$. </br></br> 
$\big|1 - \frac{x_i}{\bar{x}}\big| \gt 0.2$

## Example

The sample dataset below is included in the test-resources folder and can be used when creating your tests.

Assuming a file is ingested containing the following entries:  

```
{"Id":"1","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-02T00:00:00.000"}
{"Id":"2","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"4","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"5","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-09T00:00:00.000"}
{"Id":"6","PostId":"5","VoteTypeId":"3","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"7","PostId":"3","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"8","PostId":"4","VoteTypeId":"2","CreationDate":"2022-01-16T00:00:00.000"}
{"Id":"9","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"10","PostId":"2","VoteTypeId":"2","CreationDate":"2022-01-23T00:00:00.000"}
{"Id":"11","PostId":"1","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"12","PostId":"5","VoteTypeId":"2","CreationDate":"2022-01-30T00:00:00.000"}
{"Id":"13","PostId":"8","VoteTypeId":"2","CreationDate":"2022-02-06T00:00:00.000"}
{"Id":"14","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-13T00:00:00.000"}
{"Id":"15","PostId":"13","VoteTypeId":"3","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"16","PostId":"11","VoteTypeId":"2","CreationDate":"2022-02-20T00:00:00.000"}
{"Id":"17","PostId":"3","VoteTypeId":"3","CreationDate":"2022-02-27T00:00:00.000"}
```

Then the following outliers should be output:

```
2022, 0, 1
2022, 1, 3
2022, 2, 3
2022, 5, 1
2022, 6, 1
2022, 8, 1
```

## Other Requirements

Please include instructions about your strategy and important decisions you made in the README file. You should also include answers to the following questions:

1. What kind of data quality measures would you apply to your solution in production?
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?


## Strategy
Strategy for this task was to keep code as simple and concise without being overly complex as stated in the requirements.
On ingestion the easiest way to store the line data from the file was to loop through each line in the file via readlines,
making use of the built in json library to load the data into a python dictionary, and then extracting only the values we
want into a tuple for easy insertion into the votes table via sql.

To interact with the SQL lite DB a connect method was defined inside of db.py for reuse across ingestion and
extraction logic.

Extracting the data to detect outliers was done by making use of window functions to aggregate the average and median values
needed when grouping by weeks for the outlier output. (This was very much a trial and error approach, making full use and reference
to the Window Functions Data on the SQLite Org site: https://www.sqlite.org/windowfunctions.html)
Of particular importance here were the OVER and PARTITION BY clauses.

In response to the 2 questions above:
1. What kind of data quality measures would you apply to your solution in production?
   - Correct Data profiling and consistency in terms of  data format, patterns and completeness could be vetted as a first before ingestion
   - Introducing transformations of data if needed prior to ingestion if data comes in as inconsistent from previous loads.
   - Regression testing and logging to detect bad or dirty data.
2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?
   - We would need to make use batch loading the data as opposed to line by line input.
   - A data pipeline using airflow or argo built on top of a containerised Kubernenetes clustered environment will help scale massive loads dynamically.
   - Tools such as PySpark can be introduced for large scale real time data processing.
   - Considering Hadoop and Hive for data warehousing functionality in the cloud can also help with data in the cloud.
   - Making use of temp Tables and JOINS in the SQL procedures could be of better use than the windowing functions. (To be determined, see trial and error above...)

## Note on Testing
At time of submitting my unit tests were running locally in my IDE without fail, I'm assuming my setup is wrong when running them
through the makefile, I've submitted so as not to delay the process any further and will endeavour to fix the tests on my end locally,
I'm aware access to the repo disappears after submit.

Thanks for the opportunity
Regards
Lloyd

