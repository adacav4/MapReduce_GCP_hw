# MapReduce_GCP_hw

### Input files:
- `input_files/movies1.txt`
- `input_files/movies2.txt`
- `input_files/movies3.txt`

These are just subsets of the `input_files/movies.txt` input file from https://www.geeksforgeeks.org/how-to-find-top-n-records-using-mapreduce/.


### Running MapReduce

I ran my MapReduce program on GCP, however my credits ran out. So I started a free trial on my personal account. I was wondering if I could still get an extension for my GCP coupon if I have the free trial running.

The screenshot of my account is in the file `screenshots/account.png` and the screenshots of my results of running MapReduce are in the files, `screenshots/GCP_results_1.png` and `screenshots/GCP_results_2.png`.

The code is located in three separate Java files: `src/Driver.java` for the main class, `src/least_5_Mapper.java` for the Mapper class, and `src/least_5_Reducer.java` for the Reducer class. I have also included the JAR file of my compiled MapReduce program in `LeastN.jar` (The naming of `LeastN.jar` was accidental. It is still hardcoded for "Least 5"). 
