# CSEN1095-W20-Project

# Imports:

- numpy
- pandas
- matplotlib
- seaborn (version '0.11.0')
- re (Regular Expressions)
- plotly (px). It was used for a more sophisticated visualizations of the data such as the world scope analysis
- winsorize from scipy. It was used for removing the outliers since it uses a different technique than the traditional IQR discussed in tutorials and was worth a trial.
- Tweepy (For Analysis)

## All the datasets used in the integration are only related to the year 2015, the reason is:

- The World Happiness Dataset is only complete and valid for the year 2015.
- Data matching the World Happiness year in Life Expectancy was used.

# Feature Engineering:

- Generating a new column (Polio_HepatitisB_Avg) that tends to provide us with the total immunization percentage to the most common diseases which in turn answers the question of whether it affects the life expectancy that in turns changes the happiness score of countries (Since we already proved that both have a relation)
- Generating a new column (Population/Area) which calculates the number of population per area for a country/region which tends to answer many questions regarding whether that affects multiple attributes such as (Unemployment% or Schooling or Income Composition of Resources)
- Converting the (Status) column to (Developed) integer boolean flag (1: Developed , 0: Developing) that will help in the machine learning phase and changes the type of the column to become more applicable for other uses.
- Generating a new column (Obesity_Indicator) which is calculated using the BMI that would indicate the avg obesity for countries that would answer some questions when combined with other columns such as (Population/Area , Income , GDP) and show us a correlation of whether these affect the obesity which would then increase life expectancy and happiness or not.
- Applied one hot encoding to Subregion and Region that would inturn ease the machine learning process and help us retrieve the data faster answering the questions we further need to answer.

# ETL Pipeline
## Airflow:
### Milestone3:
- The dag file (sentimentAnalysisDaily.py) along with the data with timestamps located in the folder (data) and the file All_integrated.csv where used in Milestone 3 where the timestamps indicating the days the dag kept running and collecting tweets at, and the ipynb file (Milestone3Scrapers) was used with the All_integrated csv file to compare between the output of the sentiment analysis and the original data of (USA and Benin) as those were the countries (High , Low) used in the analysis.

### Previous Milestone:
- Make Sure To Include The Data Folder In The Location Of Script Before Running Dag
- Make Sure To Change The Location Of The Saved CSV File In The Script in store_data function
- As shown in the image below, The pipeline contains 6 stages (extract_data: Loading Dataframes, dataTidying: Renaming and dropping the unused columns, dataCleaning: Removing Outliers and Removing Null Values By Different methods, dataIntegration: Integrating the cleaned datasets together matching their country names, featureEngineering: Applying Feature Engineering on the integrated data, store_data: Extracting the data to csv) and an extra dataVisualization to include an answer to one of the intended questions.

### Data Folder Architecture:

- Happiness_Dataset Folder
- 250 countries csv
- life expectancy csv

![alt text](https://github.com/CSEN1095-W20/project-milestone-1-scrapers/blob/feature_engineering/AirlflowSucceeded.png)
