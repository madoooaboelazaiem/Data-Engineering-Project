import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
import plotly.express as px
import plotly.express as go
from scipy.stats.mstats import winsorize
from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
# Terminal Commands: 
    # airflow webserver -p 8080
    # airflow scheduler
# Make Sure To Include The Data Folder In The Location Of The Script
# Make Sure To Change The Location Of The Saved CSV File In The Script 




# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today().strftime('%Y-%m-%d')
    }
# step 3 - instantiate DAG
dag = DAG(
    'scrapers-DAG',
    default_args=default_args,
    description='Cleaning, Tyding and Feature Engineering',
    schedule_interval='@once',
)

def extract_data(**kwargs):
    df_250_countries = pd.read_csv("data/250 Country Data.csv")
    df_life_expectancy = pd.read_csv("data/Life Expectancy Data.csv")
    df_worldHappiness_2015 = pd.read_csv("data/Happiness_Dataset/2015.csv")
    df_worldHappiness_2016 = pd.read_csv("data/Happiness_Dataset/2016.csv")
    df_worldHappiness_2017 = pd.read_csv("data/Happiness_Dataset/2017.csv")
    df_worldHappiness_2018 = pd.read_csv("data/Happiness_Dataset/2018.csv")
    df_worldHappiness_2019 = pd.read_csv("data/Happiness_Dataset/2019.csv")
    return df_250_countries,df_life_expectancy,df_worldHappiness_2015


# step 4 Define tasks
def dataTidying(**context):
    df = context['task_instance'].xcom_pull(task_ids='extract_data')
    df_250_countries = df[0]
    df_life_expectancy = df[1]
    df_worldHappiness_2015 = df[2]
    # 250 Countries Tidying
    df_250_countries.drop(df_250_countries.columns[df_250_countries.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
    df_250_countries.rename(columns={"name":"Name","region":"Region","area":"Area","gini":"Gini",'subregion':"Subregion","population":"Population"},inplace=True)
    Growth_subdata = df_250_countries['Real Growth Rating(%)']
    def findPer(x):
        if(x!=x or 'n.a' in x):
            return np.NaN
        else:
            return float(re.findall('[\d.-]*%', x)[0][:-1])
    Growth_series = Growth_subdata.apply(findPer)
    df_250_countries['Real Growth Rating(%)'] = Growth_series

    Literacy_subdata = df_250_countries['Literacy Rate(%)']
    def findPer(x):
        if(x!=x or 'n.a' in x):
            return np.NaN
        else:
            return float(re.findall('[\d.-]*%', x)[0][:-1])
    Literacy_series = Literacy_subdata.apply(findPer)
    df_250_countries['Literacy Rate(%)'] = Literacy_series

    Inflation_subdata = df_250_countries['Inflation(%)']
    def findPer(x):
        if(x!=x or 'n.a' in x):
            return np.NaN
        else:
            return float(re.findall('[\d.-]*%', x)[0][:-1])
    Inflation_series = Inflation_subdata.apply(findPer)
    df_250_countries['Inflation(%)'] = Inflation_series

    Unemployement_subdata = df_250_countries['Unemployement(%)']
    def findPer(x):
        if(x!=x or 'n.a' in x or 'N.A' in x ):
            return np.NaN
        if(not('%' in x)):
            return np.NaN
        else:
            un_refined_result = re.findall('[\s\d.-]*%', x)
            semi_refined_result = un_refined_result[0]
            while(True):
                if(semi_refined_result[-1].isnumeric()):
                    break
                semi_refined_result = semi_refined_result[:-1]
            return float(semi_refined_result)
    Unemployement_series =Unemployement_subdata.apply(findPer)
    df_250_countries['Unemployement(%)'] = Unemployement_series
    # Life Expectancy Tidying
    # Renaming thinness_1to19_years to thinness_10to19_years since it is the column representing these range of ages
    df_life_expectancy.rename(columns={" BMI ":"BMI","Life expectancy ":"Life_Expectancy","Adult Mortality":"Adult_Mortality","infant deaths":"Infant_Deaths","percentage expenditure":"Percentage_Expenditure","Hepatitis B":"HepatitisB",
                  "Measles ":"Measles"," BMI ":"BMI","under-five deaths ":"Under_Five_Deaths","Diphtheria ":"Diphtheria",
                  " HIV/AIDS":"HIV/AIDS"," thinness  1-19 years":"thinness_10to19_years"," thinness 5-9 years":"thinness_5to9_years","Income composition of resources":"Income_Composition_Of_Resources",
                   "Total expenditure":"Total_Expenditure"},inplace=True)
    return df_250_countries,df_life_expectancy,df_worldHappiness_2015


def dataCleaning(**context):
    df = context['task_instance'].xcom_pull(task_ids='dataTidying')
    df_250_countries = df[0]
    df_life_expectancy = df[1]
    df_worldHappiness_2015 = df[2]
    # 250 Countries Cleaning
    # Defining the data with null values that needs to be filled 
    country_list = df_250_countries.Name.unique()
    fill_list = ['Real Growth Rating(%)','Literacy Rate(%)','Inflation(%)','Unemployement(%)']
    for country in country_list:
        df_250_countries.loc[df_250_countries['Name'] == country,fill_list] = df_250_countries.loc[df_250_countries['Name'] == country,fill_list].interpolate()
    df_250_countries = df_250_countries.fillna(df_250_countries.mean())
    def outlier_count(col, data):
        print(15*'-' + col + 15*'-')
        q75, q25 = np.percentile(data[col], [75, 25])
        iqr = q75 - q25
        min_val = q25 - (iqr*1.5)
        max_val = q75 + (iqr*1.5)
        outlier_count = len(np.where((data[col] > max_val) | (data[col] < min_val))[0])
        outlier_percent = round(outlier_count/len(data[col])*100, 2)
        print('Number of outliers: {}'.format(outlier_count))
        print('Percent of data that is outlier: {}%'.format(outlier_percent))
    col_dict = ['Population','Area','Gini','Real Growth Rating(%)','Literacy Rate(%)','Inflation(%)','Unemployement(%)']
    for col in col_dict:
        outlier_count(col,df_250_countries)
    winsorized_Population = winsorize(df_250_countries['Population'],(0,0.12))
    winsorized_Area = winsorize(df_250_countries['Area'],(0,0.099))
    winsorized_Gini = winsorize(df_250_countries['Gini'],(0.088,0.157))
    winsorized_Real_Growth_Rating = winsorize(df_250_countries['Real Growth Rating(%)'],(0.08,0.11))
    winsorized_Literacy_Rate = winsorize(df_250_countries['Literacy Rate(%)'],(0.124,0))
    winsorized_Inflation = winsorize(df_250_countries['Inflation(%)'],(0.02,0.11))
    winsorized_Unemployement = winsorize(df_250_countries['Unemployement(%)'],(0,0.11))
    # Check number of Outliers after Winsorization for each variable.
    win_list = [winsorized_Population,winsorized_Area,winsorized_Gini,winsorized_Real_Growth_Rating,winsorized_Literacy_Rate,winsorized_Inflation,winsorized_Unemployement]
    for variable in win_list:
        q75, q25 = np.percentile(variable, [75 ,25])
        iqr = q75 - q25

        min_val = q25 - (iqr*1.5)
        max_val = q75 + (iqr*1.5)

        print("Number of outliers after winsorization : {}".format(len(np.where((variable > max_val) | (variable < min_val))[0])))
    # Adding winsorized variables to the data frame.
    df_250_countries['winsorized_Population'] = winsorized_Population
    df_250_countries['winsorized_Area'] = winsorized_Area
    df_250_countries['winsorized_Gini'] = winsorized_Gini
    df_250_countries['winsorized_Real_Growth_Rating(%)'] = winsorized_Real_Growth_Rating
    df_250_countries['winsorized_Literacy_Rate(%)'] = winsorized_Literacy_Rate
    df_250_countries['winsorized_Inflation(%)'] = winsorized_Inflation
    df_250_countries['winsorized_Unemployement(%)'] = winsorized_Unemployement
    # LifeExpectancy Cleaning
    # Defining the data with null values that needs to be filled 
    country_list = df_life_expectancy.Country.unique()
    fill_list = ['Life_Expectancy','Adult_Mortality','Alcohol','HepatitisB','BMI','Polio','Total_Expenditure','Diphtheria','GDP','Population','thinness_10to19_years','thinness_5to9_years','Income_Composition_Of_Resources','Schooling']
    for country in country_list:
        df_life_expectancy.loc[df_life_expectancy['Country'] == country,fill_list] = df_life_expectancy.loc[df_life_expectancy['Country'] == country,fill_list].interpolate()
    df_life_expectancy = df_life_expectancy.fillna(df_life_expectancy.mean())
    col_dict = ['Life_Expectancy','Adult_Mortality','Infant_Deaths','Alcohol','Percentage_Expenditure','HepatitisB','Measles','BMI','Under_Five_Deaths','Polio','Total_Expenditure','Diphtheria','HIV/AIDS','GDP','Population','thinness_10to19_years','thinness_5to9_years','Income_Composition_Of_Resources','Schooling']
    for col in col_dict:
        outlier_count(col,df_life_expectancy)
    winsorized_Life_Expectancy = winsorize(df_life_expectancy['Life_Expectancy'],(0.1,0))
    winsorized_Adult_Mortality = winsorize(df_life_expectancy['Adult_Mortality'],(0,0.1))
    winsorized_Infant_Deaths = winsorize(df_life_expectancy['Infant_Deaths'],(0,0.11))
    winsorized_Alcohol = winsorize(df_life_expectancy['Alcohol'],(0,0.1))
    winsorized_Percentage_Exp = winsorize(df_life_expectancy['Percentage_Expenditure'],(0,0.15))
    winsorized_HepatitisB = winsorize(df_life_expectancy['HepatitisB'],(0.11,0))
    winsorized_Measles = winsorize(df_life_expectancy['Measles'],(0,0.19))
    winsorized_Under_Five_Deaths = winsorize(df_life_expectancy['Under_Five_Deaths'],(0,0.135))
    winsorized_Polio = winsorize(df_life_expectancy['Polio'],(0.1,0))
    winsorized_Tot_Exp = winsorize(df_life_expectancy['Total_Expenditure'],(0,0.02))
    winsorized_Diphtheria = winsorize(df_life_expectancy['Diphtheria'],(0.105,0))
    winsorized_HIV = winsorize(df_life_expectancy['HIV/AIDS'],(0,0.19))
    winsorized_thinness_10to19_years = winsorize(df_life_expectancy['thinness_10to19_years'],(0,0.1))
    winsorized_GDP = winsorize(df_life_expectancy['GDP'],(0,0.13))
    winsorized_Population = winsorize(df_life_expectancy['Population'],(0,0.1))
    winsorized_thinness_5to9_years = winsorize(df_life_expectancy['thinness_5to9_years'],(0,0.1))
    winsorized_Income_Comp_Of_Resources = winsorize(df_life_expectancy['Income_Composition_Of_Resources'],(0.05,0))
    winsorized_Schooling = winsorize(df_life_expectancy['Schooling'],(0.025,0.01))
    # Check number of Outliers after Winsorization for each variable.
    win_list = [winsorized_Life_Expectancy,winsorized_Adult_Mortality,winsorized_Infant_Deaths,winsorized_Alcohol,
            winsorized_Percentage_Exp,winsorized_HepatitisB,winsorized_Under_Five_Deaths,winsorized_Polio,winsorized_Tot_Exp,winsorized_Diphtheria,winsorized_HIV,winsorized_GDP,winsorized_Population,winsorized_thinness_10to19_years,winsorized_thinness_5to9_years,winsorized_Income_Comp_Of_Resources,winsorized_Schooling]

    for variable in win_list:
        q75, q25 = np.percentile(variable, [75 ,25])
        iqr = q75 - q25

        min_val = q25 - (iqr*1.5)
        max_val = q75 + (iqr*1.5)

        print("Number of outliers after winsorization : {}".format(len(np.where((variable > max_val) | (variable < min_val))[0])))
    # Adding winsorized variables to the data frame.
    df_life_expectancy['winsorized_Life_Expectancy'] = winsorized_Life_Expectancy
    df_life_expectancy['winsorized_Adult_Mortality'] = winsorized_Adult_Mortality
    df_life_expectancy['winsorized_Infant_Deaths'] = winsorized_Infant_Deaths
    df_life_expectancy['winsorized_Alcohol'] = winsorized_Alcohol
    df_life_expectancy['winsorized_Percentage_Expenditure'] = winsorized_Percentage_Exp
    df_life_expectancy['winsorized_HepatitisB'] = winsorized_HepatitisB
    df_life_expectancy['winsorized_Under_Five_Deaths'] = winsorized_Under_Five_Deaths
    df_life_expectancy['winsorized_Polio'] = winsorized_Polio
    df_life_expectancy['winsorized_Total_Expenditure'] = winsorized_Tot_Exp
    df_life_expectancy['winsorized_Diphtheria'] = winsorized_Diphtheria
    df_life_expectancy['winsorized_HIV'] = winsorized_HIV
    df_life_expectancy['winsorized_GDP'] = winsorized_GDP
    df_life_expectancy['winsorized_Population'] = winsorized_Population
    df_life_expectancy['winsorized_thinness_10to19_years'] = winsorized_thinness_10to19_years
    df_life_expectancy['winsorized_thinness_5to9_years'] = winsorized_thinness_5to9_years
    df_life_expectancy['winsorized_Income_Composition_Of_Resources'] = winsorized_Income_Comp_Of_Resources
    df_life_expectancy['winsorized_Schooling'] = winsorized_Schooling
    # Happiness Dataset Cleaning
    winsorized_Family = winsorize(df_worldHappiness_2015['Family'],(0.05,0))
    winsorized_Trust = winsorize(df_worldHappiness_2015['Trust (Government Corruption)'],(0,0.1))
    winsorized_Generosity = winsorize(df_worldHappiness_2015['Generosity'],(0,0.1))
    winsorized_Dystopia = winsorize(df_worldHappiness_2015['Dystopia Residual'],(0.05,0.05))
    winsorized_Standard_Error = winsorize(df_worldHappiness_2015['Standard Error'],(0,0.1))

    df_worldHappiness_2015['Dystopia Residual'] = winsorized_Dystopia
    df_worldHappiness_2015['Family'] = winsorized_Family
    df_worldHappiness_2015['Generosity'] = winsorized_Generosity
    df_worldHappiness_2015['Trust (Government Corruption)'] = winsorized_Trust
    df_worldHappiness_2015['Standard Error'] = winsorized_Standard_Error

    return df_250_countries,df_life_expectancy,df_worldHappiness_2015

def dataIntegration(**context):
    df = context['task_instance'].xcom_pull(task_ids='dataCleaning')
    df_250_countries = df[0]
    df_life_expectancy = df[1]
    df_worldHappiness_2015 = df[2]
    df_250_countries_cleaned = df_250_countries.drop(columns=['Population', 'Area','Gini','Real Growth Rating(%)','Literacy Rate(%)','Inflation(%)','Unemployement(%)'])
    df_250_countries_cleaned = df_250_countries_cleaned.rename(columns={'winsorized_Population':'Population','winsorized_Area':'Area',
         'winsorized_Gini':'Gini','winsorized_Real_Growth_Rating(%)':'Real Growth Rating(%)','winsorized_Literacy_Rate(%)':'Literacy Rate(%)',
         'winsorized_Inflation(%)':'Inflation(%)','winsorized_Unemployement(%)':'Unemployement(%)'})
    df_250_countries_cleaned = df_250_countries_cleaned.rename(columns={'Name':'Country'})
    df_250_countries_cleaned['Country'].replace({'United Kingdom of Great Britain and Northern Ireland':'United Kingdom'},inplace=True)
    df_worldHappiness_2015 = df_worldHappiness_2015
    df_worldHappiness_2015['Country'].replace({'United States': 'United States of America', 'Vietnam': 'Viet Nam', 'Venezuela': 'Venezuela (Bolivarian Republic of)','South Korea':'Republic of Korea','Bolivia':'Bolivia (Plurinational State of)','Moldova':'Republic of Moldova','Russia':'Russian Federation','Somaliland region':'Somalia','Laos':"Lao People's Democratic Republic",'Iran':'Iran (Islamic Republic of)','Congo (Kinshasa)':'Congo','Congo (Brazzaville)':'Democratic Republic of the Congo','Tanzania':'United Republic of Tanzania','Syria':'Syrian Arab Republic'},inplace=True)
    df_all_countries_world_happiness = pd.merge(df_worldHappiness_2015, df_250_countries_cleaned, how='inner', on=['Country'])
    df_life_expectancy_2015 = df_life_expectancy[(df_life_expectancy['Year']==2015)]
    df_life_expectancy_Cleaned = df_life_expectancy.drop(columns=['Life_Expectancy', 'Adult_Mortality','Infant_Deaths','Alcohol','Percentage_Expenditure','HepatitisB','Under_Five_Deaths','Polio','Total_Expenditure','Diphtheria','HIV/AIDS','GDP','Population','thinness_10to19_years','thinness_5to9_years','Income_Composition_Of_Resources','Schooling'])
    df_life_expectancy_2015_Cleaned = df_life_expectancy_2015.drop(columns=['Life_Expectancy', 'Adult_Mortality','Infant_Deaths','Alcohol','Percentage_Expenditure','HepatitisB','Under_Five_Deaths','Polio','Total_Expenditure','Diphtheria','HIV/AIDS','GDP','Population','thinness_10to19_years','thinness_5to9_years','Income_Composition_Of_Resources','Schooling'])
    df_life_expectancy_Cleaned = df_life_expectancy_Cleaned.rename(columns={'winsorized_Life_Expectancy':'Life_Expectancy','winsorized_Adult_Mortality':'Adult_Mortality',
         'winsorized_Infant_Deaths':'Infant_Deaths','winsorized_Alcohol':'Alcohol','winsorized_Percentage_Expenditure':'Percentage_Expenditure',
         'winsorized_HepatitisB':'HepatitisB','winsorized_Under_Five_Deaths':'Under_Five_Deaths','winsorized_Polio':'Polio',
         'winsorized_Total_Expenditure':'Total_Expenditure','winsorized_Diphtheria':'Diphtheria','winsorized_HIV':'HIV/AIDS','winsorized_GDP':'GDP'
         ,'winsorized_Population':'Population','winsorized_thinness_10to19_years':'thinness_10to19_years',
         'winsorized_thinness_5to9_years':'thinness_5to9_years','winsorized_Income_Composition_Of_Resources':'Income_Composition_Of_Resources'
         ,'winsorized_Schooling':'Schooling'})
    df_life_expectancy_2015_Cleaned = df_life_expectancy_2015_Cleaned.rename(columns={'winsorized_Life_Expectancy':'Life_Expectancy','winsorized_Adult_Mortality':'Adult_Mortality',
         'winsorized_Infant_Deaths':'Infant_Deaths','winsorized_Alcohol':'Alcohol','winsorized_Percentage_Expenditure':'Percentage_Expenditure',
         'winsorized_HepatitisB':'HepatitisB','winsorized_Under_Five_Deaths':'Under_Five_Deaths','winsorized_Polio':'Polio',
         'winsorized_Total_Expenditure':'Total_Expenditure','winsorized_Diphtheria':'Diphtheria','winsorized_HIV':'HIV/AIDS','winsorized_GDP':'GDP'
         ,'winsorized_Population':'Population','winsorized_thinness_10to19_years':'thinness_10to19_years',
         'winsorized_thinness_5to9_years':'thinness_5to9_years','winsorized_Income_Composition_Of_Resources':'Income_Composition_Of_Resources'
         ,'winsorized_Schooling':'Schooling'})
    df_life_expectancy_countries = df_life_expectancy_2015_Cleaned['Country']
    countries_df = pd.merge(df_worldHappiness_2015['Country'], df_life_expectancy_countries, how='outer', indicator='Exist')
    diff_df = countries_df.loc[countries_df['Exist'] != 'both']
    df_life_expectancy_2015_Cleaned['Country'].replace({'Czechia': 'Czech Republic','The former Yugoslav republic of Macedonia':'Macedonia','United Kingdom of Great Britain and Northern Ireland':'United Kingdom'},inplace=True)
    countries_df = pd.merge(df_worldHappiness_2015['Country'], df_life_expectancy_2015_Cleaned['Country'], how='outer', indicator='Exist')
    diff_df = countries_df.loc[countries_df['Exist'] != 'both']
    df_integrated = pd.merge(df_life_expectancy_2015_Cleaned, df_worldHappiness_2015,on='Country')
    df_all_integrated = pd.merge(df_integrated,df_250_countries_cleaned,on="Country")
    return df_all_integrated

def featureEngineering(**context):
    df_all_integrated = context['task_instance'].xcom_pull(task_ids='dataIntegration')
    df_all_integrated['Polio_HepatitisB_Avg'] = df_all_integrated[['Polio', 'HepatitisB']].mean(axis=1)
    df_all_integrated = df_all_integrated.drop(columns=['Population_y', 'Region_x'])
    df_all_integrated = df_all_integrated.rename(columns={'Population_x':'Population','Region_y':'Region'})
    df_all_integrated['Population/Area'] = (df_all_integrated['Population'])/(df_all_integrated['Area'])
    df_all_integrated = pd.concat([df_all_integrated,pd.get_dummies(df_all_integrated['Status'],drop_first=True)],axis=1)
    df_all_integrated = df_all_integrated.drop('Status',axis=1)
    df_all_integrated['Obesity_Indicator'] = (df_all_integrated['BMI'] >= 25)*1
    df_all_integrated['Obesity_Indicator'] = (df_all_integrated['BMI'] >= 25)*1
    df_all_integrated = pd.get_dummies(df_all_integrated, columns=['Subregion','Region'], drop_first=False)
    return df_all_integrated
def dataVisualization(**context):
    # Is happiness of a country highly depends on the life expectancy ratio of it?
    # How likely does the both polio and hepatitisB immunization increase life expectancy ?
    # What is the likelihood that high polio immunization ratios increases the happines of a country?
    df_all_integrated = context['task_instance'].xcom_pull(task_ids='featureEngineering')
    fig = plt.figure(figsize=(10,15))

    plt.subplot(4,2,7)
    sns.scatterplot(y='Life_Expectancy',x='Happiness Score', data=df_all_integrated)
    plt.subplot(4,2,8)
    sns.regplot(y='Life_Expectancy',x='Happiness Score', data=df_all_integrated)

    plt.subplot(4,2,1)
    sns.scatterplot(y='Life_Expectancy',x='Freedom', data=df_all_integrated)
    plt.subplot(4,2,2)
    sns.regplot(y='Life_Expectancy',x='Freedom', data=df_all_integrated)


    plt.subplot(4,2,3)
    sns.scatterplot(y='Life_Expectancy',x='Polio_HepatitisB_Avg', data=df_all_integrated)
    plt.subplot(4,2,4)
    sns.regplot(y='Life_Expectancy',x='Polio_HepatitisB_Avg', data=df_all_integrated)

    plt.subplot(4,2,5)
    sns.scatterplot(y='Happiness Score',x='Polio_HepatitisB_Avg', data=df_all_integrated)
    plt.subplot(4,2,6)
    sns.regplot(y='Happiness Score',x='Polio_HepatitisB_Avg', data=df_all_integrated)

    return df_all_integrated
def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='dataVisualization')
    ts = datetime.now().timestamp()
    x = str(ts)
    x = x.split('.')
    print("Saving CSV")
    df.to_csv("/home/mado/Mado/Projects/DE/Data/"+x[0]+"scrapers.csv")


t1 = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='dataTidying',
    provide_context=True,
    python_callable=dataTidying,
    dag=dag,
)
t3 = PythonOperator(
    task_id='dataCleaning',
    provide_context=True,
    python_callable=dataCleaning,
    dag=dag,
)
t4 = PythonOperator(
    task_id='dataIntegration',
    provide_context=True,
    python_callable=dataIntegration,
    dag=dag,
)
t5 = PythonOperator(
    task_id='featureEngineering',
    provide_context=True,
    python_callable=featureEngineering,
    dag=dag,
)
t6 = PythonOperator(
    task_id='dataVisualization',
    provide_context=True,
    python_callable=dataVisualization,
    dag=dag,
)
t7 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag,
)

t1>>t2>>t3>>t4>>t5>>t6>>t7
