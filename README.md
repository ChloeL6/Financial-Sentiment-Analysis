# Stock prices, CPI and inflation

#### By [Chloe (Yen Chi) Le](https://www.linkedin.com/in/chloeycl/) 

#### Scraping data from Twitter and do sentiment analysis from certain companies. Then compare that to their companies' stock prices to find any correlation when the tweet has negative or positive sentiment.


## Technologies Used

* Python
* Jupyter
* Airflow
* BigQuery
* Looker Studio
* SQL
* Pandas
* `requirements.txt`
  

## Datasets Used

1. [US Monthly Unemployment Rate 1948](https://www.kaggle.com/datasets/tunguz/us-monthly-unemployment-rate-1948-present)
2. [U.S. Inflation Data](https://www.kaggle.com/datasets/varpit94/us-inflation-data-updated-till-may-2021)
3. [Big Tech Stock Prices](https://www.kaggle.com/datasets/evangower/big-tech-stock-prices)
4. [Bitcoin Prices Dataset](https://www.kaggle.com/datasets/yasserh/bitcoin-prices-dataset)
5. [M1, M2 and other Release Data, Monthly -in billions](https://www.federalreserve.gov/datadownload/Download.aspx?rel=H6&series=798e2796917702a5f8423426ba7e6b42&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package)
6.  [U.S. Gasoline and Diesel Retail Prices 1995-2021](https://www.kaggle.com/datasets/mruanova/us-gasoline-and-diesel-retail-prices-19952021)
7.  [Tornadoes Dataset](https://www.kaggle.com/datasets/michaelbryantds/tornadoes)

<br>

## Description

My portion of the project is to work on profiling, cleaning and transformations for the [US Monthly Unemployment Rate 1948](https://www.kaggle.com/datasets/tunguz/us-monthly-unemployment-rate-1948-present) and [U.S. Inflation Data](https://www.kaggle.com/datasets/varpit94/us-inflation-data-updated-till-may-2021), then load them to BigQuery

* Here is an overview of the ETL pipeline:
<br>

<img src="imgs/Stock_drawio.drawio.png" alt="Overview of an ETL pipeline" width="750">
<br> 

* The detailed pipeline that implemented on Airflow is:
  * Check the existence of the dataset in `BigQuery` using `BigQueryGetDatasetOperator`
  * If dataset is created and the files are detected in data folder using `FileSensor`, transformation tasks will be kicked off
  * Once transformation is done, files will be load to `BigQuery`
  * `BigQueryTableExistenceSensor` is also used to detect if `stocks` table is loaded by my teammate, if it is then save to the local file
* Below is the DAG for the pipeline:
<img src="imgs/tw3_DAG_v2.png" alt="Airflow dag" width="900"/>


## Data Visualizations:
Once the datasets were cleaned and consolidated, the team created data visualizations and analysis (using Looker Studio).

* I creates a line chart that shows the correlation among unemployment, CPI (Consumer Price Index), and inflation rates over the year. (click on image of chart to use dashboard):

<br>

[<img src="imgs/inflation.png" alt="unemployment, cpi and rates" width="750"/>](https://lookerstudio.google.com/s/r66wu-F_ZH4)

<br>

   The chart information is generated by the `Custom Query` below:  
  ```sql
WITH
-- calculate difference in cpi between current year and previous year
inflation_v1 as (
  SELECT 
  year,
  avg(cpi) avg_cpi,
  LAG(avg(cpi)) OVER (ORDER BY year) as previous_year,
  avg(cpi) - LAG(avg(cpi)) OVER (ORDER BY year) as difference_previous_year
  FROM `tech_stocks_world_events.cpi_rates`
  GROUP BY year
),

--calculate inflation
inflation as (
  SELECT year, avg_cpi, previous_year, difference_previous_year,(difference_previous_year / previous_year) * 100 inflation_rate
  FROM inflation_v1
  ORDER BY 1
),

-- calculate unemployment rate per year
avg_unemp_per_year as (
  SELECT year,((jan + feb+ mar+ apr+ may+ jun+ jul+ aug+ sep+ oct+ nov+ dec)/12) as avg_unemployment
  FROM `tech_stocks_world_events.unemployment_rates`
)

--join inflation and avg_unemp_per_year
  SELECT i.year, i.avg_cpi, i.previous_year, i.difference_previous_year, i.inflation_rate, u.avg_unemployment
  FROM inflation i
  INNER JOIN avg_unemp_per_year u
  ON i.year = u.year
  ORDER BY year
```
<br>

* I also creates a stacked combo chart that shows the average of open, close, high and low of Tech Companies' and Bitcoin stock in 2022. (click on image of chart to use dashboard):

[<img src="imgs/cl_graph_stocks.png" alt="prices for tech stocks" width="750"/>](https://lookerstudio.google.com/s/r66wu-F_ZH4)

* This is another line chart to show correlation between inflation and Tech Companies' stock prices. (click on image of chart to use dashboard):

[<img src="imgs/stock_inflation.png" alt="stock prices and inflation rates" width="750"/>](https://lookerstudio.google.com/s/r66wu-F_ZH4)

  The chart information is generated by the `Custom Query` below:
```sql
WITH
-- calculate difference in cpi between current year and previous year
inflation_v1 as (
  SELECT 
  year,
  avg(cpi) avg_cpi,
  LAG(avg(cpi)) OVER (ORDER BY year) as previous_year,
  avg(cpi) - LAG(avg(cpi)) OVER (ORDER BY year) as difference_previous_year
  FROM `tech_stocks_world_events.cpi_rates`
  GROUP BY year
),

--calculate inflation
inflation as (
  SELECT year, 'inflation' as stock_name, avg_cpi, previous_year, difference_previous_year,(difference_previous_year / previous_year) * 100 avg_price
  FROM inflation_v1
  ORDER BY 1
), 

-- calculate unemployment rate per year
stock as (
  SELECT year, avg(close) as avg_price, stock_name
  FROM `tech_stocks_world_events.stocks`
  GROUP BY 1,3
),

--union_all avg_unemp and inflation
union_all as (
  SELECT i.year, i.stock_name, i.avg_price from inflation i
  UNION ALL
  SELECT s.year, s.stock_name, s.avg_price from stock s
)

select * from union_all;
```
<br>

## Setup/Installation Requirements

* Go to https://github.com/philiprobertovich/team-week3.git to find the specific repository for this website.
* Then open your terminal. I recommend going to your Desktop directory:
    ```bash
    cd Desktop
    ```
* Then clone the repository by inputting: 
  ```bash
  git clone https://github.com/philiprobertovich/team-week3.git
  ```
* Go to the new directory or open the directory folder on your desktop:
  ```bash
  cd team-week3
  ```
* Once in the directory you will need to set up a virtual environment in your terminal:
  ```bash
  python3.7 -m venv venv
  ```
* Then activate the environment:
  ```bash
  source venv/bin/activate
  ```
* Install the necessary items with requirements.txt:
  ```bash
    pip install -r requirements.txt
  ```
* Download the necessary csv files listed in the Datasets Used section
* With your virtual environment now enabled with proper requirements, open the directory:
  ```bash
  code .
  ```
* Upon launch please update the Google Cloud client and project details to configure it to load to your project

* Once VS Code is open, then run the setup file:
  ```bash
  ./setup.sh
  ```

    The contents of the `setup.sh` include the below to install:

    1. Relevant version of python
    2. Create virtual env
    3. Installing Airflow in virtual env
    4. Requirements.txt

    ```bash
    #/bin/bash
    # this script will setup the environment and install all necessary components 

    # install/upgrade virtualenv
    python3.7 -m pip install --upgrade virtualenv

    # create and run a python3.7 virtual env
    python3.7 -m venv venv
    source venv/bin/activate
    # install/upgrade pip
    python3.7 -m pip install --upgrade pip setuptools wheel

    # install Airflow in the virtual env
    AIRFLOW_VERSION=2.3.2
    PYTHON_VERSION=3.7
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

    # pip install pypi packages
    pip install -r requirements.txt
    ```

* Then run the airflow setup file:

  ```bash
  ./airflow_setup.sh
  ```
    
    The contents of the `airflow_setup.sh` include the below to:

    1. Creating ./logs and ./plugins directories in the dsa-airflow directory 
    2. Download the `docker_compose.yaml` 
    3. Create the .env 
    4. Initialize airflow
    
```bash
    #!/bin/bash
    # Move into the dsa-airflow directory and make subdirs
    cd dsa-airflow

    # download the docker-compose.yaml and set the .env
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
    echo "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env


    # initialize airflow 
    docker-compose up airflow-init
```

* Once airflow has been initialized, use the below command line tool that allows you to initialize the rest of the Docker containers:
        ```bash
        docker-compose up
        ```

* You will need to create a file connection for the `data/` folder. To do so go to the airflow GUI and click Admin -> Connections and then create a new connection with the below config and click save:

    <img src="imgs/conn_setup.png" alt="connection setup" width="640"/>


* You will need to create a cloud connection for the `BigQueryTableExistenceSensor` folder to work:
    * Connection Id: google-cloud-default
    * Connection Type: Google BigQuery

* Once this is all setup, in the Airflow GUI 1) enable your DAG and 2) trigger it to run. From there go to your VS Code and run the below command from inside the data directory:

    ```bash
    ./get_data.sh
    ```
This will download the CSV file to your local filesystem in the data folder, which will trigger the file sensor and start the DAG.

* Once setups have been completed, you will want to be using the below commands to manage airflow and docker:
    
    1. In order to shut down hit `^Ctrl C` to stop Airflow on the local host and then run the below to stop the containers and remove old volumes:
        ```bash
        docker-compose down --volumes --remove-orphans 
        ```
    2. Use the below command line tool if you want to re-initialize the rest of the Docker containers:
        ```bash
        docker-compose up
        ```

</br>

## Known Bugs

* No known bugs

<br>

## License

MIT License

Copyright (c) 2022 Ruben Giosa, Philip Kendal, Chloe (Yen Chi) Le

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

</br>
