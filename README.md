# Stylized-Facts-of-Emerging-Cryptocurrency Markets
This repository contains code for the OMMS Dissertation "Stylized Facts of Emerging Cryptocurrency Markets" at the University of Oxford.

For the data sets used, see the folder "data". The folder currently contains time series data for 5 cryptocurrencies: Bitcoin, Bitcoin Cash, Ethereum, Litecoin, and Ripple. For the Python script that downloaded the data, see "fetch data.py". 

The Python scripts can be used to recreate the figures in the dissertation as follows:

- "display autocorrelation plots.py" - functions to create linear autocorrelation plots for figure 3.5.
- "display leverage effect 3.py" - various functions to calculate and plot volatility measures and correlations to investigate the leverage effect. Used for figures 3.6, 3.10, and A1-A10.
- "display mean reversion.py" - functions to create data for investigation of mean reversion. Used for tables 4.1-4.6.
- "display seasonality 3.py" - functions to create various boxplots to investigate seasonality. Used for figures 3.1-3.4.
- "display small and decreasing autocorrelation.py" - functions to create autocorrelation plots for investigation of volatility clustering. Used for figures 3.7, 3.8, and 3.9.
- "display stationarity.py" - functions to calculate the augmented Dickey-Fuller and KPSS test. Used for table 2.2.
- "display summary statistics.py " - function to create summary statistics of data downloaded from gateio. Used for table 2.1.
- "display timeline plots.py" - functions to create various timeline plots. Used for figures 2.1 and 2.2.
- "display trading strategy.py" - functions to simulate trading strategy, calculate and plot profits, margins etc and save to csv files. Used for tables 4.1-4.9.
