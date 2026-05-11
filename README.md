# Introduction
In today’s data-driven economy, e-commerce platforms rely on scalable data infrastructures to transform raw transactional data into actionable business insights. This project simulates a production-oriented analytics platform built on top of the Olist Brazilian E-commerce dataset, combining data engineering, business modeling, and quantitative analysis.

The system is implemented using a Medallion Architecture (Bronze, Silver, Gold) in Databricks, leveraging PySpark and SQL to build reproducible and scalable data pipelines.

The dataset used for the analysis is the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, which contains transactional data including customers, orders, products, sellers, payments, geolocalization and reviews.

# Project Objective

The goal of this project is to design an end-to-end analytical framework that transforms raw marketplace data into structured metrics for evaluating seller performance and simulating operational incentives.

The project focuses on:

- Building a layered ELT pipeline using Databricks (Bronze–Silver–Gold)
- Designing a Seller Operational Index to quantify seller reliability
- Analyzing marketplace dynamics under different operational assumptions
- Exploring the relationship between seller behavior and platform economics

# About Olist

Olist is a Brazilian e-commerce startup founded in 2015 that provides a platform enabling small and medium-sized businesses to sell their products across multiple online marketplaces.

Rather than operating as a traditional retailer, Olist acts as an intermediary that simplifies the process of selling online for merchants by offering a centralized operational infrastructure.

Through its platform, Olist provides several services to sellers:

 - Marketplace integration: sellers can distribute their products across multiple e-commerce platforms (such as Amazon, Mercado Livre, or Olist Shop) without managing each marketplace separately.

 - Order management: orders from different channels are centralized in a single system, allowing automatic inventory updates and real-time notifications about new sales or status changes.

 - Customer service management: the platform centralizes communication with customers and manages returns, refunds, and complaints.

 - Marketing and analytics tools: sellers gain access to performance reports, product visibility tools, and data-driven recommendations designed to improve sales performance.

From a data perspective, these activities generate large volumes of transactional data such as orders, payments, deliveries, and customer interactions. This type of operational information is typically associated with Online Transaction Processing (OLTP) systems, where the priority is efficient transaction handling and data integrity.

## Olist Business Model

Olist generates revenue through a commission-based marketplace model. For each transaction processed through the platform, Olist charges sellers:

 - A fixed fee per item sold

 - A commission of approximately 21% of the product price

However, part of this commission is used to cover logistics costs, particularly shipping expenses. These logistics costs depend in part on seller performance, which Olist evaluates through internal operational metrics. Poor seller performance can generate delayed deliveries, cancellations, or customer dissatisfaction, which ultimately harms both platform reputation and operational profitability. 

The key challenge is therefore to design performance metrics that align seller incentives with the economic objectives of the platform. This project explores this dichotomy in the construction of a *Seller Operational Index* that captures several aspects of seller behavior. Using historical marketplace data, the goal is to evaluate how different index constructions and thresholds affect seller classification, the distribution of transport costs between seller and OLIST, and the overall marketplace revenue.

# Seller Operational Index


The Seller Operational Index is designed as a composite scoring system based on three operational dimensions: late shipments, order cancellations, and customer dissatisfaction (low review scores). The index serves two main purposes:

 - First, it acts as an incentive mechanism to influence seller behavior. Olist partially subsidizes shipping costs depending on seller performance. Sellers with higher operational reliability receive greater support in logistics costs, which encourages them to improve fulfillment quality and delivery performance.

 - Second, the index directly affects Olist’s revenue structure. Since Olist shares part of the logistics costs with sellers, the way the index is constructed determines how these costs are distributed across the marketplace. This introduces several important trade-offs.

To align the incentives of seller and platform, Sellers with better Seller Operational Index  are granted with increaseally high discounts in the percentage of the shipping cost they have to cover.

Using historical transactional data, the project evaluates how different specifications of the Seller Index affect the distribution of shipping cost burden between sellers and Olist as well as the capacity of the metric to effectively incentivice sellers to improve their operational performance.

An in dept analysis of index construction can be found in: `analysis/seller_index.ipynb`

## Calibration and Design Choices

A key part of the model is the calibration of structural parameters that ensure robustness and interpretability:

- Time window selection: definition of the observation window used to compute operational metrics
- Smoothing mechanisms: reduction of volatility in seller-level metrics to avoid distortions in low-volume sellers
- Threshold definition: establishment of minimum performance levels for meaningful classification
- Capping rules: control of extreme values to prevent outlier dominance in the scoring distribution

These design choices are critical to ensure that the index is stable, comparable across time, and economically interpretable.

## Functional Specifications (Secondary Layer)

Once the calibration framework is defined, different functional transformations are applied to map operational metrics into a final score:

- Linear specification
- Quadratic specification
- Logistic specification

## Financial impact
Finally, seller index evaluation and historical orders are linked, allowing not only to compare the financial impact of the different versions of the index, but also to easily adress how changes in calibration, thresholds, or functional forms would have influenced Olist’s profitability and cost structure under identical historical market conditions.

#  Tech Stack
- Python (Pandas, NumPy, Scikit-learn)

- PySpark

- SQL

- Databricks

- Power BI

- Git/GitHub
