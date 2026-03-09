# Introduction
In today’s data-driven economy, organizations rely on robust data platforms to transform raw transactional information into strategic insights. Modern businesses — particularly in e-commerce — generate large volumes of operational data that must be reliably stored, cleaned, and modeled before they can support decision-making.

Technologies such as Databricks and Delta Lake enable scalable and reliable data architectures by combining distributed processing with ACID-compliant storage. This allows teams to build structured data pipelines that ensure traceability, reproducibility, and analytical readiness.
However, technical infrastructure alone does not create value. The real impact comes from transforming structured data into meaningful business metrics that inform decisions around revenue growth, customer behavior, operational efficiency, and overall performance.

The objective of this project is to simulate a production-oriented data platform, progressively transforming raw operational data from a marketplace into reliable analytical models that serve as the foundation for Business Intelligence and performance monitoring. In particular, the aim to:
- Implement a layered Medallion Architecture (Bronze, Silver, Gold) in Databricks.
-	Construct a Seller Operational Index to evaluate seller diligence in terms of late shipments, cancellations and customer complaints.
-	Evaluate marketplace economics based on the Olist business model.
-	Provide actionable insights on revenue performance, transport efficiency, opportunities of vertical integration, etc.

The dataset used for the analysis is the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, which contains transactional data including customers, orders, products, sellers, payments, geolocalization and reviews.

# About Olist

Olist is a Brazilian e-commerce startup founded in 2015 that provides a platform enabling small and medium-sized businesses to sell their products across multiple online marketplaces.

Rather than operating as a traditional retailer, Olist acts as an intermediary that simplifies the process of selling online for merchants by offering a centralized operational infrastructure.

Through its platform, Olist provides several services to sellers:

 - Marketplace integration: sellers can distribute their products across multiple e-commerce platforms (such as Amazon, Mercado Livre, or Olist Shop) without managing each marketplace separately.

 - Order management: orders from different channels are centralized in a single system, allowing automatic inventory updates and real-time notifications about new sales or status changes.

 - Customer service management: the platform centralizes communication with customers and manages returns, refunds, and complaints.

 - Marketing and analytics tools: sellers gain access to performance reports, product visibility tools, and data-driven recommendations designed to improve sales performance.

From a data perspective, these activities generate large volumes of transactional data such as orders, payments, deliveries, and customer interactions. This type of operational information is typically associated with Online Transaction Processing (OLTP) systems, where the priority is efficient transaction handling and data integrity.

# Olist Business Model

Olist generates revenue through a commission-based marketplace model.

For each transaction processed through the platform, Olist charges sellers:

 - A fixed fee per item sold

 - A commission of approximately 21% of the product price

However, part of this commission is used to cover logistics costs, particularly shipping expenses. These logistics costs depend in part on seller performance, which Olist evaluates through internal operational metrics.
Poor seller performance can generate delayed deliveries, cancellations, or customer dissatisfaction, which ultimately harms both platform reputation and operational profitability.

The key challenge is therefore to design performance metrics that align seller incentives with the economic objectives of the platform. This project explores the construction of a *Seller Operational Index* that captures several aspects of seller behavior, including:

 - Late shipment rates

 - Order cancellation rates

 - Customer review signals

Sellers with better operational performance are granted with increaseally high discounts in the percentage of the shipping cost they have to cover.
Using historical marketplace data, the goal is to evaluate how different index constructions and thresholds affect seller classification, the distribution of transport costs between seller and OLIST, and the overall marketplace revenue.

Addressing this problem requires combining operational data processing with analytical modeling, which reflects the classical distinction between OLTP and OLAP systems.
