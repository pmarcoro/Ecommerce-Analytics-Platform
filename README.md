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

However, part of this commission is used to cover logistics costs, particularly shipping expenses. These logistics costs depend in part on seller performance, which Olist evaluates through internal operational metrics. Poor seller performance can generate delayed deliveries, cancellations, or customer dissatisfaction, which ultimately harms both platform reputation and operational profitability. 

The key challenge is therefore to design performance metrics that align seller incentives with the economic objectives of the platform. This project explores this dichotomy in the construction of a *Seller Operational Index* that captures several aspects of seller behavior. Using historical marketplace data, the goal is to evaluate how different index constructions and thresholds affect seller classification, the distribution of transport costs between seller and OLIST, and the overall marketplace revenue.

## Seller Index 
The Seller Operational Index serves two main purposes:

 - First, it acts as an incentive mechanism to influence seller behavior. Olist partially subsidizes shipping costs depending on seller performance. Sellers with higher operational reliability receive greater support in logistics costs, which encourages them to improve fulfillment quality and delivery performance.

 - Second, the index directly affects Olist’s revenue structure. Since Olist shares part of the logistics costs with sellers, the way the index is constructed determines how these costs are distributed across the marketplace. This introduces several important trade-offs.

To align the incentives of seller and platform, Sellers with better Seller Operational Index  are granted with increaseally high discounts in the percentage of the shipping cost they have to cover.

Three main operational metrics are used to construct the index:

### A. Late Shipments

Late shipments represent the most critical operational metric for Olist.

In the marketplace logistics model, Olist acts as an intermediary between sellers and external logistics partners. As a result, Olist has a dual commitment:

 - to customers, who expect deliveries within the estimated timeframe

 - to logistics partners, who operate under predefined service agreements

When a seller dispatches a package late, several issues may arise. The logistics partner may need to adjust delivery schedules, potentially increasing operational costs. In some cases, delays can also disrupt contractual service guarantees within the logistics chain.

For these reasons, late shipments typically receive the highest weight in the seller index.

### B. Order Cancellations

Order cancellations are closely linked to inventory management and order lifecycle processes.

Olist centralizes order processing and inventory synchronization across multiple marketplaces. Maintaining accurate inventory data is therefore essential to prevent overselling and ensure consistent order fulfillment.

From a data systems perspective, these operations belong to OLTP (Online Transaction Processing) environments, where systems must guarantee reliable and consistent transaction records. Accurate inventory management requires strong consistency properties so that order creation, payment processing, and stock updates remain synchronized.

Frequent cancellations may indicate poor inventory management by sellers, which can negatively affect both customer trust and marketplace reliability.

### C. Customer Complaints and Reviews

The third dimension of seller performance is customer complaints and dissatisfaction. Customer experience in the Olist dataset is primarily measured through the *review_score*, which ranges from 1 to 5. Our aim is to capture signs of dissatisfaction events, which are particularly harmful to marketplace reputation. To do so, we analyze the proportion of reviews with a rating of 2 or less.

Review scores not only provide a numerical summary of customer satisfaction, but also a signal of the seller's after-sales service. Reviews are computed ex-post and review scores for an order_item can be changed by the customer anytime, offering a consistent snatshop of customer satisfaction and seller's after-sales service.


## Threshold sellection

One key factor when designing the index are the thresholds used for its key metrics. If the index thresholds are too permissive, sellers can easily reach high performance tiers with minimal operational effort. While this may attract more sellers and increase marketplace supply, it can also generate higher operational costs for Olist. Late shipments and cancellations may lead to additional logistics expenses, increased customer service workload, and lower customer satisfaction. Conversely, if the thresholds are too demanding, the index may fail to provide effective incentives. Sellers may perceive the targets as unattainable therefore make no effort to improve their operational performance. In addition, the prospect of facing high shipping cost without a prospect of cost reduction may discourage them from entering the marketplace in the first place, preventing profitable sales transactions for both OLIST and the sellers.

The challenge is therefore to design thresholds that are demanding yet attainable, aligning seller incentives with the platform’s operational efficiency and profitability.
