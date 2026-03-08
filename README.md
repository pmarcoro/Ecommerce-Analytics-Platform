# Introduction
In today’s data-driven economy, organizations rely on robust data platforms to transform raw transactional information into strategic insights. Modern businesses — particularly in e-commerce — generate large volumes of operational data that must be reliably stored, cleaned, and modeled before they can support decision-making.

Technologies such as Databricks and Delta Lake enable scalable and reliable data architectures by combining distributed processing with ACID-compliant storage. This allows teams to build structured data pipelines that ensure traceability, reproducibility, and analytical readiness.
However, technical infrastructure alone does not create value. The real impact comes from transforming structured data into meaningful business metrics that inform decisions around revenue growth, customer behavior, operational efficiency, and overall performance.

The objective of this project is to simulate a production-oriented data platform, progressively transforming raw operational data from a marketplace into reliable analytical models that serve as the foundation for Business Intelligence and performance monitoring. In particular, we aim to:
- Implement a layered Medallion Architecture (Bronze, Silver, Gold) in Databricks.
-	Construct a Seller Operational Index to evaluate seller diligence in terms of late shipments, cancellations and customer complaints.
-	Evaluate marketplace economics based on the Olist business model.
-	Provide actionable insights on revenue performance, transport efficiency, opportunities of vertical integration, etc.

The dataset used for the analysis is the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle, which contains transactional data including customers, orders, products, sellers, payments, geolocalization and reviews.
