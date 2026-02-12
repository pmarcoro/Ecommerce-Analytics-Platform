# Introduction
In today’s data-driven economy, organizations rely on robust data platforms to transform raw transactional information into strategic insights. Modern businesses — particularly in e-commerce — generate large volumes of operational data that must be reliably stored, cleaned, and modeled before they can support decision-making.

Technologies such as Databricks and Delta Lake enable scalable and reliable data architectures by combining distributed processing with ACID-compliant storage. This allows teams to build structured data pipelines that ensure traceability, reproducibility, and analytical readiness.
However, technical infrastructure alone does not create value. The real impact comes from transforming structured data into meaningful business metrics that inform decisions around revenue growth, customer behavior, operational efficiency, and overall performance.

The objective of this project is to simulate a production-oriented data platform following the Medallion Architecture (Bronze, Silver, Gold), progressively transforming raw data into reliable analytical models that serve as the foundation for Business Intelligence and performance monitoring.

The dataset used is the Olist Brazilian E-commerce dataset, which contains transactional data including customers, orders, products, sellers, payments, geolocalization and reviews.

# Project objectives
Technical Objectives:
-	Implement a layered Medallion Architecture in Databricks
-	Build clean and validated Silver tables
-	Design analytical Gold models optimized for reporting
  
Business & BI Objectives:
-	Define key e-commerce KPIs
-	Model fact and dimension tables for reporting
-	Build a Power BI dashboard using the Gold layer
-	Provide actionable insights on revenue performance, transport efficiency, seller performance, customer behavior, etc.



