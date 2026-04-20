## 🎬 Streamflow Analytics Dashboard

End-to-end analytics pipeline for a fictional streaming platform, built to analyze **revenue, churn, and user engagement** across 5K+ users and 50K+ sessions.

---

## Live Dashboard

 [View Looker Dashboard](https://datastudio.google.com/reporting/9b0f7731-dafe-467b-97a0-312fc4ec76aa)

---

## Project Overview

This project simulates a real-world data workflow:  
- Synthetic data generation using Python  
- Data modeling and transformations in BigQuery  
- Business intelligence dashboard built in Looker Studio

The goal is to answer key business questions around:  
- Customer churn and retention  
-  Revenue performance (MRR, ARPU, LTV)  
- Content engagement and user behavior  
- User distribution and growth trends

---

## Architecture

Python (Data Generation)  
↓  
CSV Files  
↓  
BigQuery (Tables, Transformations, Views)  
↓  
Looker Studio (Dashboard)

---

## 📁 Repository Structure

├── dashboard/ # Final dashboard PDF/report  
├── data/ # Generated CSV datasets  
├── sql/ # BigQuery SQL (tables, transformations, views)  
├── requirements.txt # Python dependencies  
└── README.md

---  
  
## Tech Stack 
  
- Python → Data generation  
- BigQuery (SQL) → Data modeling & transformations  
- Looker Studio → Dashboard & visualization  
  
---  
  
## Key Metrics Tracked
  
- Monthly Recurring Revenue (MRR)  
- Average Revenue Per User (ARPU)  
- Customer Lifetime Value (LTV)  
- Churn Rate  
- Daily Active Users (DAU)  
- Session distribution (time, device, category)  
  
---  
  
## Key Insights 
  
- Premium plan drives the highest revenue despite equal user distribution  
- Mobile and Smart TV dominate user engagement  
- Kids content leads in total views and morning engagement  
- Churn rate stabilizes around ~4% monthly  
  
---

## How to Use
  
1. Clone the repo:  
   ```bash  
   git clone https://github.com/KongaraLikhith/streamflow-analytics-dashboard.git```
2.  Explore datasets in _/data_
3.  Run SQL scripts in _/sql_ inside BigQuery:
    -   Create tables
    -   Apply transformations
    -   Build analytical views
4.  Connect BigQuery to Looker Studio to recreate the dashboard
---

## Dashboard Preview

See ```/dashboard/Streamflow_Dashboard.pdf``` for a static version of the dashboard.

---

## Purpose

This project demonstrates:

-   End-to-end data pipeline design
-   SQL-based data modeling
-   BI dashboard development
-   Translating data into business insights
