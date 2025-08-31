# Youtube-Data-Harvesting
YouTube Data Harvesting involves collecting data from multiple YouTube channels using the YouTube API. The data is first stored in MongoDB for warehousing and then migrated to SQL for analysis. With Streamlit, users can query, visualize, and analyze insights such as views, comments, and top-performing videos.

**YouTube Data Harvesting and Warehousing**
📌 **Project Overview**

This project collects and analyzes data from YouTube channels using the YouTube Data API. The harvested data is first stored in MongoDB for warehousing and then migrated to SQL databases (MySQL/PostgreSQL) for structured analysis. A Streamlit application is built to allow users to query, visualize, and analyze channel and video insights interactively.

🚀 **Features**

Fetch channel and video details using YouTube API

Store data in MongoDB (data warehouse)

Migrate structured data to SQL databases

Interactive Streamlit dashboard for querying and visualization

Answer specific business questions (e.g., top videos, most viewed channels, comments per video, etc.)

🛠️ **Tech Stack**

Python

YouTube Data API

MongoDB (NoSQL Database)

MySQL / PostgreSQL (SQL Database)

Streamlit (Web Application Framework)

📊 Example Insights

Most popular videos by views

Channel with maximum subscribers

Comments distribution across videos

Total videos uploaded by each channel

⚙️ Installation

Install dependencies

pip install -r requirements.txt


Add your YouTube API Key in the config file.

Run the Streamlit app

streamlit run app.py

🙌 Skills Gained

Python scripting

API integration

Data Warehousing with MongoDB

SQL Database Management

Interactive Visualization with Streamlit
