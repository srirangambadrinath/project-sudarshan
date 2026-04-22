# Project Sudarshan

**Cinematic Real-Time Space Situational Awareness Dashboard**

An autonomous Indian Space Command Sentinel built as a one-person side project to showcase next-generation space intelligence capabilities.

![Sudarshan Cinematic Intro](https://github.com/srirangambadrinath/project-sudarshan/blob/main/assets/sudarshan_intro.png?raw=true)

---

🎥 Screenshots

<div align="center">

<img src="https://github.com/srirangambadrinath/project-sudarshan/blob/main/assets/sudarshan_intro.png?raw=true" width="800" alt="Sudarshan Cinematic Intro">

<br><br>

<img src="https://github.com/srirangambadrinath/project-sudarshan/blob/main/assets/dashboard_main.png?raw=true" width="800" alt="Live 3D Dashboard">

<br><br>

<img src="https://github.com/srirangambadrinath/project-sudarshan/blob/main/assets/trajectory_planner.png?raw=true" width="800" alt="Trajectory Planner">

</div>---

## 🚀 Problem Statement

With rapidly growing space debris, expanding satellite constellations, and increasing space weather threats, protecting national space assets has become critical. Traditional tools are often complex, expensive, and not specifically focused on Indian constellation priorities (GSAT, IRNSS/NavIC, RISAT, Cartosat, Pixxel, Dhruva, etc.).

**Sudarshan** delivers an accessible, visually stunning, and physics-based solution for real-time Space Situational Awareness (SSA) and mission control.

---

## ✨ Key Features

- **Cinematic Intro**: Full-screen glowing golden Sudarshan Chakra with orbital rings, particles, and thunder effects
- **Live 3D Orbit Visualizer**: Interactive Earth globe with real-time SGP4 propagation of Indian satellites
- **Collision Risk Analysis**: Physics-based conjunction detection and unified threat scoring
- **Trajectory Planner**: Animated maneuver calculations (Hohmann transfers, Δv, fuel estimation)
- **Indian Constellation Monitor**: Real-time telemetry and hazard analysis focused on national assets
- **Command Uplink**: Simple mission control interface with pipeline engagement
- **Beautiful Dark Cosmic UI**: Custom Streamlit dashboard with saffron-teal mission control aesthetics

---

## 🛠️ Tech Stack

- **Framework**: Streamlit + Custom CSS + Three.js
- **Orbital Mechanics**: SGP4 propagation with physics-based calculations
- **Data Sources**: Public CelesTrak TLEs + NOAA space weather (free APIs only)
- **Visualization**: Plotly, PyDeck, Pandas, Pillow
- **Architecture**: Modular Python (data_ingestion, dashboard, models)
- **Deployment**: Ready for Render, Railway, Streamlit Cloud

**Built entirely on ASUS TUF F15 (RTX 2050, 16GB RAM)** as a solo side project.

---

## ⚠️ Disclaimer

> **Personal side project** | Educational & demonstration purpose only  
> Not affiliated with ISRO, DRDO, or any government organization  
> Uses only public data from CelesTrak & NOAA

---

## 📋 How to Run Locally

```bash
# 1. Clone the repository
git clone https://github.com/srirangambadrinath/project-sudarshan.git
cd project-sudarshan

# 2. Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
# source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the application
streamlit run sudarshan_full_pipeline.py
