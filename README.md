 **Project Sudarshan**

**Cinematic Real-Time Space Situational Awareness Dashboard**

An autonomous Indian Space Command Sentinel built as a one-person side project to demonstrate next-generation space intelligence capabilities.


##  Demo Video / Screenshots

<img width="1919" height="970" alt="image" src="https://github.com/user-attachments/assets/1fdb6a83-f31a-4ac0-9b9a-9fa39514120f" />
<img width="1887" height="815" alt="image" src="https://github.com/user-attachments/assets/cca4dd6f-4023-4c0f-a5a0-ac8bb55b058d" />
<img width="1901" height="824" alt="image" src="https://github.com/user-attachments/assets/6a18252b-c188-4011-99f6-f06dc46a75fb" />


- **Cinematic Intro**: Glowing golden Sudarshan Chakra with orbital rings and cosmic effects
- **Live 3D Globe**: Real-time SGP4 orbit propagation with Indian satellite constellation
- **Trajectory Planner**: Animated Hohmann transfers with Δv and fuel calculations
- **Hazard Analysis**: Real-time collision risk assessment for Indian assets

## Problem Statement

With growing space debris, increasing satellite constellations, and rising space weather threats, protecting national space assets has become critical. Traditional tools are often complex, expensive, or not focused on Indian constellation priorities (GSAT, IRNSS/NavIC, RISAT, Cartosat, Pixxel, Dhruva, etc.).

**Sudarshan** provides an accessible, visually stunning, and physics-based solution for real-time Space Situational Awareness (SSA) and mission control.

##  Key Features

- **Cinematic Opening Sequence**: Full-screen animated golden Sudarshan Chakra intro with particle effects and thunder
- **Live 3D Orbit Visualization**: Interactive Earth globe with SGP4 propagation of Indian satellites
- **Collision Risk Analysis**: Physics-based conjunction detection and threat scoring
- **Trajectory Planner**: Animated maneuver calculations (Hohmann transfers, Δv, fuel estimation)
- **Indian Constellation Monitor**: Real-time telemetry and hazard analysis for national assets
- **Command Uplink Interface**: Simple control panel with pipeline engagement
- **Beautiful Dark Cosmic UI**: Custom Streamlit dashboard with saffron-teal mission control aesthetics

##  Tech Stack

- **Frontend**: Streamlit + Custom CSS + Three.js (for cinematic intro and 3D globe)
- **Orbital Mechanics**: SGP4 propagation + physics-based calculations
- **Data Sources**: Public CelesTrak TLEs + NOAA space weather (free APIs only)
- **Visualization**: Plotly, PyDeck, Pandas
- **Backend**: Python with modular structure (`data_ingestion`, `dashboard`, `models`)
- **Deployment Ready**: Designed for cloud platforms (Render, Railway, etc.)

**Built entirely on ASUS TUF F15 (RTX 2050, 16GB RAM)** as a solo side project using only free public data.

##  How to Run Locally

```bash
# 1. Clone the repository
git clone https://github.com/srirangabadinath/project-sudarshan.git
cd project-sudarshan

# 2. Create and activate virtual environment
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the application
streamlit run sudarshan_full_pipeline.py
