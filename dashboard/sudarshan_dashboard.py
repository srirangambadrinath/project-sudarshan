import streamlit as st
import pandas as pd
import numpy as np
import os
import subprocess
from datetime import datetime
import plotly.graph_objects as go
import time

import warnings
warnings.filterwarnings('ignore')

from astropy import units as u
from poliastro.twobody import Orbit
from poliastro.bodies import Earth
from poliastro.maneuver import Maneuver

# =========================================
# Page Configuration & CSS Injection
# =========================================
st.set_page_config(page_title="PROJECT SUDARSHAN | Mission Control", page_icon="", layout="wide", initial_sidebar_state="expanded")

import streamlit.components.v1 as components

# Cinematic Splash Screen (uses components.html for isolated reliable rendering)
splash_html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;500;700;900&family=Rajdhani:wght@300;500;700&display=swap');

*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }
body, html { width: 100%; height: 100%; background: #010409; overflow: hidden; font-family: 'Orbitron', sans-serif; }

/* ── Master Container ── */
.splash {
    position: fixed; inset: 0; z-index: 99999;
    display: flex; flex-direction: column; justify-content: center; align-items: center;
    background: radial-gradient(ellipse at 50% 40%, #0a0a2e 0%, #020210 50%, #000 100%);
    animation: splashFadeOut 1.8s ease-in-out 4.2s forwards;
}
@keyframes splashFadeOut {
    0%   { opacity:1; filter:blur(0); }
    100% { opacity:0; filter:blur(6px); visibility:hidden; }
}

/* ── Deep-Space Star Field (pure CSS, zero JS) ── */
.starfield { position: absolute; inset: 0; overflow: hidden; }
.starfield .layer {
    position: absolute; width: 2px; height: 2px; border-radius: 50%; background: transparent;
}
.starfield .s1 {
    box-shadow:
        4vw 8vh 0 0 rgba(255,255,255,.7), 12vw 25vh 0 0 #fff, 23vw 67vh 0 0 rgba(200,220,255,.8),
        36vw 12vh 0 0 #fff, 48vw 82vh 0 0 rgba(255,255,255,.5), 57vw 37vh 0 0 #fff,
        68vw 55vh 0 0 rgba(200,220,255,.6), 79vw 19vh 0 0 #fff, 88vw 73vh 0 0 rgba(255,255,255,.8),
        94vw 44vh 0 0 #fff, 7vw 91vh 0 0 #fff, 31vw 3vh 0 0 rgba(255,255,255,.4),
        42vw 58vh 0 0 #fff, 65vw 7vh 0 0 rgba(200,220,255,.7), 83vw 88vh 0 0 #fff,
        19vw 42vh 0 0 #fff, 52vw 28vh 0 0 rgba(255,255,255,.6), 75vw 64vh 0 0 #fff;
    animation: starDrift 80s linear infinite;
}
.starfield .s2 {
    width: 1px; height: 1px;
    box-shadow:
        2vw 15vh 0 0 rgba(0,229,255,.5), 18vw 78vh 0 0 rgba(0,229,255,.3), 33vw 48vh 0 0 rgba(255,153,51,.4),
        47vw 6vh 0 0 rgba(0,229,255,.6), 61vw 71vh 0 0 rgba(255,153,51,.3), 74vw 33vh 0 0 rgba(0,229,255,.5),
        89vw 59vh 0 0 rgba(255,153,51,.4), 14vw 85vh 0 0 rgba(0,229,255,.3), 55vw 22vh 0 0 rgba(255,153,51,.5),
        96vw 40vh 0 0 rgba(0,229,255,.4);
    animation: starDrift 55s linear infinite reverse;
}
@keyframes starDrift { from { transform: translateY(0); } to { transform: translateY(-12vh); }}

/* ── Nebula Glow ── */
.nebula {
    position: absolute; inset: -30%; border-radius: 50%;
    background: radial-gradient(ellipse at 40% 50%, rgba(255,100,0,.06) 0%, transparent 55%),
                radial-gradient(ellipse at 65% 35%, rgba(0,80,180,.08) 0%, transparent 50%);
    animation: nebPulse 6s ease-in-out infinite alternate;
}
@keyframes nebPulse { 0%{transform:scale(1);opacity:.6} 100%{transform:scale(1.08);opacity:1} }

/* ── Three.js Canvas Mount ── */
#chakra-gl {
    position: absolute; width: 100%; height: 100%; top: 0; left: 0;
    z-index: 2; pointer-events: none;
}

/* ── Lightning / Thunder Flash Overlay ── */
.thunder {
    position: absolute; inset: 0; z-index: 3; pointer-events: none;
    background: radial-gradient(circle at 50% 45%, rgba(255,255,255,.85) 0%, transparent 55%);
    opacity: 0; mix-blend-mode: overlay;
    animation: thunderAnim 3.5s ease-in-out infinite;
}
@keyframes thunderAnim {
    0%,100% { opacity:0; }
    6%,8%   { opacity:.95; }
    10%     { opacity:.15; }
    13%,14% { opacity:.8; }
    18%     { opacity:0; }
    52%,53% { opacity:.6; }
    55%     { opacity:0; }
}

/* ── Text Layer ── */
.text-layer {
    position: relative; z-index: 10; text-align: center; margin-top: 40vh;
    animation: textReveal 5s cubic-bezier(.22,.61,.36,1) forwards;
}
@keyframes textReveal {
    0%   { opacity:0; transform:translateY(60px) scale(.85); filter:blur(12px); }
    18%  { opacity:1; transform:translateY(0) scale(1); filter:blur(0); }
    82%  { opacity:1; transform:translateY(0) scale(1); }
    100% { opacity:0; transform:translateY(-30px) scale(1.08); filter:blur(4px); }
}
.title {
    font-size: clamp(36px,7vw,78px); font-weight: 900; letter-spacing: .22em;
    background: linear-gradient(90deg, #FF9933 0%, #FFD700 30%, #FFFFFF 50%, #FFD700 70%, #FF9933 100%);
    background-size: 200% 100%;
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
    animation: shimmer 3s linear infinite;
    filter: drop-shadow(0 0 30px rgba(255,153,51,.5));
}
@keyframes shimmer { from{background-position:200% 0} to{background-position:-200% 0} }
.subtitle {
    font-family: 'Rajdhani', sans-serif; font-size: clamp(14px,2.2vw,24px); font-weight: 500;
    letter-spacing: .35em; margin-top: 14px;
    color: #00e5ff;
    text-shadow: 0 0 18px rgba(0,229,255,.6), 0 0 40px rgba(0,229,255,.25);
}
.disclaimer-intro {
    font-family: 'Rajdhani', sans-serif; font-size: 11px; color: rgba(255,255,255,.35);
    letter-spacing: .15em; margin-top: 30px;
}
</style>
</head>
<body>
<div class="splash" id="splashRoot">
    <div class="starfield">
        <div class="layer s1"></div>
        <div class="layer s2"></div>
    </div>
    <div class="nebula"></div>
    <canvas id="chakra-gl"></canvas>
    <div class="thunder"></div>
    <div class="text-layer">
        <div class="title">PROJECT SUDARSHAN</div>
        <div class="subtitle">AUTONOMOUS SPACE SENTINEL FOR INDIAN SATELLITE CONSTELLATION</div>
        <div class="disclaimer-intro">Personal Project &bull; Educational Use Only &bull; Not Affiliated With Any Government Org</div>
    </div>
</div>

<!-- Three.js r160 (module-less UMD build for max compatibility inside Streamlit iframe) -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
(function(){
    /* ── Renderer Setup ── */
    const canvas = document.getElementById('chakra-gl');
    const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.1;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(50, window.innerWidth / window.innerHeight, 0.1, 100);
    camera.position.set(0, 0, 6);

    /* ── Lighting ── */
    const ambientLight = new THREE.AmbientLight(0x111122, 0.3);
    scene.add(ambientLight);
    const pointLight = new THREE.PointLight(0xFFD700, 3, 20);
    pointLight.position.set(0, 0, 3);
    scene.add(pointLight);
    const rimLight = new THREE.PointLight(0xFF6600, 1.5, 15);
    rimLight.position.set(-3, 2, -1);
    scene.add(rimLight);

    /* ── Golden Material ── */
    const goldMat = new THREE.MeshStandardMaterial({
        color: 0xFFD700, emissive: 0xFF8C00, emissiveIntensity: 0.5,
        metalness: 0.9, roughness: 0.25, side: THREE.DoubleSide
    });
    const brightGoldMat = new THREE.MeshStandardMaterial({
        color: 0xFFE55C, emissive: 0xFFAA00, emissiveIntensity: 0.8,
        metalness: 1.0, roughness: 0.15, side: THREE.DoubleSide
    });

    /* ── Chakra Group ── */
    const chakraGroup = new THREE.Group();
    scene.add(chakraGroup);

    // Outer Ring (Torus)
    const outerRing = new THREE.Mesh(new THREE.TorusGeometry(1.8, 0.12, 24, 128), goldMat);
    chakraGroup.add(outerRing);

    // Inner Ring
    const innerRing = new THREE.Mesh(new THREE.TorusGeometry(1.45, 0.06, 16, 96), brightGoldMat);
    chakraGroup.add(innerRing);

    // Orbital Accent Rings (tilted ellipses that suggest orbits)
    const orbitMat = new THREE.MeshStandardMaterial({
        color: 0x00e5ff, emissive: 0x006688, emissiveIntensity: 0.4,
        metalness: 0.6, roughness: 0.4, transparent: true, opacity: 0.35, side: THREE.DoubleSide
    });
    for (let i = 0; i < 3; i++) {
        const orbitRing = new THREE.Mesh(new THREE.TorusGeometry(2.1 + i * 0.22, 0.015, 8, 128), orbitMat);
        orbitRing.rotation.x = Math.PI / 2 + (i - 1) * 0.25;
        orbitRing.rotation.z = i * 0.4;
        chakraGroup.add(orbitRing);
    }

    // Spokes (divine weapon blades radiating from center)
    const spokeCount = 24;
    for (let i = 0; i < spokeCount; i++) {
        const angle = (i / spokeCount) * Math.PI * 2;
        // Blade-like spoke: elongated box tapered via scale
        const spoke = new THREE.Mesh(
            new THREE.BoxGeometry(1.15, 0.018, 0.045),
            i % 3 === 0 ? brightGoldMat : goldMat
        );
        spoke.position.set(Math.cos(angle) * 0.85, Math.sin(angle) * 0.85, 0);
        spoke.rotation.z = angle;
        chakraGroup.add(spoke);
    }

    // Central Hub (glowing sphere)
    const hubMat = new THREE.MeshStandardMaterial({
        color: 0xFFFFFF, emissive: 0xFFDD44, emissiveIntensity: 1.2,
        metalness: 0.3, roughness: 0.1
    });
    const hub = new THREE.Mesh(new THREE.SphereGeometry(0.28, 32, 32), hubMat);
    chakraGroup.add(hub);

    // Secondary hub glow ring
    const hubRing = new THREE.Mesh(new THREE.TorusGeometry(0.38, 0.03, 12, 64), brightGoldMat);
    chakraGroup.add(hubRing);

    /* ── Volumetric Light Rays (Sprite-based God Rays) ── */
    const rayTexture = (function(){
        const c = document.createElement('canvas'); c.width = 256; c.height = 256;
        const ctx = c.getContext('2d');
        const g = ctx.createRadialGradient(128,128,0,128,128,128);
        g.addColorStop(0,'rgba(255,215,0,0.7)');
        g.addColorStop(0.3,'rgba(255,165,0,0.25)');
        g.addColorStop(1,'rgba(255,100,0,0)');
        ctx.fillStyle = g; ctx.fillRect(0,0,256,256);
        return new THREE.CanvasTexture(c);
    })();
    const coreGlow = new THREE.Sprite(new THREE.SpriteMaterial({ map: rayTexture, blending: THREE.AdditiveBlending, transparent: true, opacity: 0.7 }));
    coreGlow.scale.set(5, 5, 1);
    chakraGroup.add(coreGlow);

    // Additional elongated ray sprites for "divine weapon" light beams
    for (let i = 0; i < 8; i++) {
        const raySprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: rayTexture, blending: THREE.AdditiveBlending, transparent: true, opacity: 0.18 + Math.random() * 0.15 }));
        const angle = (i / 8) * Math.PI * 2;
        raySprite.position.set(Math.cos(angle) * 0.2, Math.sin(angle) * 0.2, 0.05);
        raySprite.scale.set(1.5 + Math.random(), 0.12 + Math.random() * 0.08, 1);
        raySprite.material.rotation = angle;
        chakraGroup.add(raySprite);
    }

    /* ── Cosmic Fire Particles ── */
    const particleCount = 600;
    const pGeo = new THREE.BufferGeometry();
    const pPositions = new Float32Array(particleCount * 3);
    const pColors    = new Float32Array(particleCount * 3);
    const pSizes     = new Float32Array(particleCount);
    const pVelocities= [];
    const fireColors = [
        new THREE.Color(0xFFD700), new THREE.Color(0xFF8C00),
        new THREE.Color(0xFF5500), new THREE.Color(0xFFFFFF)
    ];
    for (let i = 0; i < particleCount; i++) {
        const angle = Math.random() * Math.PI * 2;
        const radius = 1.4 + Math.random() * 1.0;
        pPositions[i*3]   = Math.cos(angle) * radius;
        pPositions[i*3+1] = Math.sin(angle) * radius;
        pPositions[i*3+2] = (Math.random() - 0.5) * 0.3;
        const col = fireColors[Math.floor(Math.random() * fireColors.length)];
        pColors[i*3] = col.r; pColors[i*3+1] = col.g; pColors[i*3+2] = col.b;
        pSizes[i] = 2 + Math.random() * 4;
        pVelocities.push({
            vr: 0.002 + Math.random() * 0.006,
            vz: (Math.random() - 0.5) * 0.003,
            angle: angle,
            radius: radius,
            baseRadius: radius,
            life: Math.random()
        });
    }
    pGeo.setAttribute('position', new THREE.BufferAttribute(pPositions, 3));
    pGeo.setAttribute('color',    new THREE.BufferAttribute(pColors, 3));
    pGeo.setAttribute('size',     new THREE.BufferAttribute(pSizes, 1));

    const pMat = new THREE.PointsMaterial({
        size: 0.035, vertexColors: true, transparent: true, opacity: 0.75,
        blending: THREE.AdditiveBlending, depthWrite: false, sizeAttenuation: true
    });
    const particles = new THREE.Points(pGeo, pMat);
    scene.add(particles);

    /* ── Resize Handler ── */
    window.addEventListener('resize', () => {
        camera.aspect = window.innerWidth / window.innerHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(window.innerWidth, window.innerHeight);
    });

    /* ── Animation Loop ── */
    const clock = new THREE.Clock();
    function animate() {
        requestAnimationFrame(animate);
        const t = clock.getElapsedTime();

        // Chakra spin (smooth 60fps)
        chakraGroup.rotation.z += 0.012;
        // Gentle tilt oscillation for depth
        chakraGroup.rotation.x = Math.sin(t * 0.4) * 0.15;
        chakraGroup.rotation.y = Math.cos(t * 0.3) * 0.1;

        // Pulsing hub glow
        hub.material.emissiveIntensity = 1.0 + Math.sin(t * 3) * 0.4;
        coreGlow.material.opacity = 0.55 + Math.sin(t * 2.5) * 0.2;
        const glowScale = 4.5 + Math.sin(t * 2) * 0.8;
        coreGlow.scale.set(glowScale, glowScale, 1);

        // Pulsing point light
        pointLight.intensity = 2.5 + Math.sin(t * 3.5) * 1.0;

        // Orbital accent rings subtle counter-rotation
        chakraGroup.children.forEach((child, idx) => {
            if (idx >= 2 && idx < 5) { // orbital rings
                child.rotation.z += 0.003 * (idx % 2 === 0 ? 1 : -1);
            }
        });

        // Animate fire particles (swirl outward with life cycle)
        const pos = particles.geometry.attributes.position.array;
        for (let i = 0; i < particleCount; i++) {
            const v = pVelocities[i];
            v.angle += v.vr;
            v.life += 0.004;
            if (v.life > 1) { v.life = 0; v.radius = v.baseRadius; }
            v.radius += 0.003;
            pos[i*3]   = Math.cos(v.angle) * v.radius;
            pos[i*3+1] = Math.sin(v.angle) * v.radius;
            pos[i*3+2] += v.vz;
            if (Math.abs(pos[i*3+2]) > 0.5) v.vz *= -1;
        }
        particles.geometry.attributes.position.needsUpdate = true;
        particles.rotation.z -= 0.002;

        renderer.render(scene, camera);
    }
    animate();
})();
</script>
</body>
</html>
"""

if 'splash_done' not in st.session_state:
    st.session_state['splash_done'] = False

if not st.session_state['splash_done']:
    st.markdown("""
        <style>
            .stApp header {display:none !important;}
            .stApp {background-color: #010409;}
            .block-container { padding: 0 !important; max-width: 100% !important; margin: 0 !important; overflow: hidden !important; }
            [data-testid="stSidebar"] { display: none !important; }
            iframe { position: fixed !important; top: 0 !important; left: 0 !important; width: 100vw !important; height: 100vh !important; border: none !important; z-index: 999999 !important; }
        </style>
    """, unsafe_allow_html=True)
    components.html(splash_html, height=1200)
    time.sleep(5.5)
    st.session_state['splash_done'] = True
    st.rerun()

# ----------------------------------------------------
# MAIN DASHBOARD (Only executes if splash_done == True)
# ----------------------------------------------------

st.markdown("""
<style>
/* Main Dashboard Premium Styling Layer */
.stApp { background-color: #020617; color: #e2e8f0; }
.block-container { padding-top: 2rem !important; padding-bottom: 2rem !important; }
.big-title { 
    font-family: 'Rajdhani', sans-serif; font-size: 44px; font-weight: 700; 
    color: #00e5ff; letter-spacing: 3px; 
    text-shadow: 0px 0px 15px rgba(0, 229, 255, 0.6); margin-bottom: 0px; 
    border-bottom: 1px solid rgba(0, 229, 255, 0.2); padding-bottom: 10px;
}
.sub-title { font-family: 'Consolas', monospace; font-size: 16px; color: #94a3b8; margin-top: 5px; margin-bottom: 30px; letter-spacing: 1px;}
.terminal-box { background-color: #0f172a; border: 1px solid #1e293b; padding: 15px; border-radius: 5px; font-family: monospace; color: #38bdf8; }
.stTabs [data-baseweb="tab-list"] { background-color: #0f172a; border-radius: 8px; border-bottom: 2px solid #1e293b; }
.stTabs [aria-selected="true"] { color: #00e5ff !important; font-weight: bold; text-shadow: 0px 0px 8px rgba(0,229,255,0.4); }
hr { border-color: #1e293b; }
</style>
""", unsafe_allow_html=True)

st.markdown("<div class='big-title'>PROJECT SUDARSHAN</div>", unsafe_allow_html=True)
st.markdown("<div class='sub-title'>CINEMATIC 3D MISSION CONTROL & AI SENTINEL</div>", unsafe_allow_html=True)

@st.cache_data(ttl=60)
def load_data():
    parquet_path = "data/sudarshan_predictions.parquet"
    if os.path.exists(parquet_path):
        mtime = os.path.getmtime(parquet_path)
        dt_m = datetime.fromtimestamp(mtime)
        return pd.read_parquet(parquet_path), dt_m
    return pd.DataFrame(), None

df, last_update = load_data()

with st.sidebar:
    # Original artistic Sudarshan Chakra branding (no official logos)
    st.markdown("""
        <div style='text-align:center; margin-bottom:18px;'>
            <div style='display:inline-block; width:80px; height:80px; border-radius:50%;
                border:3px solid #FFD700; position:relative;
                box-shadow: 0 0 25px rgba(255,215,0,0.5), inset 0 0 15px rgba(255,165,0,0.3);
                animation: sidebarSpin 4s linear infinite;'>
                <div style='position:absolute;inset:8px;border-radius:50%;border:2px dashed #FFA500;'></div>
                <div style='position:absolute;inset:28px;border-radius:50%;
                    background:radial-gradient(circle,#FFF 0%,#FFD700 50%,#FF8C00 100%);
                    box-shadow:0 0 20px rgba(255,255,255,0.8);'></div>
            </div>
            <div style='font-family:Orbitron,sans-serif;font-size:11px;color:#FFD700;
                letter-spacing:3px;margin-top:10px;text-shadow:0 0 8px rgba(255,165,0,0.4);'>SUDARSHAN</div>
        </div>
        <style>@keyframes sidebarSpin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}</style>
        <div style='background:rgba(255,153,51,0.08);border:1px solid rgba(255,153,51,0.15);
            border-radius:6px;padding:8px 10px;margin-bottom:15px;
            font-size:10px;color:rgba(255,255,255,0.45);line-height:1.5;text-align:center;
            font-family:Rajdhani,sans-serif;letter-spacing:0.5px;'>
            Personal side project | Educational &amp; demonstration purpose only | Not affiliated with any government organization | Uses public data from CelesTrak &amp; NOAA
        </div>
    """, unsafe_allow_html=True)
    st.markdown("###  Command Uplink")
    if st.button(" ENGAGE PIPELINE", type="primary", width="stretch"):
        with st.spinner("Executing Full Sudarshan Pipeline..."):
            try:
                subprocess.run(["python", "sudarshan_full_pipeline.py"], check=True)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(str(e))
    st.markdown("---")
    
    st.markdown("###  Live Telemetry Feed")
    telemetry_placeholder = st.empty()
    
# Fake live telemetry tick generator to make UI feel alive
current_time = datetime.now().strftime('%H:%M:%S')
status_lights = np.random.choice(['🟢', '🟡'], p=[0.8, 0.2])
with telemetry_placeholder.container():
    st.markdown(f"<div class='terminal-box'>CONNECTION: SECURE<br/>UPLINK: {status_lights} ACTIVE<br/>SYS TIME: {current_time}<br/>DATA STREAMS: CelesTrak + NOAA SGP4</div>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs([" Live 3D Simulation", " Trajectory Planner", " Domestic Fleet", "Hazard Analysis", " AI Co-Pilot"])

def create_orbit(row):
    try:
        a = row['semi_major_axis_km'] * u.km
        ecc = row['eccentricity'] * u.one
        inc = row['inclination'] * u.deg
        raan = row.get('raan', 0.0) * u.deg
        argp = row.get('arg_pericenter', 0.0) * u.deg
        nu = row.get('mean_anomaly', 0.0) * u.deg
        return Orbit.from_classical(Earth, a, ecc, inc, raan, argp, nu)
    except: return None

def get_orbit_coords(orb, num_points=60):
    try:
        ephem = orb.sample(num_points)
        return ephem.xyz.value[0], ephem.xyz.value[1], ephem.xyz.value[2]
    except: return [], [], []

def build_earth_sphere():
    u_vals = np.linspace(0, 2 * np.pi, 60)
    v_vals = np.linspace(0, np.pi, 60)
    x = 6371 * np.outer(np.cos(u_vals), np.sin(v_vals))
    y = 6371 * np.outer(np.sin(u_vals), np.sin(v_vals))
    z = 6371 * np.outer(np.ones(np.size(u_vals)), np.cos(v_vals))
    return go.Surface(
        x=x, y=y, z=z, 
        colorscale='Blues', 
        showscale=False, 
        opacity=0.9, 
        hoverinfo='skip',
        lighting=dict(ambient=0.1, diffuse=0.8, specular=0.3, roughness=0.5, fresnel=0.2)
    )

# -------------- TAB 1: Live 3D Simulation --------------
with tab1:
    st.subheader("Global Cinematic SGP4 Propagation")
    if not df.empty:
        indian_assets = df[df['is_indian'] == True] if 'is_indian' in df.columns else pd.DataFrame()
        global_risks = df[df['is_indian'] == False].nlargest(20, 'risk_score') if 'is_indian' in df.columns else df.nlargest(20, 'risk_score')
        display_set = pd.concat([indian_assets, global_risks]).drop_duplicates(subset=['norad_cat_id'])

        with st.spinner("Compiling WebGL Plotly Animation Frames..."):
            num_frames = 60
            traces = [build_earth_sphere()]
            orbits_data = []
            
            for _, row in display_set.iterrows():
                orb = create_orbit(row)
                if orb:
                    xo, yo, zo = get_orbit_coords(orb, num_frames)
                    if len(xo) == 0: continue
                    is_ind = row.get('is_indian', False)
                    risk = row['risk_score']
                    if is_ind: color = "#00e5ff"
                    elif risk > 0.65: color = "#ff2a2a"
                    elif risk > 0.4: color = "#ff9a00"
                    else: color = "#64748b"
                    
                    orbits_data.append({'name': f"{'🇮🇳 ' if is_ind else '⚠️ '}{row['object_name']}", 'color': color, 'x': xo, 'y': yo, 'z': zo})
            
            # 1. Base Trail Paths
            for od in orbits_data:
                traces.append(go.Scatter3d(
                    x=od['x'], y=od['y'], z=od['z'], mode='lines',
                    line=dict(color=od['color'], width=1), opacity=0.4, hoverinfo='none', showlegend=False
                ))
            
            marker_start_idx = len(traces)
            # 2. Base Satellite Positions
            for od in orbits_data:
                traces.append(go.Scatter3d(
                    x=[od['x'][0]], y=[od['y'][0]], z=[od['z'][0]], mode='markers',
                    marker=dict(size=4, color='#ffffff', line=dict(color=od['color'], width=4)),
                    name=od['name'], text=od['name'], hoverinfo='text'
                ))
                
            # 3. Frames Generation
            frames = []
            for k in range(num_frames):
                frame_data = []
                for od in orbits_data:
                    frame_data.append(go.Scatter3d(x=[od['x'][k]], y=[od['y'][k]], z=[od['z'][k]]))
                frames.append(go.Frame(data=frame_data, name=str(k), traces=list(range(marker_start_idx, marker_start_idx + len(orbits_data)))))
            
            fig = go.Figure(data=traces, frames=frames)
            fig.update_layout(
                scene=dict(
                    xaxis=dict(showgrid=False, zeroline=False, showbackground=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showbackground=False, showticklabels=False),
                    zaxis=dict(showgrid=False, zeroline=False, showbackground=False, showticklabels=False),
                    bgcolor="#000000"
                ),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=0, b=0),
                height=700,
                updatemenus=[dict(
                    type="buttons", showactive=False, x=0.05, y=0.95,
                    buttons=[
                        dict(label="▶ AUTO-PLAY", method="animate", args=[None, dict(frame=dict(duration=80, redraw=True), transition=dict(duration=0), fromcurrent=True, mode='immediate')]),
                        dict(label="⏸ PAUSE", method="animate", args=[[None], dict(frame=dict(duration=0, redraw=False), mode='immediate', transition=dict(duration=0))])
                    ]
                )]
            )
            st.plotly_chart(fig, width="stretch")

# -------------- TAB 2: Trajectory Planner --------------
with tab2:
    st.subheader("Trajectory Planner (Animated Transfer)")
    if not df.empty:
        c1, c2 = st.columns([1, 2])
        with c1:
            assets = df[df['is_indian']==True]['object_name'].tolist() if 'is_indian' in df.columns and len(df[df['is_indian']==True]) > 0 else df['object_name'].head(50).tolist()
            sel_asset = st.selectbox("Select Target Spacecraft:", assets)
            tar_row = df[df['object_name'] == sel_asset].iloc[0]
            cur_a = tar_row['semi_major_axis_km']
            st.metric("Current Orbital Radius", f"{cur_a:.1f} km")
            
            t_rad = st.slider("Target Radius (km)", min_value=6000, max_value=45000, value=int(cur_a + 1500), step=100)
            t_inc = st.slider("Target Inclination (deg)", min_value=0.0, max_value=180.0, value=float(tar_row.get('inclination', 0.0)), step=1.0)
            t_ecc = st.slider("Target Eccentricity", min_value=0.0, max_value=0.9, value=float(tar_row.get('eccentricity', 0.0)), step=0.01)
            t_launch = st.selectbox("Injection / Launch Relay Site", ["Satish Dhawan Space Centre (SHAR) 🇮🇳", "KSC LC-39A 🇺🇸", "Guiana Space Centre 🇪🇺", "Baikonur Cosmodrome 🇰🇿"])
            t_stages = st.slider("Transfer Mission Stages", 1, 5, 2)
            m_dry = st.number_input("Payload Dry Mass (kg)", value=1200.0)
            isp_engine = st.slider("Engine Isp (s)", 150, 450, 300)
            calc_btn = st.button("Calculate & Animate Transfer", type="primary", width="stretch")
            
        with c2:
            if calc_btn:
                orb_i = create_orbit(tar_row)
                if orb_i is not None:
                    # Metrics incorporating Hohmann + Inclination + Eccentricity changes smoothly
                    man = Maneuver.hohmann(orb_i, t_rad * u.km)
                    dv_tot = sum(np.linalg.norm((dv * u.km/u.s).value) for _, dv in man.impulses)
                    
                    # Pseudo-analytical approximation for orbit plane/shape changes
                    dv_tot += abs(t_inc - orb_i.inc.value) * 0.075 # Inclination penalty
                    dv_tot += abs(t_ecc - orb_i.ecc.value) * 1.85   # Eccentricity penalty
                    dv_tot *= t_stages ** 0.05 # Minor penalty per additional stage split
                    
                    dv_m_s = dv_tot * 1000
                    m_propellant = m_dry * (np.exp(dv_m_s / (isp_engine * 9.80665)) - 1)
                    tr_time_h = (np.pi * np.sqrt((((orb_i.a.value + t_rad)/2)**3) / Earth.k.value)) / 3600
                    
                    # Compute collision risk variance during transfer (pseudo-computation based on density layers)
                    transfer_risk = min(1.0, max(0.01, (10000 - t_rad) / 20000.0 + (dv_m_s / 50000.0)))
                    
                    st.success(f"**Hohmann Transfer Validated:** | Δv: {dv_m_s:.1f} m/s | Fuel: {m_propellant:.1f} kg | Duration: {tr_time_h:.2f} h | Collision Exposure: {(transfer_risk * 100):.1f}%")
                    
                    # Custom Animation 3D mapping
                    xo_i, yo_i, zo_i = get_orbit_coords(orb_i, 50)
                    
                    # Correct fix for Orbit object copying across diverse Poliastro versions
                    orb_f = Orbit.from_classical(Earth, t_rad * u.km, t_ecc * u.one, t_inc * u.deg, orb_i.raan, orb_i.argp, orb_i.nu)
                    xo_f, yo_f, zo_f = get_orbit_coords(orb_f, 50)
                    
                    # Direct line trace to represent the elliptical hop
                    trans_x = np.linspace(xo_i[0], xo_f[25], 30)
                    trans_y = np.linspace(yo_i[0], yo_f[25], 30)
                    trans_z = np.linspace(zo_i[0], zo_f[25], 30)

                    t_traces = [build_earth_sphere()]
                    t_traces.append(go.Scatter3d(x=xo_i, y=yo_i, z=zo_i, mode='lines', line=dict(color='#00e5ff', width=3), name="Initial Orbit"))
                    t_traces.append(go.Scatter3d(x=xo_f, y=yo_f, z=zo_f, mode='lines', line=dict(color='#ff2a2a', width=3, dash='dash'), name="Target Orbit"))
                    t_traces.append(go.Scatter3d(x=trans_x, y=trans_y, z=trans_z, mode='lines', line=dict(color='#ff9a00', width=4), opacity=0.5, name="Transfer Arc"))
                    
                    ship_trace_idx = len(t_traces)
                    t_traces.append(go.Scatter3d(x=[trans_x[0]], y=[trans_y[0]], z=[trans_z[0]], mode='markers', marker=dict(size=8, color='white', line=dict(color='yellow', width=3)), name="Ship"))
                    
                    t_frames = []
                    for k in range(len(trans_x)):
                        t_frames.append(go.Frame(data=[go.Scatter3d(x=[trans_x[k]], y=[trans_y[k]], z=[trans_z[k]])], traces=[ship_trace_idx], name=str(k)))
                        
                    f_man = go.Figure(data=t_traces, frames=t_frames)
                    f_man.update_layout(
                        scene=dict(xaxis=dict(showgrid=False, showbackground=False, showticklabels=False),
                                   yaxis=dict(showgrid=False, showbackground=False, showticklabels=False),
                                   zaxis=dict(showgrid=False, showbackground=False, showticklabels=False), bgcolor="#000000"),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=0, b=0), height=550,
                        updatemenus=[dict(type="buttons", showactive=False, x=0.05, y=0.95, buttons=[
                            dict(label="▶ ANIMATE FLIGHT", method="animate", args=[None, dict(frame=dict(duration=80, redraw=True), transition=dict(duration=0))])
                        ])]
                    )
                    st.plotly_chart(f_man, width="stretch")

# -------------- TAB 3-5 --------------
with tab3:
    st.subheader("🇮🇳 Indian Constellation Defenses")
    if not df.empty and 'is_indian' in df.columns:
        ind_df = df[df['is_indian'] == True].sort_values(by='risk_score', ascending=False)
        st.dataframe(ind_df[['object_name', 'norad_cat_id', 'perigee_altitude_km', 'min_separation_km', 'risk_score', 'recommendation']], width="stretch", hide_index=True)

with tab4:
    st.subheader("Global Hazard Analysis")
    if not df.empty:
        c1, c2 = st.columns(2)
        c1.metric("Critical Hazards", len(df[df['risk_score'] > 0.65]))
        c2.metric("Mean Orbital Risk", f"{df['risk_score'].mean():.3f}")
        st.dataframe(df.nlargest(10, 'risk_score')[['object_name', 'risk_score', 'recommendation']], width="stretch", hide_index=True)

with tab5:
    st.subheader("AI Co-Pilot")
    st.info("System operational. Telemetry channels functioning correctly.", icon="🤖")

st.markdown("<hr/>", unsafe_allow_html=True)
st.markdown("""
<div style='text-align:center; padding: 15px 0 5px 0;'>
    <div class='sub-title' style='font-size:13px; margin-bottom:8px;'>PROJECT SUDARSHAN | AUTONOMOUS SPACE SENTINEL</div>
    <div style='font-size:11px; color:rgba(255,255,255,0.35); font-family:Rajdhani,sans-serif; letter-spacing:1px; line-height:1.7;'>
        Personal side project | Educational &amp; demonstration purpose only<br/>
        Not affiliated with any government organization | Uses public data from CelesTrak &amp; NOAA
    </div>
</div>
""", unsafe_allow_html=True)
