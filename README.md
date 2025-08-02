# Modular Ingestion and Validation Pipeline for Directional Wave Modeling

This repository contains a modular ingestion and validation pipeline that integrates multi-source environmental data into a relational PostgreSQL database for downstream machine learning and hazard modeling.

---

## Integrated Data Sources

- **NOAA-NDBC** and **CDIP** ocean buoy data (including directional spectra)
- **ERA5** hindcast meteorological data (wind, pressure, temperature)
- **NOAA Tides** from the nearest applicable station
- **ENSO Index** values from the MEIV2 dataset

---

## Core Database Layers

1. **`time_steps`** – Buoy-level, timestamped records of:
   - Wave parameters (`wvht`, `dpd`, `mwd`, etc.)
   - Storm metadata (e.g., `storm_distance_km`, `quadrant`)
   - Climate signals (`enso`, `tide`, `pres`)
   - Spectral classification outputs (`modality_model`, `modality_conf`)
2. **`spectra_parameters`** / **`spectra_directional`** –
   - Frequency-binned values of spectral energy (`Ef`)
   - Directional spreading functions (`D(θ)`) reconstructed from r₁, r₂, α₁, α₂

---

## Project Goals

This ingestion system forms the foundation for a broader project designed to:

- **Train machine learning models** to infer directional spectra from scalar wave conditions
- **Identify storm-modified sea states** and classify spectral modality (e.g., unimodal vs. bimodal)
- **Support real-time hazard classification**, offshore planning, and event-based wave forecasting
