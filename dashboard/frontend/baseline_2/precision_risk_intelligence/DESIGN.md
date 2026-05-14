---
name: Precision Risk Intelligence
colors:
  surface: '#0b1326'
  surface-dim: '#0b1326'
  surface-bright: '#31394d'
  surface-container-lowest: '#060e20'
  surface-container-low: '#131b2e'
  surface-container: '#171f33'
  surface-container-high: '#222a3d'
  surface-container-highest: '#2d3449'
  on-surface: '#dae2fd'
  on-surface-variant: '#bdc8d1'
  inverse-surface: '#dae2fd'
  inverse-on-surface: '#283044'
  outline: '#87929a'
  outline-variant: '#3e484f'
  surface-tint: '#7bd0ff'
  primary: '#8ed5ff'
  on-primary: '#00354a'
  primary-container: '#38bdf8'
  on-primary-container: '#004965'
  inverse-primary: '#00668a'
  secondary: '#b7c8e1'
  on-secondary: '#213145'
  secondary-container: '#3a4a5f'
  on-secondary-container: '#a9bad3'
  tertiary: '#ffc176'
  on-tertiary: '#472a00'
  tertiary-container: '#f1a02b'
  on-tertiary-container: '#613b00'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#c4e7ff'
  primary-fixed-dim: '#7bd0ff'
  on-primary-fixed: '#001e2c'
  on-primary-fixed-variant: '#004c69'
  secondary-fixed: '#d3e4fe'
  secondary-fixed-dim: '#b7c8e1'
  on-secondary-fixed: '#0b1c30'
  on-secondary-fixed-variant: '#38485d'
  tertiary-fixed: '#ffddb8'
  tertiary-fixed-dim: '#ffb960'
  on-tertiary-fixed: '#2a1700'
  on-tertiary-fixed-variant: '#653e00'
  background: '#0b1326'
  on-background: '#dae2fd'
  surface-variant: '#2d3449'
typography:
  display-lg:
    fontFamily: Geist
    fontSize: 36px
    fontWeight: '700'
    lineHeight: '1.2'
    letterSpacing: -0.02em
  headline-md:
    fontFamily: Geist
    fontSize: 24px
    fontWeight: '600'
    lineHeight: '1.3'
  title-sm:
    fontFamily: Geist
    fontSize: 18px
    fontWeight: '600'
    lineHeight: '1.4'
  body-base:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: '1.5'
  body-sm:
    fontFamily: Inter
    fontSize: 12px
    fontWeight: '400'
    lineHeight: '1.5'
  data-mono:
    fontFamily: JetBrains Mono
    fontSize: 13px
    fontWeight: '500'
    lineHeight: '1.4'
    letterSpacing: -0.01em
  label-caps:
    fontFamily: Inter
    fontSize: 11px
    fontWeight: '700'
    lineHeight: 1rem
    letterSpacing: 0.05em
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  unit: 4px
  container-padding: 24px
  panel-gap: 12px
  element-tight: 8px
  element-loose: 16px
  grid-columns: '12'
  sidebar-width: 280px
---

## Brand & Style

The design system is engineered for high-stakes, real-time analytical environments. It prioritizes data density without compromising clarity, fostering an atmosphere of **precision, reliability, and technical sophistication**. 

The aesthetic leverages **Modern Corporate** principles with a heavy emphasis on **Glassmorphism** for utility panels. By utilizing semi-transparent surfaces and background blurs, the interface allows geospatial data to remain the primary visual context, even when overlaid with complex analytics. The style is "utility-first," stripping away decorative flourishes in favor of crisp borders, purposeful whitespace, and a strict semantic hierarchy. It is designed specifically for expert users who require rapid cognitive processing of risk vectors and spatial patterns.

## Colors

The palette is anchored in a deep navy/charcoal spectrum to minimize eye fatigue during extended monitoring sessions and to provide a high-contrast foundation for data visualization. 

- **Primary & UI**: A technical Sky Blue (#38bdf8) serves as the primary action color, ensuring high visibility against dark backgrounds.
- **Surface Strategy**: The background uses `#020617` for the deepest map layer, while UI panels use a semi-transparent `#0f172a` with a backdrop filter to maintain legibility over moving map data.
- **Risk Semantics**: A strict traffic-light system (Red/Amber/Emerald) is applied to all data points, charts, and status indicators. These colors are calibrated for maximum "pop" against the dark theme to ensure critical alerts are never missed.

## Typography

This design system utilizes a tri-font strategy to differentiate between structural UI, reading content, and technical data.

- **Geist** is used for headlines and titles, providing a sharp, technical character that aligns with the "developer-grade" aesthetic.
- **Inter** handles all body copy and standard UI labels, chosen for its exceptional legibility at small sizes and high-density layouts.
- **JetBrains Mono** is reserved for coordinates, timestamps, risk scores, and numeric values within tables. This monospaced choice ensures that numbers align vertically in data grids, facilitating rapid comparison of fluctuating values.

Mobile scaling: For display sizes, reduce `display-lg` to 28px and `headline-md` to 20px to accommodate narrower viewports without losing the typographic hierarchy.

## Layout & Spacing

The layout follows a **Hybrid Fluid/Fixed** model. The central MapView is fluid, expanding to fill the viewport, while analytical panels are "floated" or "docked" using a precise 4px-based grid system.

- **Desktop**: A 12-column grid is used for dashboard views. KPI cards and charts should span 3 or 4 columns respectively.
- **Panels**: Use 12px gaps between floating modules to maintain the "glass fragment" aesthetic. Sidebars are fixed at 280px to protect chart readability.
- **Safe Areas**: Maps must maintain a 24px inner margin for UI overlays to prevent interactive elements from hugging the screen edge.
- **Mobile**: Panels stack vertically and convert to bottom sheets or full-screen overlays to maximize the limited map real estate.

## Elevation & Depth

Hierarchy is established through **Tonal Layering** and **Glassmorphism** rather than traditional heavy shadows.

1.  **Level 0 (Base)**: The MapLibre/Deck.gl canvas.
2.  **Level 1 (Sub-surface)**: Persistent sidebars and headers with a solid `#0f172a` fill.
3.  **Level 2 (Floating Panels)**: Analytical modules using `rgba(15, 23, 42, 0.75)` with a `20px` backdrop-blur. These feature a `1px` border of `rgba(255, 255, 255, 0.1)` to define their edges against the map.
4.  **Level 3 (Overlays/Modals)**: Popovers and tooltips. These use a darker `rgba(2, 6, 23, 0.9)` fill with a subtle sky-blue tint in the border to indicate focus.

Shadows, where used, are "Ambient Shadows": extremely soft, 15% opacity black, with a 12px blur to suggest a gentle lift off the map surface.

## Shapes

The shape language is **Soft yet Disciplined**. A base radius of `4px` (0.25rem) is applied to most UI components (buttons, input fields, and tags) to maintain a modern, professional feel that isn't overly aggressive.

Larger analytical containers and KPI cards use `rounded-lg` (8px) to soften the perimeter of the data-heavy interface. This subtle rounding helps differentiate distinct "blocks" of information when they are positioned closely together in a dense dashboard layout.

## Components

- **KPI Cards**: Feature a `data-mono` primary value in white, a `label-caps` title in `secondary_color`, and a small Sparkline (Recharts) at the bottom. Risk status is indicated by a 2px top-border in the semantic color.
- **Buttons**: Default state is a ghost-style with a thin border. Primary actions use a solid Sky Blue fill with dark text. 
- **Charts**: Use thin, low-contrast grid lines (`rgba(255,255,255,0.05)`). Tooltips within charts must mirror the glassmorphic panel style with blurred backgrounds.
- **Input Fields**: Dark-filled with a `1px` border that glows primary blue on focus. Use monospaced text for coordinate inputs.
- **Data Tables**: Striped rows are avoided. Instead, use thin `1px` separators. Header cells use `label-caps` for clear categorization.
- **Risk Chips**: Small, pill-shaped indicators with high-saturation backgrounds and white text for 'High' risk, and low-saturation backgrounds with colored text for 'Low/Medium' risk.
- **Map Overlays**: Custom Deck.gl layers should use additive blending for "Heatmap" modes to ensure visibility over dark basemaps.