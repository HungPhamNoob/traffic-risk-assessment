---
name: High-Tech Risk Monitor
colors:
  surface: '#fbf8fa'
  surface-dim: '#dcd9db'
  surface-bright: '#fbf8fa'
  surface-container-lowest: '#ffffff'
  surface-container-low: '#f5f3f4'
  surface-container: '#f0edef'
  surface-container-high: '#eae7e9'
  surface-container-highest: '#e4e2e3'
  on-surface: '#1b1b1d'
  on-surface-variant: '#45474c'
  inverse-surface: '#303032'
  inverse-on-surface: '#f3f0f2'
  outline: '#75777d'
  outline-variant: '#c5c6cd'
  surface-tint: '#545f73'
  primary: '#091426'
  on-primary: '#ffffff'
  primary-container: '#1e293b'
  on-primary-container: '#8590a6'
  inverse-primary: '#bcc7de'
  secondary: '#505f76'
  on-secondary: '#ffffff'
  secondary-container: '#d0e1fb'
  on-secondary-container: '#54647a'
  tertiary: '#1e1200'
  on-tertiary: '#ffffff'
  tertiary-container: '#35260c'
  on-tertiary-container: '#a38c6a'
  error: '#ba1a1a'
  on-error: '#ffffff'
  error-container: '#ffdad6'
  on-error-container: '#93000a'
  primary-fixed: '#d8e3fb'
  primary-fixed-dim: '#bcc7de'
  on-primary-fixed: '#111c2d'
  on-primary-fixed-variant: '#3c475a'
  secondary-fixed: '#d3e4fe'
  secondary-fixed-dim: '#b7c8e1'
  on-secondary-fixed: '#0b1c30'
  on-secondary-fixed-variant: '#38485d'
  tertiary-fixed: '#fadfb8'
  tertiary-fixed-dim: '#ddc39d'
  on-tertiary-fixed: '#271902'
  on-tertiary-fixed-variant: '#564427'
  background: '#fbf8fa'
  on-background: '#1b1b1d'
  surface-variant: '#e4e2e3'
typography:
  display-lg:
    fontFamily: Inter
    fontSize: 36px
    fontWeight: '700'
    lineHeight: '1.2'
    letterSpacing: -0.02em
  headline-md:
    fontFamily: Inter
    fontSize: 24px
    fontWeight: '600'
    lineHeight: '1.3'
    letterSpacing: -0.01em
  title-sm:
    fontFamily: Inter
    fontSize: 18px
    fontWeight: '600'
    lineHeight: '1.4'
  body-base:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: '1.6'
  body-sm:
    fontFamily: Inter
    fontSize: 13px
    fontWeight: '400'
    lineHeight: '1.5'
  label-caps:
    fontFamily: Geist
    fontSize: 11px
    fontWeight: '600'
    lineHeight: '1.2'
    letterSpacing: 0.05em
  mono-data:
    fontFamily: Geist
    fontSize: 13px
    fontWeight: '500'
    lineHeight: '1.4'
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  container-max-width: 1440px
  gutter: 1.5rem
  margin-x: 2rem
  margin-y: 1.5rem
  stack-gap: 1rem
  grid-columns: '12'
---

## Brand & Style

The design system is engineered for high-stakes decision-making environments where clarity and speed of cognition are paramount. It adopts a **Modern Corporate** aesthetic—a synthesis of high-density data utility and a clean, minimalist interface. 

The visual language communicates authority through a deep navy foundation, while maintaining a sense of openness using generous whitespace and a "Flat-Plus" approach. This ensures that the platform feels sophisticated and high-tech without the visual clutter often associated with legacy analytics tools. The style prioritizes functional beauty, utilizing subtle borders and refined typography to guide the user's eye through complex risk landscapes.

## Colors

The palette is anchored by **Professional Navy (#1E293B)**, used strategically for structural elements like sidebars and primary navigation to provide a solid frame for data. The **Trustworthy Blue (#3B82F6)** accent is reserved for primary actions and interactive states, ensuring high discoverability.

The semantic system follows a strict traffic-light protocol for risk assessment:
- **High Risk (Red):** Used sparingly for critical alerts and dangerous thresholds.
- **Medium Risk (Yellow):** Indicates caution and escalating trends.
- **Low Risk (Green):** Represents stability and nominal performance.

Backgrounds utilize a cool-toned **Very Light Gray (#F8FAFC)** to reduce eye strain, while cards and content containers use **Pure White (#FFFFFF)** to create a clear visual distinction between the canvas and the content.

## Typography

The design system utilizes **Inter** as the primary typeface for its exceptional legibility in data-heavy contexts and its neutral, systematic character. For technical data points and labels, **Geist** is introduced to provide a subtle "high-tech" monospaced feel that aids in vertical alignment of numbers and codes.

- **Headlines:** Use tighter letter spacing and heavier weights to establish clear hierarchy.
- **Data Tables:** Utilize `mono-data` for numerical values to ensure readability when comparing figures.
- **Labels:** Small caps with increased tracking are used for section headers within sidebars and cards to differentiate metadata from primary content.

## Layout & Spacing

This design system employs a **Fluid Grid** model with a 12-column structure, optimized for 1440px displays but responsive down to mobile. 

- **Dashboard Layout:** A fixed left sidebar (256px) houses the primary navigation. The main content area uses a flexible container with 32px (2rem) horizontal margins.
- **Rhythm:** An 8px base unit governs all spacing. Gutters between dashboard "widgets" or cards are set to 24px (1.5rem) to ensure visual breathing room amidst complex data visualizations.
- **Mobile Adaptation:** On mobile devices, the 12-column grid collapses to a 1-column stack. The sidebar transitions to a bottom navigation bar or a hidden drawer (hamburger menu) to maximize screen real estate for charts.

## Elevation & Depth

To maintain a clean and professional aesthetic, this design system minimizes the use of heavy shadows. Depth is primarily achieved through **Tonal Layering** and **Low-Contrast Outlines**:

- **Level 0 (Background):** #F8FAFC - The base canvas.
- **Level 1 (Cards/Surface):** #FFFFFF with a 1px border (#E2E8F0). No shadow.
- **Level 2 (Dropdowns/Modals):** #FFFFFF with a subtle, diffused shadow (0px 10px 15px -3px rgba(0,0,0,0.05)) to suggest floating above the UI.
- **Hover States:** Elements slightly darken the background or brighten the border color rather than lifting off the page, maintaining a sleek, flat profile.

## Shapes

The shape language is **Soft (0.25rem)**, reflecting the precision of a data-driven platform while avoiding the coldness of sharp corners. 

- **Standard Elements:** Buttons, input fields, and small cards use a 4px (0.25rem) radius.
- **Large Containers:** Main dashboard widgets and modal windows use 8px (0.5rem) to provide a more modern, approachable feel.
- **Interactive Indicators:** Status pips and active nav indicators may use a full pill radius for distinctiveness.

## Components

### Buttons
- **Primary:** Trustworthy Blue background, white text. Flat styling.
- **Secondary:** Ghost style with #E2E8F0 border and Navy text.
- **Destructive:** Red background for critical risk-mitigation actions.

### Cards (Widgets)
- The core of the dashboard. Pure white background, 1px subtle border, and 24px internal padding. Titles are always `title-sm` in Navy.

### Data Tables
- Header cells use `label-caps` with a light gray background (#F1F5F9).
- Row borders are thin (#F1F5F9) to keep the focus on the data.
- Hover states on rows use a very faint blue tint.

### Risk Badges
- Small, rounded-pill badges using semantic colors with 10% opacity backgrounds and 100% opacity text for high legibility (e.g., Light Red background with Deep Red text for "High Risk").

### Inputs
- Minimalist design. 1px gray border that transitions to the Accent Blue on focus. Labels sit clearly above the field in `body-sm` bold.

### Charts & Viz
- Utilize the full semantic palette. Ensure grid lines within charts are a faint #F1F5F9 to remain secondary to the data series.